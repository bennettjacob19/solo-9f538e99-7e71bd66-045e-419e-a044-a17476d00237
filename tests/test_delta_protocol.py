"""Unit tests for the JSON incremental-metrics protocol.

Covers full-table parsing, delta merging, lost-response catch-up (the
baseline only advances on acknowledged successful scrapes), restart handling
and all payload validation rules.
"""

from __future__ import annotations

import math

import pytest

from app.delta import (
    DELTA,
    FULL,
    DeltaProtocolError,
    DeltaSession,
    apply_delta,
    snapshot_from_full,
)
from app.histogram import INF, compute_delta

METRIC = "http_request_duration_seconds"
BOUNDARIES = [0.1, 0.5, 1.0]


def _full_payload(
    run_id="p1",
    seq=0,
    exclusive=(0, 0, 0),
    tail=0,
    sum_value=0.0,
    scrape_seq=0,
) -> dict:
    return {
        "type": FULL,
        "run_id": run_id,
        "seq": seq,
        "boundaries": BOUNDARIES,
        "exclusive_buckets": list(exclusive),
        "tail": tail,
        "count": sum(exclusive) + tail,
        "sum": sum_value,
        "scrape_seq": scrape_seq,
    }


def _delta_payload(
    run_id="p1",
    since_seq=0,
    seq=1,
    changed=None,
    count=None,
    sum_value=None,
    scrape_seq=1,
) -> dict:
    return {
        "type": DELTA,
        "run_id": run_id,
        "since_seq": since_seq,
        "seq": seq,
        "changed_buckets": changed or [],
        "count": 0 if count is None else count,
        "sum": 0.0 if sum_value is None else sum_value,
        "scrape_seq": scrape_seq,
    }


def test_full_payload_reconstructs_prometheus_cumulative_buckets():
    # exclusive: 5 in (-inf,0.1], 10 in (0.1,0.5], 3 in (0.5,1.0], 2 tail
    payload = _full_payload(exclusive=(5, 10, 3), tail=2, sum_value=7.5)
    snapshot, session = snapshot_from_full(payload, METRIC, 100.0)

    assert snapshot.process_id == "p1"
    assert snapshot.count == 20
    assert snapshot.sum_value == 7.5
    assert snapshot.buckets[0.1] == 5
    assert snapshot.buckets[0.5] == 15
    assert snapshot.buckets[1.0] == 18
    assert snapshot.buckets[INF] == 20
    assert session.seq == 0
    assert session.exclusive[INF] == 2


def test_full_payload_rejects_count_mismatch_and_bad_layout():
    bad = _full_payload(exclusive=(1, 2, 3), tail=4)
    bad["count"] = 99
    with pytest.raises(DeltaProtocolError):
        snapshot_from_full(bad, METRIC, 0.0)

    unsorted = _full_payload()
    unsorted["boundaries"] = [1.0, 0.1]
    unsorted["exclusive_buckets"] = [0, 0]
    with pytest.raises(DeltaProtocolError):
        snapshot_from_full(unsorted, METRIC, 0.0)

    with pytest.raises(DeltaProtocolError):
        snapshot_from_full({"type": FULL}, METRIC, 0.0)


def test_delta_merges_only_changed_buckets_and_rebuilds_cumulative():
    _, session = snapshot_from_full(_full_payload(), METRIC, 0.0)

    # Two requests land in (0.1, 0.5], one in the tail.
    delta = _delta_payload(
        since_seq=0, seq=1,
        changed=[{"le": 0.5, "count": 2}, {"le": "+Inf", "count": 1}],
        count=3, sum_value=2.4,
    )
    snapshot, session = apply_delta(delta, session, METRIC, 1.1)

    assert session.seq == 1
    assert snapshot.buckets[0.1] == 0
    assert snapshot.buckets[0.5] == 2
    assert snapshot.buckets[1.0] == 2
    assert snapshot.buckets[INF] == 3
    assert snapshot.count == 3
    assert math.isclose(snapshot.sum_value, 2.4)


def test_empty_delta_is_a_successful_scrape():
    _, session = snapshot_from_full(_full_payload(seq=2), METRIC, 0.0)
    delta = _delta_payload(since_seq=2, seq=2, changed=[], count=0)
    snapshot, new_session = apply_delta(delta, session, METRIC, 1.0)

    assert new_session.seq == 2
    assert snapshot.count == 0
    assert all(value == 0 for value in new_session.exclusive.values())
    # No observations -> window delta against the same state is zero traffic.
    window = compute_delta(snapshot, snapshot)
    assert window.total_requests == 0


def test_lost_response_catch_up_extends_the_old_baseline():
    # Baseline at seq=0; the seq=1 response is lost, so the next successful
    # response must still be based on seq=0 and include every missing count.
    _, session = snapshot_from_full(_full_payload(), METRIC, 0.0)
    catch_up = _delta_payload(
        since_seq=0, seq=2,
        changed=[{"le": 0.1, "count": 4}, {"le": "+Inf", "count": 6}],
        count=10, sum_value=3.0,
    )
    snapshot, session = apply_delta(catch_up, session, METRIC, 2.0)
    assert session.seq == 2
    assert snapshot.buckets[0.1] == 4
    assert snapshot.buckets[INF] == 10

    # The following scrape can advance normally from seq=2.
    next_delta = _delta_payload(
        since_seq=2, seq=3, changed=[{"le": 0.1, "count": 1}],
        count=11, sum_value=3.1,
    )
    snapshot, session = apply_delta(next_delta, session, METRIC, 3.0)
    assert snapshot.buckets[0.1] == 5
    assert snapshot.count == 11


def test_delta_rejects_wrong_run_id_non_extending_cursor_and_reset():
    _, session = snapshot_from_full(_full_payload(), METRIC, 0.0)

    restarted = _delta_payload(run_id="p2")
    with pytest.raises(DeltaProtocolError):
        apply_delta(restarted, session, METRIC, 1.0)

    skipped = _delta_payload(since_seq=5, seq=6, count=0)
    with pytest.raises(DeltaProtocolError):
        apply_delta(skipped, session, METRIC, 1.0)

    reset = _delta_payload(
        since_seq=0, seq=1, changed=[], count=0, sum_value=-1.0
    )
    with pytest.raises(DeltaProtocolError):
        apply_delta(reset, session, METRIC, 1.0)

    backwards_count = _delta_payload(
        since_seq=0, seq=1,
        changed=[{"le": 0.1, "count": 1}], count=0,
    )
    with pytest.raises(DeltaProtocolError):
        apply_delta(backwards_count, session, METRIC, 1.0)


def test_delta_rejects_unknown_bucket_and_negative_increment():
    _, session = snapshot_from_full(_full_payload(), METRIC, 0.0)
    unknown = _delta_payload(
        since_seq=0, seq=1,
        changed=[{"le": 0.25, "count": 1}], count=1,
    )
    with pytest.raises(DeltaProtocolError):
        apply_delta(unknown, session, METRIC, 1.0)

    negative = _delta_payload(
        since_seq=0, seq=1,
        changed=[{"le": 0.1, "count": -1}], count=0,
    )
    with pytest.raises(DeltaProtocolError):
        apply_delta(negative, session, METRIC, 1.0)


def test_reconstructed_snapshot_equals_full_table_at_same_state():
    # Simulate the exact same true state reached two ways: one full scrape and
    # a sequence of delta scrapes. Cumulative counters must match exactly.
    full = _full_payload(seq=3, exclusive=(7, 11, 4), tail=2, sum_value=12.0)
    full_snapshot, _ = snapshot_from_full(full, METRIC, 9.0)

    _, session = snapshot_from_full(_full_payload(seq=0), METRIC, 0.0)
    steps = [
        _delta_payload(
            since_seq=0, seq=1,
            changed=[{"le": 0.1, "count": 5}, {"le": 0.5, "count": 8}],
            count=13, sum_value=4.0,
        ),
        _delta_payload(
            since_seq=1, seq=2,
            changed=[{"le": 0.1, "count": 2}, {"le": 1.0, "count": 4},
                     {"le": "+Inf", "count": 2}],
            count=21, sum_value=9.5,
        ),
        _delta_payload(
            since_seq=2, seq=3, changed=[{"le": 0.5, "count": 3}],
            count=24, sum_value=12.0,
        ),
    ]
    for step in steps:
        rebuilt, session = apply_delta(step, session, METRIC, 9.0)

    assert rebuilt.buckets == full_snapshot.buckets
    assert rebuilt.count == full_snapshot.count
    assert rebuilt.sum_value == full_snapshot.sum_value
    assert rebuilt.process_id == full_snapshot.process_id


def test_session_cumulative_buckets_sums_exclusive_counts():
    session = DeltaSession(
        run_id="p", seq=0, boundaries=BOUNDARIES,
        exclusive={0.1: 2, 0.5: 3, 1.0: 0, INF: 5},
        count=10, sum_value=1.0,
    )
    assert session.cumulative_buckets() == {0.1: 2, 0.5: 5, 1.0: 5, INF: 10}
