"""Target-side tests for the incremental exposition (``/metrics_delta``).

These exercise :class:`TargetState` directly for deterministic cursor
handling, plus one ASGI-level test for the HTTP contract and the equivalence
with the classic Prometheus text exposition.
"""

from __future__ import annotations

import httpx
import pytest

from app.delta import DELTA, FULL, apply_delta, snapshot_from_full
from app.histogram import INF, parse_metrics_text
from app.sample_target import METRIC_NAME, TargetState


def _state(boundaries=(0.1, 0.5, 1.0)) -> TargetState:
    return TargetState("sample-a", list(boundaries))


def test_first_scrape_gets_full_table():
    state = _state()
    payload = state.metrics_delta(None)
    assert payload["type"] == FULL
    assert payload["run_id"] == state.run_id
    assert payload["seq"] == 0
    assert payload["boundaries"] == [0.1, 0.5, 1.0]
    assert payload["exclusive_buckets"] == [0, 0, 0]
    assert payload["tail"] == 0
    assert payload["count"] == 0


def test_zero_change_delta_is_successful_and_empty():
    state = _state()
    full = state.metrics_delta(None)
    assert full["type"] == FULL

    empty = state.metrics_delta(full["seq"])
    assert empty["type"] == DELTA
    assert empty["since_seq"] == 0
    assert empty["seq"] == 0
    assert empty["changed_buckets"] == []
    assert empty["count"] == 0


def test_delta_contains_only_mutated_exclusive_buckets_including_tail():
    state = _state()
    state.metrics_delta(None)  # establish baseline at seq 0

    state.inject(0.05, 4)   # lands in (-inf, 0.1]
    state.inject(2.0, 2)    # above largest boundary -> tail
    payload = state.metrics_delta(0)
    assert payload["type"] == DELTA
    assert payload["since_seq"] == 0
    assert payload["seq"] == 2
    assert payload["count"] == 6
    by_le = {entry["le"]: entry["count"] for entry in payload["changed_buckets"]}
    assert by_le == {0.1: 4, "+Inf": 2}
    # Unchanged finite buckets are absent: payload size tracks changed buckets.
    assert len(payload["changed_buckets"]) == 2


def test_lost_response_keeps_baseline_and_next_success_catches_up():
    state = _state()
    state.metrics_delta(None)

    state.inject(0.05, 3)
    served = state.metrics_delta(0)          # response is lost by the scraper
    assert served["seq"] == 1

    state.inject(0.2, 2)
    # The scraper never acknowledged seq 1 and still presents the old cursor.
    catch_up = state.metrics_delta(0)
    assert catch_up["type"] == DELTA
    assert catch_up["since_seq"] == 0
    by_le = {entry["le"]: entry["count"] for entry in catch_up["changed_buckets"]}
    # Both mutations since the real baseline are present in one payload.
    assert by_le == {0.1: 3, 0.5: 2}
    assert catch_up["count"] == 5


def test_ack_compacts_baseline_and_later_deltas_start_there():
    state = _state()
    state.metrics_delta(None)
    state.inject(0.05, 3)
    first = state.metrics_delta(0)

    # Scraper confirms the response; baseline moves to seq 1.
    state.inject(0.2, 2)
    second = state.metrics_delta(first["seq"])
    assert second["since_seq"] == 1
    by_le = {entry["le"]: entry["count"] for entry in second["changed_buckets"]}
    assert by_le == {0.5: 2}

    # After another ack, an unchanged table yields an empty delta.
    empty = state.metrics_delta(second["seq"])
    assert empty["changed_buckets"] == []


def test_repeated_injection_into_same_bucket_counts_since_baseline():
    state = _state()
    state.metrics_delta(None)
    state.inject(0.05, 3)
    first = state.metrics_delta(0)
    assert first["changed_buckets"] == [{"le": 0.1, "count": 3}]

    state.metrics_delta(first["seq"])  # ack
    state.inject(0.05, 5)
    second = state.metrics_delta(1)
    assert second["changed_buckets"] == [{"le": 0.1, "count": 5}]
    assert second["count"] == 8


def test_restart_forces_full_table_for_any_cursor():
    state = _state()
    state.metrics_delta(None)
    state.inject(0.05, 3)
    old = state.metrics_delta(0)

    state.reset()  # process restart: counters and cursor reset
    payload = state.metrics_delta(old["seq"])
    assert payload["type"] == FULL
    assert payload["run_id"] != old["run_id"]
    assert payload["seq"] == 0
    assert payload["count"] == 0


def test_unknown_or_future_cursor_forces_full_table():
    state = _state()
    state.metrics_delta(None)
    state.inject(0.05, 1)

    future = state.metrics_delta(99)
    assert future["type"] == FULL

    # A full response re-bases the session; the next call is a delta again.
    delta = state.metrics_delta(future["seq"])
    assert delta["type"] == DELTA
    assert delta["changed_buckets"] == []


def test_delta_reconstruction_matches_prometheus_text_exposition():
    state = _state(boundaries=(0.05, 0.1, 0.25, 0.5, 1.0))
    full_payload = state.metrics_delta(None)
    snapshot, session = snapshot_from_full(full_payload, METRIC_NAME, 0.0)

    for duration, count in [(0.02, 10), (0.07, 20), (0.3, 5), (4.0, 2)]:
        state.inject(duration, count)
        payload = state.metrics_delta(session.seq)
        assert payload["type"] == DELTA
        snapshot, session = apply_delta(payload, session, METRIC_NAME, 0.0)

    text_snapshot = parse_metrics_text(
        # render via the FastAPI app below would need HTTP; emulate exposition
        # from state counters directly
        _render_text(state),
        METRIC_NAME,
        0.0,
    )
    assert snapshot.buckets == text_snapshot.buckets
    assert snapshot.count == text_snapshot.count
    assert snapshot.sum_value == text_snapshot.sum_value
    assert snapshot.process_id == text_snapshot.process_id
    # Exclusive bookkeeping keeps the tail separately.
    assert session.exclusive[INF] == 2
    assert snapshot.buckets[INF] == 37


def _render_text(state: TargetState) -> str:
    lines = [
        f"# TYPE {METRIC_NAME} histogram",
    ]
    for boundary, value in state.bucket_counts.items():
        lines.append(
            f'{METRIC_NAME}_bucket{{le="{boundary:g}",run_id="{state.run_id}"}} {value}'
        )
    lines.append(
        f'{METRIC_NAME}_bucket{{le="+Inf",run_id="{state.run_id}"}} {state.total_count}'
    )
    lines.append(f"{METRIC_NAME}_count {state.total_count}")
    lines.append(f"{METRIC_NAME}_sum {state.total_sum}")
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- #
# HTTP contract
# --------------------------------------------------------------------------- #


@pytest.fixture
async def client(monkeypatch):
    monkeypatch.setenv("INSTANCE_ID", "sample-a")
    monkeypatch.setenv("SAMPLE_BUCKETS", "0.1,0.5,1")
    from app.sample_target import create_app

    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        yield c


async def test_http_endpoint_full_then_delta_and_inject(client):
    first = (await client.get("/metrics_delta")).json()
    assert first["type"] == FULL

    resp = await client.post(
        "/inject", json={"duration_seconds": 0.2, "count": 7}
    )
    assert resp.json()["ok"] is True

    delta = (
        await client.get("/metrics_delta", params={"since_seq": first["seq"]})
    ).json()
    assert delta["type"] == DELTA
    assert delta["changed_buckets"] == [{"le": 0.5, "count": 7}]
    assert delta["count"] == 7

    # The classic endpoint still serves the whole Prometheus table.
    text = (await client.get("/metrics")).text
    assert f'{METRIC_NAME}_bucket{{le="+Inf"' in text
    assert ' 7' in text


async def test_http_endpoint_rejects_negative_cursor(client):
    resp = await client.get("/metrics_delta", params={"since_seq": -1})
    assert resp.status_code == 422
