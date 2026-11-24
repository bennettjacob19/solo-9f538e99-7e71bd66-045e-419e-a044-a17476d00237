"""JSON incremental-metrics protocol shared by sample targets and the scraper.

Sample targets expose two expositions:

* ``GET /metrics``        -- the classic Prometheus text format (full table);
* ``GET /metrics_delta``  -- a JSON payload that is either a *full* snapshot
  (first scrape, process restart or unknown cursor) or a *delta* carrying only
  the mutually exclusive latency buckets (the tail ``+Inf`` bucket included)
  that received new requests since the scraper's last successful baseline.

The functions in this module are pure so the reconstruction rules can be unit
tested without any HTTP machinery.  A reconstructed :class:`HistogramSnapshot`
is logically identical to one parsed from a full Prometheus exposition at the
same instant: the same cumulative integer bucket counts, ``_count`` and
``_sum`` -- which is what keeps ``/check`` results bit-for-bit equivalent to
full-table scraping.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from .histogram import INF, HistogramSnapshot, MetricsParseError

INF_LABEL = "+Inf"
FULL = "full"
DELTA = "delta"


class DeltaProtocolError(MetricsParseError):
    """Raised when a JSON delta payload cannot be interpreted safely."""


@dataclass
class DeltaSession:
    """The scraper-side baseline for one target process lifetime."""

    run_id: str
    seq: int
    boundaries: list[float]  # finite upper bounds, sorted ascending
    # Current mutually exclusive counts: finite boundary -> observations in
    # (previous_bound, boundary], plus INF -> observations in the tail bucket.
    exclusive: dict[float, int]
    count: int
    sum_value: float
    scrape_seq: int | None = None

    def cumulative_buckets(self) -> dict[float, int]:
        """Derive Prometheus cumulative counters from exclusive counts."""
        running = 0
        buckets: dict[float, int] = {}
        for boundary in self.boundaries:
            running += self.exclusive.get(boundary, 0)
            buckets[boundary] = running
        buckets[INF] = self.count
        return buckets


def _require(payload: dict, key: str):
    if key not in payload:
        raise DeltaProtocolError(f"delta payload missing required field {key!r}")
    return payload[key]


def _le_to_boundary(raw) -> float:
    if raw in (INF_LABEL, "Inf", "-Inf"):
        return INF
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise DeltaProtocolError(f"invalid bucket label le={raw!r}") from exc


def _boundary_to_le(boundary: float):
    return INF_LABEL if math.isinf(boundary) else boundary


def _validate_full(payload: dict) -> None:
    boundaries = _require(payload, "boundaries")
    exclusive_buckets = _require(payload, "exclusive_buckets")
    tail = _require(payload, "tail")
    count = _require(payload, "count")
    if not isinstance(boundaries, list) or not boundaries:
        raise DeltaProtocolError("full payload needs a non-empty boundaries list")
    if len(boundaries) != len(exclusive_buckets):
        raise DeltaProtocolError(
            "exclusive_buckets length must match boundaries length"
        )
    parsed_boundaries = [float(b) for b in boundaries]
    if parsed_boundaries != sorted(parsed_boundaries) or len(set(parsed_boundaries)) != len(
        parsed_boundaries
    ):
        raise DeltaProtocolError("boundaries must be sorted and unique")
    if any(not math.isfinite(b) for b in parsed_boundaries):
        raise DeltaProtocolError("finite boundaries must not contain infinity")
    if any(int(c) < 0 for c in exclusive_buckets) or int(tail) < 0:
        raise DeltaProtocolError("bucket counts must be non-negative")
    if int(count) != sum(int(c) for c in exclusive_buckets) + int(tail):
        raise DeltaProtocolError(
            "full payload count must equal the sum of all exclusive buckets"
        )


def snapshot_from_full(
    payload: dict, metric_name: str, scraped_at: float
) -> tuple[HistogramSnapshot, DeltaSession]:
    """Build a snapshot and a fresh session from a ``full`` JSON payload."""
    if payload.get("type") != FULL:
        raise DeltaProtocolError(f"expected type={FULL!r}, got {payload.get('type')!r}")
    run_id = _require(payload, "run_id")
    if not isinstance(run_id, str) or not run_id:
        raise DeltaProtocolError("full payload needs a non-empty run_id")
    _validate_full(payload)

    boundaries = [float(b) for b in payload["boundaries"]]
    exclusive = {
        boundary: int(count)
        for boundary, count in zip(boundaries, payload["exclusive_buckets"])
    }
    exclusive[INF] = int(payload["tail"])
    count = int(payload["count"])
    sum_value = float(payload.get("sum", 0.0))
    scrape_seq = payload.get("scrape_seq")
    session = DeltaSession(
        run_id=run_id,
        seq=int(_require(payload, "seq")),
        boundaries=boundaries,
        exclusive=exclusive,
        count=count,
        sum_value=sum_value,
        scrape_seq=int(scrape_seq) if scrape_seq is not None else None,
    )
    snapshot = HistogramSnapshot(
        metric_name=metric_name,
        buckets=session.cumulative_buckets(),
        count=count,
        sum_value=sum_value,
        process_id=run_id,
        scrape_seq=session.scrape_seq,
        scraped_at=scraped_at,
    )
    return snapshot, session


def apply_delta(
    payload: dict,
    session: DeltaSession,
    metric_name: str,
    scraped_at: float,
) -> tuple[HistogramSnapshot, DeltaSession]:
    """Apply a ``delta`` JSON payload on top of an established session."""
    if payload.get("type") != DELTA:
        raise DeltaProtocolError(f"expected type={DELTA!r}, got {payload.get('type')!r}")
    run_id = _require(payload, "run_id")
    if run_id != session.run_id:
        # A restart must arrive as a full payload; a delta from another process
        # can never be merged into this baseline.
        raise DeltaProtocolError(
            f"delta run_id {run_id!r} does not match session {session.run_id!r}"
        )
    since_seq = int(_require(payload, "since_seq"))
    if since_seq != session.seq:
        raise DeltaProtocolError(
            f"delta since_seq={since_seq} does not extend session seq={session.seq}"
        )
    new_seq = int(_require(payload, "seq"))
    if new_seq < since_seq:
        raise DeltaProtocolError("delta seq must be >= since_seq")
    changed = _require(payload, "changed_buckets")
    if not isinstance(changed, list):
        raise DeltaProtocolError("changed_buckets must be a list")

    exclusive = dict(session.exclusive)
    known = set(session.boundaries) | {INF}
    for entry in changed:
        boundary = _le_to_boundary(_require(entry, "le"))
        increment = int(_require(entry, "count"))
        if boundary not in known:
            raise DeltaProtocolError(
                f"delta references unknown bucket le={entry['le']!r}"
            )
        if increment < 0:
            raise DeltaProtocolError("bucket increments must be non-negative")
        exclusive[boundary] = exclusive.get(boundary, 0) + increment

    new_count = int(_require(payload, "count"))
    if new_count < session.count:
        raise DeltaProtocolError("absolute count moved backwards (counter reset)")
    reconstructed = sum(exclusive.values())
    if reconstructed != new_count:
        raise DeltaProtocolError(
            f"applied buckets sum to {reconstructed} but payload count is {new_count}"
        )
    new_sum = float(_require(payload, "sum"))
    if new_sum < session.sum_value - 1e-12:
        raise DeltaProtocolError("absolute sum moved backwards")

    scrape_seq = payload.get("scrape_seq")
    new_session = DeltaSession(
        run_id=run_id,
        seq=new_seq,
        boundaries=list(session.boundaries),
        exclusive=exclusive,
        count=new_count,
        sum_value=new_sum,
        scrape_seq=int(scrape_seq) if scrape_seq is not None else None,
    )
    snapshot = HistogramSnapshot(
        metric_name=metric_name,
        buckets=new_session.cumulative_buckets(),
        count=new_count,
        sum_value=new_sum,
        process_id=run_id,
        scrape_seq=new_session.scrape_seq,
        scraped_at=scraped_at,
    )
    return snapshot, new_session


def render_le(boundary: float):
    """External JSON form of a bucket label."""
    return _boundary_to_le(boundary)
