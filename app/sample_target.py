"""Local sample target exposing a Prometheus latency histogram.

Two instances of this app are started with different ``SAMPLE_BUCKETS``
values so the verifier has to cope with differing bucket layouts.

Runtime controls (used by tests and demos):

* ``POST /inject``  -- add one or more request observations
                       body: ``{"duration_seconds": 0.23, "count": 1}``
* ``POST /restart`` -- simulate a process restart: new run id, new process
                       start time, all counters reset to zero
* ``GET  /state``   -- inspect the in-memory counters

Exposition:

* ``GET /metrics``       -- classic Prometheus text format, always the full
                            cumulative table (Prometheus itself scrapes this);
* ``GET /metrics_delta`` -- JSON exposition supporting incremental scraping:
                            the first request (or one after a restart / with an
                            unknown cursor) gets the full table, later requests
                            only the mutually exclusive buckets that changed
                            since the scraper's last successful response.
"""

from __future__ import annotations

import os
import time
import uuid

from fastapi import FastAPI, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from .delta import DELTA, FULL, render_le
from .histogram import INF

METRIC_NAME = "http_request_duration_seconds"
DEFAULT_BUCKETS_A = "0.05,0.1,0.25,0.5,1,2.5,5"
DEFAULT_BUCKETS_B = "0.1,0.3,0.7,1.5,3,6"


class InjectBody(BaseModel):
    duration_seconds: float = Field(..., ge=0)
    count: int = Field(1, ge=1, le=100_000)


class TargetState:
    def __init__(self, instance_id: str, boundaries: list[float]) -> None:
        self.instance_id = instance_id
        self.boundaries = boundaries
        self.reset()

    def reset(self) -> None:
        """Restart the (simulated) process."""
        self.run_id = uuid.uuid4().hex[:12]
        self.process_start = time.time()
        # cumulative counts per finite boundary (Prometheus exposition)
        self.bucket_counts: dict[float, int] = {b: 0 for b in self.boundaries}
        self.total_count = 0
        self.total_sum = 0.0
        self.scrape_seq = 0
        self.restart_count = getattr(self, "restart_count", 0) + 1
        # ---- incremental-exposition bookkeeping ----
        # Monotonic per-process mutation counter; every /inject bumps it once
        # and a restart resets it to zero. It is the cursor of the delta
        # protocol, independent of how often scrapes happen.
        self.state_seq = 0
        # Mutually exclusive per-bucket counts: finite boundary -> requests in
        # (previous_bound, boundary], plus the tail (largest_bound, +Inf).
        self.exclusive: dict[float, int] = {b: 0 for b in self.boundaries}
        self.tail_count = 0
        # Baseline last acknowledged by the scraper. Only buckets whose latest
        # mutation seq exceeds base_seq are "dirty", i.e. need to be sent.
        self.base_seq = 0
        self.base_buckets: dict[float, int] = {b: 0 for b in self.boundaries}
        self.base_buckets[INF] = 0
        # bucket (finite bound or INF) -> seq of its most recent mutation
        self.dirty: dict[float, int] = {}
        # seq of the most recent delta/full response served to the scraper
        self.served_seq: int | None = None

    def inject(self, duration: float, count: int) -> None:
        for boundary in self.boundaries:
            if duration <= boundary:
                self.bucket_counts[boundary] += count
        self.total_count += count
        self.total_sum += duration * count

        # Exclusive placement: the first boundary >= duration owns the
        # observation; anything above the largest boundary lands in the tail.
        self.state_seq += 1
        key = INF
        for boundary in self.boundaries:
            if duration <= boundary:
                key = boundary
                break
        if key is INF:
            self.tail_count += count
        else:
            self.exclusive[key] += count
        self.dirty[key] = self.state_seq

    # ------------------------------------------------------------------ #
    # Incremental exposition
    # ------------------------------------------------------------------ #

    def _full_payload(self) -> dict:
        # Rebase the session onto the current state.
        self.base_seq = self.state_seq
        self.base_buckets = {b: self.exclusive[b] for b in self.boundaries}
        self.base_buckets[INF] = self.tail_count
        self.dirty = {}
        self.served_seq = self.state_seq
        return {
            "type": FULL,
            "run_id": self.run_id,
            "seq": self.state_seq,
            "boundaries": list(self.boundaries),
            "exclusive_buckets": [self.exclusive[b] for b in self.boundaries],
            "tail": self.tail_count,
            "count": self.total_count,
            "sum": self.total_sum,
            "scrape_seq": self.scrape_seq,
        }

    def _current_bucket(self, key: float) -> int:
        return self.tail_count if key is INF else self.exclusive[key]

    def _delta_payload(self) -> dict:
        # Every dirty bucket is one whose latest mutation is newer than the
        # baseline, so generating the payload touches only changed buckets.
        changed = [
            {
                "le": render_le(key),
                "count": self._current_bucket(key) - self.base_buckets[key],
            }
            for key in self.dirty
            if self.dirty[key] > self.base_seq
        ]
        self.served_seq = self.state_seq
        return {
            "type": DELTA,
            "run_id": self.run_id,
            "since_seq": self.base_seq,
            "seq": self.state_seq,
            "changed_buckets": changed,
            "count": self.total_count,
            "sum": self.total_sum,
            "scrape_seq": self.scrape_seq,
        }

    def metrics_delta(self, since_seq: int | None) -> dict:
        """Serve a full table or an incremental payload for one scrape.

        ``since_seq`` is the cursor of the scraper's last successfully
        processed response. A full table is returned when there is no
        established session, the cursor is unknown (restart / lost
        bookkeeping), or it points into the future. An acknowledged cursor
        advances the baseline; a cursor that never reached us (lost response)
        leaves the baseline untouched so the next successful scrape
        automatically covers the longer interval.
        """
        if (
            since_seq is None
            or self.served_seq is None
            or since_seq > self.state_seq
            or since_seq < self.base_seq
        ):
            return self._full_payload()

        if since_seq == self.served_seq and since_seq > self.base_seq:
            # The scraper confirmed our last response: compact the baseline to
            # that cursor. Only previously dirty buckets need copying, so the
            # work stays proportional to the number of changed buckets.
            for key, seq in self.dirty.items():
                if seq <= since_seq:
                    self.base_buckets[key] = self._current_bucket(key)
            self.dirty = {
                key: seq for key, seq in self.dirty.items() if seq > since_seq
            }
            self.base_seq = since_seq
        # Otherwise a response was lost: the scraper still holds base_seq and
        # the next payload must catch up everything since then.
        return self._delta_payload()

    def next_scrape_seq(self) -> int:
        seq = self.scrape_seq
        self.scrape_seq += 1
        return seq


def _read_buckets(instance_id: str) -> list[float]:
    raw = os.environ.get("SAMPLE_BUCKETS")
    if raw is None:
        raw = DEFAULT_BUCKETS_A if instance_id.endswith("a") else DEFAULT_BUCKETS_B
    values = [float(part.strip()) for part in raw.split(",") if part.strip()]
    if not values:
        raise ValueError("SAMPLE_BUCKETS must contain at least one boundary")
    return sorted(values)


def create_app() -> FastAPI:
    instance_id = os.environ.get("INSTANCE_ID", "sample-a")
    state = TargetState(instance_id, _read_buckets(instance_id))
    app = FastAPI(title=f"sample-target-{instance_id}", version="1.0.0")
    app.state.target = state

    @app.post("/inject")
    def inject(body: InjectBody) -> dict:
        state.inject(body.duration_seconds, body.count)
        return {"ok": True, "total_count": state.total_count, "seq": state.state_seq}

    @app.post("/restart")
    def restart() -> dict:
        state.reset()
        return {"ok": True, "run_id": state.run_id}

    @app.get("/state")
    def get_state() -> dict:
        return {
            "instance_id": state.instance_id,
            "run_id": state.run_id,
            "process_start_time_seconds": state.process_start,
            "boundaries": state.boundaries,
            "bucket_counts": state.bucket_counts,
            "count": state.total_count,
            "sum": state.total_sum,
            "restart_count": state.restart_count,
            "state_seq": state.state_seq,
            "exclusive": {render_le(k): v for k, v in state.exclusive.items()},
            "tail": state.tail_count,
            "delta_base_seq": state.base_seq,
            "delta_served_seq": state.served_seq,
        }

    # Plain-text exposition, returned with the Prometheus content type.
    @app.get("/metrics", response_class=PlainTextResponse)
    def render_metrics() -> PlainTextResponse:
        scrape_seq = state.next_scrape_seq()
        lines = [
            f"# HELP {METRIC_NAME} request latency in seconds",
            f"# TYPE {METRIC_NAME} histogram",
        ]
        for boundary, value in state.bucket_counts.items():
            lines.append(
                f'{METRIC_NAME}_bucket{{le="{boundary:g}",run_id="{state.run_id}"}} {value}'
            )
        lines.append(
            f'{METRIC_NAME}_bucket{{le="+Inf",run_id="{state.run_id}"}} {state.total_count}'
        )
        lines.append(f'{METRIC_NAME}_count{{run_id="{state.run_id}"}} {state.total_count}')
        lines.append(f'{METRIC_NAME}_sum{{run_id="{state.run_id}"}} {state.total_sum}')
        lines.extend(
            [
                "# HELP process_start_time_seconds start time of the process since unix epoch",
                "# TYPE process_start_time_seconds gauge",
                f"process_start_time_seconds {state.process_start:.6f}",
                "# HELP sample_scrape_seq number of scrapes served since process start",
                "# TYPE sample_scrape_seq gauge",
                f"sample_scrape_seq {scrape_seq}",
            ]
        )
        return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

    @app.get("/metrics_delta", tags=["增量抓取"])
    def render_metrics_delta(
        since_seq: int | None = Query(
            default=None,
            alias="since_seq",
            ge=0,
            description="上次成功处理的响应游标；缺省或未知时返回整表",
        ),
    ) -> dict:
        """JSON 增量 exposition：整表或仅含变动互斥桶（含尾桶）的增量。"""
        payload = state.metrics_delta(since_seq)
        # Keep the per-exposition scrape sequence counter consistent with the
        # text endpoint regardless of which payload is returned.
        payload["scrape_seq"] = state.next_scrape_seq()
        return payload

    return app


app = create_app()
