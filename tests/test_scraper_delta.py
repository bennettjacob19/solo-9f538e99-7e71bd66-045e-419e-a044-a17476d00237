"""Scrape-store tests for incremental fetching, failure retention and fallback.

A fake ASGI target implements the same JSON delta contract as the bundled
sample target, which lets tests deterministically inject failed scrapes and
assert that the verifier keeps the last successful baseline until the next
success catches the missing counts up.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from app.delta import DELTA, FULL
from app.histogram import INF
from app.scraper import (
    TRANSPORT_DELTA_JSON,
    TRANSPORT_FULL_JSON,
    TRANSPORT_TEXT,
    ScrapeStore,
    delta_url_for,
)

BOUNDARIES = [0.1, 0.5, 1.0]


class FakeDeltaTarget:
    """Minimal in-memory delta endpoint + classic text endpoint."""

    def __init__(self, fail_delta_once: int = 0, boundaries=None) -> None:
        self.boundaries = list(boundaries) if boundaries is not None else BOUNDARIES
        self.exclusive = {b: 0 for b in self.boundaries}
        self.tail = 0
        self.total = 0
        self.sum_value = 0.0
        self.run_id = "r-1"
        self.seq = 0
        self.base_seq = 0
        self.base_buckets = {b: 0 for b in self.boundaries} | {INF: 0}
        self.served_seq: int | None = None
        self.dirty: dict[float, int] = {}
        self.fail_delta_remaining = fail_delta_once
        self.delta_hits = 0
        self.text_hits = 0

    def inject(self, duration: float, count: int) -> None:
        self.seq += 1
        key = INF
        for b in self.boundaries:
            if duration <= b:
                key = b
                break
        if key is INF:
            self.tail += count
        else:
            self.exclusive[key] += count
        self.total += count
        self.sum_value += duration * count
        self.dirty[key] = self.seq

    def full(self) -> dict:
        self.base_seq = self.seq
        self.base_buckets = {b: self.exclusive[b] for b in self.boundaries} | {
            INF: self.tail
        }
        self.dirty = {}
        self.served_seq = self.seq
        return {
            "type": FULL,
            "run_id": self.run_id,
            "seq": self.seq,
            "boundaries": list(self.boundaries),
            "exclusive_buckets": [self.exclusive[b] for b in self.boundaries],
            "tail": self.tail,
            "count": self.total,
            "sum": self.sum_value,
            "scrape_seq": 0,
        }

    def delta(self, since: int | None) -> dict:
        if (
            since is None
            or self.served_seq is None
            or since > self.seq
            or since < self.base_seq
        ):
            return self.full()
        if since == self.served_seq and since > self.base_seq:
            for key, s in self.dirty.items():
                if s <= since:
                    self.base_buckets[key] = (
                        self.tail if key is INF else self.exclusive[key]
                    )
            self.base_seq = since
            self.dirty = {k: s for k, s in self.dirty.items() if s > since}
        changed = [
            {
                "le": ("+Inf" if key is INF else key),
                "count": (self.tail if key is INF else self.exclusive[key])
                - self.base_buckets[key],
            }
            for key in self.dirty
            if self.dirty[key] > self.base_seq
        ]
        self.served_seq = self.seq
        return {
            "type": DELTA,
            "run_id": self.run_id,
            "since_seq": self.base_seq,
            "seq": self.seq,
            "changed_buckets": changed,
            "count": self.total,
            "sum": self.sum_value,
            "scrape_seq": 0,
        }

    def text(self) -> str:
        lines = ["# TYPE m histogram"]
        running = 0
        for b in self.boundaries:
            running += self.exclusive[b]
            lines.append(f'm_bucket{{le="{b}",run_id="{self.run_id}"}} {running}')
        lines.append(f'm_bucket{{le="+Inf",run_id="{self.run_id}"}} {self.total}')
        lines.append(f"m_count {self.total}")
        lines.append(f"m_sum {self.sum_value}")
        return "\n".join(lines) + "\n"

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return
        path = scope["path"].rstrip("/")
        query = scope.get("query_string", b"").decode()
        if path.endswith("/metrics_delta"):
            self.delta_hits += 1
            if self.fail_delta_remaining > 0:
                self.fail_delta_remaining -= 1
                await send({"type": "http.response.start", "status": 500,
                            "headers": [(b"content-type", b"text/plain")]})
                await send({"type": "http.response.body", "body": b"boom"})
                return
            since = None
            if query.startswith("since_seq="):
                since = int(query.split("=", 1)[1])
            body = json.dumps(self.delta(since)).encode()
            await send({"type": "http.response.start", "status": 200,
                        "headers": [(b"content-type", b"application/json")]})
            await send({"type": "http.response.body", "body": body})
            return
        self.text_hits += 1
        body = self.text().encode()
        await send({"type": "http.response.start", "status": 200,
                    "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": body})


class TextOnlyTarget:
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return
        if scope["path"].rstrip("/").endswith("/metrics_delta"):
            await send({"type": "http.response.start", "status": 404,
                        "headers": [(b"content-type", b"text/plain")]})
            await send({"type": "http.response.body", "body": b"not found"})
            return
        body = (
            "# TYPE http_request_duration_seconds histogram\n"
            'http_request_duration_seconds_bucket{le="0.1",run_id="x"} 1\n'
            'http_request_duration_seconds_bucket{le="+Inf",run_id="x"} 1\n'
            "http_request_duration_seconds_count 1\n"
            "http_request_duration_seconds_sum 0.05\n"
        ).encode()
        await send({"type": "http.response.start", "status": 200,
                    "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": body})


async def _store_with(target, name="i1") -> ScrapeStore:
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=target), base_url="http://target"
    )
    store = ScrapeStore({name: "http://target/metrics"})
    store._client = client
    return store


async def _scrape(store: ScrapeStore, name="i1"):
    await store._scrape_once(store.instances[name])
    return store.instances[name].history[-1]


async def test_first_scrape_full_then_deltas_and_byte_counters():
    target = FakeDeltaTarget()
    store = await _store_with(target)
    state = store.instances["i1"]

    record = await _scrape(store)
    assert record.ok
    assert record.transport == TRANSPORT_FULL_JSON
    assert state.full_responses == 1 and state.delta_responses == 0
    assert state.full_body_bytes == record.body_bytes > 0
    assert state.session is not None and state.session.seq == 0

    target.inject(0.05, 4)
    target.inject(2.0, 1)  # tail
    record = await _scrape(store)
    assert record.transport == TRANSPORT_DELTA_JSON
    assert record.snapshot.buckets[0.1] == 4
    assert record.snapshot.buckets[INF] == 5
    assert state.delta_responses == 1
    assert state.delta_body_bytes == record.body_bytes
    # The full layout is retained locally while the wire payload only carries
    # the two changed buckets; savings scale with the number of idle buckets.
    assert len(record.snapshot.buckets) > 2

    # Empty delta still counts as a successful scrape and zero traffic.
    record = await _scrape(store)
    assert record.ok and record.transport == TRANSPORT_DELTA_JSON
    assert record.snapshot.count == 5
    assert state.delta_responses == 2


async def test_failed_scrape_keeps_baseline_and_next_success_catches_up():
    target = FakeDeltaTarget()
    store = await _store_with(target)
    state = store.instances["i1"]

    await _scrape(store)  # full baseline at seq 0
    target.inject(0.05, 3)

    # The next response is a transport failure (500): the baseline must stay.
    target.fail_delta_remaining = 1
    failed = await _scrape(store)
    assert failed.ok is False
    assert state.total_failures == 1
    assert state.session is not None and state.session.seq == 0
    assert state.delta_responses == 0  # failed body never counted

    # More traffic arrives while the store is still holding the old baseline.
    target.inject(0.2, 2)
    caught_up = await _scrape(store)
    assert caught_up.ok
    assert caught_up.transport == TRANSPORT_DELTA_JSON
    # The delta still starts at the old cursor and contains every missing count.
    snapshot = caught_up.snapshot
    assert snapshot.buckets[0.1] == 3
    assert snapshot.buckets[0.5] == 5
    assert snapshot.count == 5
    assert state.session.seq == target.seq == 2


async def test_restart_returns_full_table_and_rebuilds_session():
    target = FakeDeltaTarget()
    store = await _store_with(target)
    state = store.instances["i1"]

    await _scrape(store)
    target.inject(0.05, 3)
    await _scrape(store)
    assert state.session.run_id == "r-1"

    # Simulated process restart: counters and cursor reset, new run id.
    target.__init__()
    target.run_id = "r-2"
    record = await _scrape(store)
    assert record.ok
    assert record.transport == TRANSPORT_FULL_JSON
    assert state.session.run_id == "r-2"
    assert state.session.seq == 0
    assert record.snapshot.count == 0
    assert state.full_responses == 2


async def test_text_only_target_falls_back_and_keeps_full_scraping():
    target = TextOnlyTarget()
    store = await _store_with(target)
    state = store.instances["i1"]

    first = await _scrape(store)
    assert first.ok and first.transport == TRANSPORT_TEXT
    assert state.delta_capable is False
    assert state.full_body_bytes == first.body_bytes

    second = await _scrape(store)
    assert second.ok and second.transport == TRANSPORT_TEXT
    assert state.delta_responses == 0
    assert state.full_responses == 2
    # No more delta attempts after the 404 probe.
    stats = store.stats()["instances"]["i1"]
    assert stats["transport_mode"] == "full-table-only"


async def test_delta_payload_size_tracks_changed_buckets_not_total():
    # With many bucket boundaries, a full table serializes every bucket while
    # a delta serializes only the one bucket that actually changed.
    many = [round(0.05 * i, 3) for i in range(1, 41)]  # 40 finite boundaries
    target = FakeDeltaTarget(boundaries=many)
    target_full = target.full()
    store = await _store_with(target)
    state = store.instances["i1"]
    full_record = await _scrape(store)
    assert full_record.transport == TRANSPORT_FULL_JSON

    target.inject(0.02, 1)  # only the smallest bucket changes
    delta_record = await _scrape(store)
    assert delta_record.transport == TRANSPORT_DELTA_JSON
    assert delta_record.snapshot.count == 1
    # One changed bucket + metadata vs 40 buckets + boundaries in the full.
    assert delta_record.body_bytes < full_record.body_bytes
    assert state.delta_body_bytes == delta_record.body_bytes
    # Full target-side payload contains all 40 finite buckets + tail; the
    # delta payload contains exactly one changed bucket.
    assert len(target_full["exclusive_buckets"]) == 40
    wire_delta = target.delta(target.base_seq)
    assert len(wire_delta["changed_buckets"]) == 1


def test_delta_url_derivation():
    assert (
        delta_url_for("http://h:9101/metrics") == "http://h:9101/metrics_delta"
    )
    assert (
        delta_url_for("https://h/path/metrics?x=1")
        == "https://h/path/metrics_delta?x=1"
    )
    assert delta_url_for("ftp://h/metrics") is None


async def test_malformed_delta_payload_fails_open_without_baseline_change():
    target = FakeDeltaTarget()
    store = await _store_with(target)
    state = store.instances["i1"]
    await _scrape(store)
    target.inject(0.05, 2)

    real_delta = target.delta
    target.delta = lambda since: {"type": "delta", "run_id": "r-1"}  # invalid
    bad = await _scrape(store)
    assert bad.ok is False
    target.delta = real_delta

    good = await _scrape(store)
    assert good.ok and good.transport == TRANSPORT_DELTA_JSON
    assert good.snapshot.count == 2
    assert state.session.seq == target.seq


async def test_concurrent_scrape_loop_shares_one_session(monkeypatch):
    target = FakeDeltaTarget()
    store = await _store_with(target)
    state = store.instances["i1"]
    monkeypatch.setattr(store, "scrape_interval", 0.01)
    # Drive the real loop coroutine directly so the injected ASGI client stays
    # in use (store.start() would replace it with a network client).
    task = asyncio.create_task(store._scrape_loop())
    try:
        target.inject(0.05, 10)
        await asyncio.sleep(0.1)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert state.total_successes >= 2
    assert state.delta_responses >= 1
    # Every successful scrape advanced the single shared session.
    assert state.session.count == 10
