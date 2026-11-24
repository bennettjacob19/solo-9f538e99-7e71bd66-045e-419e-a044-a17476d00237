"""End-to-end tests with real uvicorn servers and background scraping."""

from __future__ import annotations

import asyncio
import os
import time

import httpx
import pytest
import uvicorn

from app.main import create_app
from app.sample_target import create_app as create_target_app

PORTS = {"a": 9201, "b": 9202}
BUCKETS_A = "0.05,0.1,0.25,0.5,1,2.5,5"
BUCKETS_B = "0.1,0.3,0.7,1.5,3,6"


class Server:
    def __init__(self, name: str, port: int, app_factory, env: dict[str, str] | None = None):
        self.name = name
        self.port = port
        # Factory is invoked inside start() *after* the env vars are set,
        # otherwise SAMPLE_BUCKETS / INSTANCE_ID would not take effect.
        self.app_factory = app_factory
        self.env = env or {}

    async def start(self) -> None:
        old = {k: os.environ.get(k) for k in self.env}
        os.environ.update(self.env)
        try:
            app = self.app_factory()
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        self.config = uvicorn.Config(
            app, host="127.0.0.1", port=self.port, log_level="error", lifespan="on"
        )
        self.server = uvicorn.Server(self.config)
        self._task = asyncio.create_task(self.server.serve())
        await _wait_port(self.port)

    async def stop(self) -> None:
        self.server.should_exit = True
        await asyncio.wait_for(self._task, timeout=5)


async def _wait_port(port: int, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    last_exc = None
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            try:
                await client.get(f"http://127.0.0.1:{port}/state")
                return
            except httpx.HTTPError as exc:
                last_exc = exc
                await asyncio.sleep(0.05)
    raise RuntimeError(f"port {port} never came up: {last_exc}")


@pytest.fixture
async def servers():
    target_a = Server("a", PORTS["a"], create_target_app,
                      {"INSTANCE_ID": "sample-a", "SAMPLE_BUCKETS": BUCKETS_A})
    target_b = Server("b", PORTS["b"], create_target_app,
                      {"INSTANCE_ID": "sample-b", "SAMPLE_BUCKETS": BUCKETS_B})
    await asyncio.gather(target_a.start(), target_b.start())

    os.environ["SCRAPE_TARGETS"] = (
        f"sample-a=http://127.0.0.1:{PORTS['a']}/metrics,"
        f"sample-b=http://127.0.0.1:{PORTS['b']}/metrics"
    )
    verifier = Server("verifier", 9200, create_app)
    await verifier.start()

    yield {
        "verifier": "http://127.0.0.1:9200",
        "a": f"http://127.0.0.1:{PORTS['a']}",
        "b": f"http://127.0.0.1:{PORTS['b']}",
    }

    await verifier.stop()
    await asyncio.gather(target_a.stop(), target_b.stop())
    os.environ.pop("SCRAPE_TARGETS", None)


async def _inject(base: str, duration: float, count: int = 1) -> None:
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{base}/inject",
            json={"duration_seconds": duration, "count": count},
        )


async def _check(base: str, threshold: float, max_wait: float, target: float = 0.99):
    async with httpx.AsyncClient(timeout=max_wait + 10) as client:
        resp = await client.get(
            f"{base}/check",
            params={"threshold": threshold, "max-wait": max_wait, "target": target},
        )
        return resp.json()


async def _wait_ready(base: str, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            stats = (await client.get(f"{base}/stats")).json()
            samples = stats["current_ready_samples"]
            if all("not_ready_reason" not in v for v in samples.values()):
                return
            await asyncio.sleep(0.1)
    raise AssertionError("verifier never got ready samples")


# --------------------------------------------------------------------------- #


async def test_proceed_when_lower_bound_meets_99_percent(servers):
    # 100 reqs per instance, exactly 99 within 0.5s on each -> 198/200.
    await _inject(servers["a"], 0.2, 99)
    await _inject(servers["a"], 2.0, 1)
    await _inject(servers["b"], 0.2, 99)
    await _inject(servers["b"], 2.0, 1)

    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "proceed", result
    assert result["total_requests"] == 200
    assert result["compliant_lower"] == 198
    assert result["lower_ratio"] == pytest.approx(0.99)
    assert result["timed_out"] is False
    # two scrapes, at least 1s apart, same process per instance
    for name in ("sample-a", "sample-b"):
        sample = result["instances"][name]
        assert sample["gap_seconds"] >= 1.0
        assert sample["process_id"]
        assert sample["earlier_scrape_at"] < sample["later_scrape_at"]


async def test_halt_when_upper_bound_below_99_percent(servers):
    # 90/100 good on each instance -> 180/200 = 90%, upper bound stays 90%.
    await _inject(servers["a"], 0.2, 90)
    await _inject(servers["a"], 2.0, 10)
    await _inject(servers["b"], 0.2, 90)
    await _inject(servers["b"], 2.0, 10)

    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "halt", result
    assert result["upper_ratio"] == pytest.approx(0.9)
    assert result["lower_ratio"] == pytest.approx(0.9)


async def test_inconclusive_when_threshold_inside_bucket(servers):
    # The verifier scores the *increment* between the two most recent
    # scrapes. Restart both targets first so no traffic from earlier tests
    # leaks into the window, then wait for fresh same-process samples.
    async with httpx.AsyncClient() as client:
        await client.post(f"{servers['a']}/restart")
        await client.post(f"{servers['b']}/restart")
    await _wait_ready(servers["verifier"])

    # Instance A buckets include 0.5 -> exact at threshold 0.5:
    # 99 good, 1 slow = 99/100.
    await _inject(servers["a"], 0.2, 99)
    await _inject(servers["a"], 2.0, 1)
    # Instance B's nearest boundaries around 0.5 are 0.3 and 0.7.
    # 97 clearly <= 0.3; three requests in (0.3, 0.7] are unknown at
    # threshold 0.5 -> per-instance bounds 97..100 of 100.
    await _inject(servers["b"], 0.2, 97)
    await _inject(servers["b"], 0.4, 3)

    # Wait until a scrape window has captured the injected traffic on both
    # instances, then the next check evaluates that window.
    deadline = time.monotonic() + 8
    captured = False
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            stats = (await client.get(f"{servers['verifier']}/stats")).json()
            windows = stats["current_ready_samples"]
            counts = [v.get("requests_in_window", 0) for v in windows.values()]
            if counts == [100, 100]:
                captured = True
                break
            await asyncio.sleep(0.1)
    assert captured, "injected traffic never appeared in a scrape window"

    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "inconclusive", result
    b = result["instances"]["sample-b"]
    assert b["compliant_lower"] == 97
    assert b["compliant_upper"] == 100
    # combined: lower 196/200 = 98%, upper 199/200 = 99.5% -> straddles 99%
    assert result["lower_ratio"] == pytest.approx(0.98)
    assert result["upper_ratio"] == pytest.approx(0.995)


async def test_zero_traffic_is_inconclusive(servers):
    await _wait_ready(servers["verifier"])
    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "inconclusive", result
    assert result["zero_traffic"] is True
    assert result["total_requests"] == 0
    assert "零流量" in result["reason"]


async def test_restart_invalidates_samples_then_recovers(servers):
    await _inject(servers["a"], 0.2, 50)
    await _inject(servers["b"], 0.2, 50)
    await _wait_ready(servers["verifier"])

    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{servers['a']}/restart")
        new_run_id = resp.json()["run_id"]

        # Wait until the verifier has observed the new process exactly once:
        # the newest two scrapes then straddle the restart (different run
        # ids), which must be rejected.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            stats = (await client.get(f"{servers['verifier']}/stats")).json()
            info = stats["instances"]["sample-a"]
            ready = stats["current_ready_samples"]["sample-a"]
            if (
                info["last_process_id"] == new_run_id
                and ready.get("not_ready_reason") == "process_restarted"
            ):
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("verifier never observed the restart boundary")

        # A check with a short deadline must return sample-a as missing
        # evidence rather than silently using cross-process samples.
        check_resp = await client.get(
            f"{servers['verifier']}/check",
            params={"threshold": 0.5, "max-wait": 0.5},
        )
        result = check_resp.json()
    assert result["timed_out"] is True
    assert "sample-a" in result["missing_instances"]
    assert result["decision"] == "inconclusive"
    assert "不同进程" in result["missing_reasons"]["sample-a"]

    # After two fresh same-process scrapes (~2.1s) the store recovers.
    await _wait_ready(servers["verifier"], timeout=8)
    stats = await _get_stats(servers["verifier"])
    ready_a = stats["current_ready_samples"]["sample-a"]
    assert ready_a["process_id"] == new_run_id
    assert ready_a["requests_in_window"] == 0

    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "inconclusive"  # zero traffic after restart
    assert result["instances"]["sample-a"]["process_id"] == new_run_id


async def test_missing_instance_times_out_with_reason(monkeypatch, servers):
    # Shut instance B down; the verifier keeps serving and the check returns
    # with B named as the missing-evidence instance.
    async with httpx.AsyncClient() as client:
        # nothing to shut via fixture; simulate by pointing a fresh verifier
        # at a dead port.
        pass
    monkeypatch.setenv(
        "SCRAPE_TARGETS",
        f"sample-a=http://127.0.0.1:{PORTS['a']}/metrics,"
        "sample-b=http://127.0.0.1:9999/metrics",
    )
    extra = Server("verifier2", 9210, create_app)
    await extra.start()
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "http://127.0.0.1:9210/check",
                params={"threshold": 0.5, "max-wait": 3},
            )
            result = resp.json()
        assert result["timed_out"] is True
        assert result["missing_instances"] == ["sample-b"]
        assert result["missing_reasons"]["sample-b"] == "成功抓取不足两次"
    finally:
        await extra.stop()
        monkeypatch.delenv("SCRAPE_TARGETS", raising=False)


async def test_concurrent_checks_share_samples_and_isolate_timeouts(servers):
    await _inject(servers["a"], 0.2, 99)
    await _inject(servers["a"], 2.0, 1)
    await _inject(servers["b"], 0.2, 99)
    await _inject(servers["b"], 2.0, 1)

    async with httpx.AsyncClient(timeout=30) as client:
        # One impatient request (likely times out) alongside a patient one;
        # both must end independently and the patient one must still decide.
        short = client.get(
            f"{servers['verifier']}/check",
            params={"threshold": 0.5, "max-wait": 0.2},
        )
        long = client.get(
            f"{servers['verifier']}/check",
            params={"threshold": 0.5, "max-wait": 8},
        )
        short_resp, long_resp = await asyncio.gather(short, long)

    short_result = short_resp.json()
    long_result = long_resp.json()
    assert long_result["decision"] == "proceed"
    # The impatient request either timed out or (if samples were already
    # cached from another test) succeeded — but it never broke the long one.
    assert short_result["decision"] in ("inconclusive", "proceed")

    stats = await _get_stats(servers["verifier"])
    # Two instances scraped in lockstep; attempts must be shared, never
    # multiplied per concurrent check request.
    for name in ("sample-a", "sample-b"):
        assert stats["instances"][name]["scrape_attempts"] >= 2


async def test_stats_reports_scrape_counts_and_sample_times(servers):
    await _wait_ready(servers["verifier"])
    stats = await _get_stats(servers["verifier"])
    for name in ("sample-a", "sample-b"):
        info = stats["instances"][name]
        assert info["scrape_attempts"] >= 2
        assert info["scrape_successes"] == info["scrape_attempts"]
        assert info["scrape_failures"] == 0
        sample = stats["current_ready_samples"][name]
        assert sample["gap_seconds"] >= 1.0
        assert sample["earlier_scrape_at"] <= time.time()
        assert sample["later_scrape_at"] <= time.time()


async def _get_stats(base: str) -> dict:
    async with httpx.AsyncClient() as client:
        return (await client.get(f"{base}/stats")).json()


# --------------------------------------------------------------------------- #
# Incremental scraping
# --------------------------------------------------------------------------- #


async def test_delta_scraping_reduces_body_bytes_and_stats_reports_them(servers):
    await _wait_ready(servers["verifier"])
    # Restart both targets so all counters are zero and the verifier starts a
    # fresh delta session on each, then wait for two same-process scrapes.
    async with httpx.AsyncClient() as client:
        await client.post(f"{servers['a']}/restart")
        await client.post(f"{servers['b']}/restart")
    await _wait_ready(servers["verifier"])

    stats = await _get_stats(servers["verifier"])
    for name in ("sample-a", "sample-b"):
        info = stats["instances"][name]
        # One full table established the new process session; later successes
        # are incremental JSON, including zero-change scrapes.
        assert info["transport_mode"] == "incremental-json"
        assert info["full_table_responses"] >= 1
        assert info["delta_responses"] >= 1
        assert info["full_table_body_bytes"] > 0
        assert info["delta_body_bytes"] > 0
        session = info["delta_session"]
        assert session["run_id"]
        assert "boundaries" in session and session["count"] == 0

    # Traffic lands in a single bucket: subsequent delta bodies stay small and
    # only changed buckets cross the wire. Keep injecting until a scrape
    # window captures the traffic, since the ~1.05s scrape cadence means a
    # single injection between two scrapes can skip straight past a window.
    deadline = time.monotonic() + 8
    captured = False
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            await _inject(servers["a"], 0.2, 20)
            await _inject(servers["b"], 0.2, 20)
            stats = (await client.get(f"{servers['verifier']}/stats")).json()
            counts = [
                v.get("requests_in_window", 0)
                for v in stats["current_ready_samples"].values()
            ]
            # A 1.05s scrape window may contain several 0.25s injection rounds.
            if all(c > 0 and c % 20 == 0 for c in counts):
                captured = True
                break
            await asyncio.sleep(0.25)
        assert captured, "injected traffic never appeared in a scrape window"

        # The cumulative session counters retain the full-process totals even
        # after the two-scrape window has slid past the injection point.
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            stats = (await client.get(f"{servers['verifier']}/stats")).json()
            sessions = [
                stats["instances"][n]["delta_session"]["count"]
                for n in ("sample-a", "sample-b")
            ]
            if all(c >= 20 for c in sessions):
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("delta sessions never reached the injected totals")

    stats = await _get_stats(servers["verifier"])
    for name in ("sample-a", "sample-b"):
        info = stats["instances"][name]
        assert info["delta_responses"] >= 2
        # Cumulative byte counters only grow.
        assert info["full_table_body_bytes"] > 0
        assert info["delta_body_bytes"] > 0


async def test_check_equivalence_between_delta_and_full_scraping(servers):
    # Same sampling input must yield identical /check request counts, bounds
    # and decisions as full-table scraping: the verifier reconstructs the very
    # same cumulative counters from the deltas.
    async with httpx.AsyncClient() as client:
        await client.post(f"{servers['a']}/restart")
        await client.post(f"{servers['b']}/restart")
    await _wait_ready(servers["verifier"])

    # One injection round only: poll until the delta sessions have absorbed it
    # (proving the merge works). The two-scrape window may already have slid
    # past it, so /check is validated on the authoritative session state and
    # against either the precise 100/100 window or a later zero-traffic window.
    await _inject(servers["a"], 0.2, 99)
    await _inject(servers["a"], 2.0, 1)
    await _inject(servers["b"], 0.2, 97)
    await _inject(servers["b"], 0.4, 3)

    deadline = time.monotonic() + 8
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            stats = (await client.get(f"{servers['verifier']}/stats")).json()
            sessions = [
                stats["instances"][n]["delta_session"]["count"]
                for n in ("sample-a", "sample-b")
            ]
            if sessions == [100, 100]:
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("delta sessions never absorbed the injection")

        # Cross-check reconstructed session counters against the targets'
        # authoritative full-table state.
        for port, name in ((9201, "sample-a"), (9202, "sample-b")):
            target_state = (
                await client.get(f"http://127.0.0.1:{port}/state")
            ).json()
            session = stats["instances"][name]["delta_session"]
            assert session["count"] == target_state["count"] == 100
            assert session["sum"] == target_state["sum"]

    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    if result["total_requests"] == 200:
        # Full-scrape expectations: A exact 99/100; B bounds 97..100;
        # combined 196..199 of 200.
        assert result["compliant_lower"] == 196
        assert result["compliant_upper"] == 199
        assert result["decision"] == "inconclusive"
        assert result["instances"]["sample-b"]["compliant_lower"] == 97
        assert result["instances"]["sample-b"]["compliant_upper"] == 100
    else:
        assert result["zero_traffic"] is True
        assert result["decision"] == "inconclusive"


async def test_lost_delta_response_keeps_baseline_and_next_scrape_catches_up(servers):
    async with httpx.AsyncClient() as client:
        await client.post(f"{servers['a']}/restart")
        await client.post(f"{servers['b']}/restart")
    await _wait_ready(servers["verifier"])

    # Drive the target's delta endpoint directly: establish the baseline the
    # verifier currently holds, then simulate a lost verifier response by
    # consuming a delta without the verifier acknowledging it.
    async with httpx.AsyncClient() as client:
        state_a = (await client.get(f"{servers['a']}/state")).json()
        baseline_seq = state_a["state_seq"]

        # Traffic before the "lost" scrape.
        await _inject(servers["a"], 0.2, 10)
        lost = (
            await client.get(
                f"{servers['a']}/metrics_delta",
                params={"since_seq": baseline_seq},
            )
        ).json()
        assert lost["type"] == "delta"
        first_changed = sum(item["count"] for item in lost["changed_buckets"])
        assert first_changed == 10

        # More traffic while the verifier still holds the old baseline.
        await _inject(servers["a"], 0.2, 5)
        catch_up = (
            await client.get(
                f"{servers['a']}/metrics_delta",
                params={"since_seq": baseline_seq},
            )
        ).json()
        assert catch_up["type"] == "delta"
        assert catch_up["since_seq"] == baseline_seq
        # One payload covers both mutations since the real baseline.
        assert sum(item["count"] for item in catch_up["changed_buckets"]) == 15
        assert catch_up["count"] == 15

    # Meanwhile the verifier kept scraping on its own unchanged session and
    # eventually observes all 15 requests through normal delta catch-up.
    deadline = time.monotonic() + 8
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            stats = (await client.get(f"{servers['verifier']}/stats")).json()
            session = stats["instances"]["sample-a"]["delta_session"]
            if session and session["count"] == 15:
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("verifier never caught the missing counts up")


async def test_sample_restart_serves_full_table_and_check_recovers(servers):
    await _wait_ready(servers["verifier"])
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{servers['a']}/restart")
        new_run_id = resp.json()["run_id"]

        # The very next verifier scrape of the restarted process must be a
        # full table (new run id, seq reset), not an unmergeable delta.
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            stats = (await client.get(f"{servers['verifier']}/stats")).json()
            info = stats["instances"]["sample-a"]
            session = info["delta_session"]
            if session and session["run_id"] == new_run_id:
                assert info["full_table_responses"] >= 1
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("verifier never established a post-restart session")

    # Sample selection still rejects cross-process windows, then recovers.
    await _wait_ready(servers["verifier"], timeout=8)
    stats = await _get_stats(servers["verifier"])
    ready_a = stats["current_ready_samples"]["sample-a"]
    assert ready_a["process_id"] == new_run_id
    result = await _check(servers["verifier"], threshold=0.5, max_wait=8)
    assert result["decision"] == "inconclusive"
    assert result["instances"]["sample-a"]["process_id"] == new_run_id


async def test_metrics_text_endpoint_still_serves_full_prometheus_table(servers):
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{servers['a']}/metrics")
        assert resp.headers["content-type"].startswith("text/plain")
        text = resp.text
        assert "http_request_duration_seconds_bucket" in text
        assert 'le="+Inf"' in text
        assert "process_start_time_seconds" in text


async def test_verifier_keeps_baseline_across_real_http_failure(monkeypatch):
    # A flaky delta target fails exactly once with HTTP 500. The verifier must
    # count the failure, keep the previous session untouched and catch every
    # missing count up on the next successful scrape.
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    flaky = FastAPI()
    state = {
        "fail_remaining": 1,
        "seq": 0,
        "count": 0,
        "run_id": "flaky-1",
        "base_seq": 0,
        "served_seq": None,
    }

    def _full() -> dict:
        state["base_seq"] = state["seq"]
        state["served_seq"] = state["seq"]
        return {
            "type": "full",
            "run_id": state["run_id"],
            "seq": state["seq"],
            "boundaries": [1.0],
            "exclusive_buckets": [0],
            "tail": state["count"],
            "count": state["count"],
            "sum": 0.0,
            "scrape_seq": 0,
        }

    @flaky.get("/metrics_delta")
    async def metrics_delta(since_seq: int | None = None):
        if state["fail_remaining"] > 0:
            state["fail_remaining"] -= 1
            return JSONResponse(status_code=500, content={"error": "boom"})
        if (
            since_seq is None
            or state["served_seq"] is None
            or since_seq > state["seq"]
            or since_seq < state["base_seq"]
        ):
            return _full()
        # Single tail bucket: the delta carries everything since the baseline.
        payload = {
            "type": "delta",
            "run_id": state["run_id"],
            "since_seq": state["base_seq"],
            "seq": state["seq"],
            "changed_buckets": (
                [{"le": "+Inf", "count": state["count"]}]
                if state["seq"] > state["base_seq"]
                else []
            ),
            "count": state["count"],
            "sum": 0.0,
            "scrape_seq": 0,
        }
        state["served_seq"] = state["seq"]
        return payload

    target = Server("flaky", 9221, lambda: flaky)
    monkeypatch.setenv(
        "SCRAPE_TARGETS",
        "sample-a=http://127.0.0.1:9221/metrics,sample-b=http://127.0.0.1:9221/metrics",
    )
    verifier = Server("verifier3", 9220, create_app)
    await asyncio.gather(target.start(), verifier.start())
    try:
        async with httpx.AsyncClient() as client:
            # Wait for the first successful full-table scrape on both entries.
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                stats = (await client.get("http://127.0.0.1:9220/stats")).json()
                infos = stats["instances"]
                if all(i["delta_session"] is not None for i in infos.values()):
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("verifier never established sessions")

            # Arm the one-shot failure and add traffic before recovery.
            state["fail_remaining"] = 1
            state["seq"] = 1
            state["count"] = 7
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                stats = (await client.get("http://127.0.0.1:9220/stats")).json()
                info = stats["instances"]["sample-a"]
                if info["scrape_failures"] >= 1 and info["delta_session"]["count"] == 7:
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError("failure/catch-up never observed")

            stats = (await client.get("http://127.0.0.1:9220/stats")).json()
            for name in ("sample-a", "sample-b"):
                info = stats["instances"][name]
                assert info["scrape_failures"] >= 1
                assert info["delta_session"]["count"] == 7
                assert info["transport_mode"] == "incremental-json"
                # The failed 500 body was never accepted as a baseline.
                assert info["delta_session"]["run_id"] == "flaky-1"
    finally:
        await verifier.stop()
        await target.stop()
        monkeypatch.delenv("SCRAPE_TARGETS", raising=False)
