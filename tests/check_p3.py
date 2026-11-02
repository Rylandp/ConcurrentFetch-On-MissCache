#!/usr/bin/env python3
"""Property-based trace verifier and concurrency check suite for Project 3."""

from pathlib import Path
import os
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
BIN = Path(os.environ.get("CACHELAB_BIN", str(ROOT / "cachelab")))


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def parse_trace_file(name):
    requests = []
    backing = {}
    for raw in (ROOT / "test_tr" / name).read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if parts[0] == "BACKING":
            key = parts[1]
            size = int(parts[2])
            fail = "FAIL" in parts[3:]
            backing[key] = (size, fail)
        else:
            requests.append(parts)
    return requests, backing


def fields(line):
    parts = line.split()
    require(parts, "empty event line")
    values = {}
    for part in parts[1:]:
        key, sep, value = part.partition("=")
        require(sep and key and value, f"malformed event field: {part!r}")
        require(key not in values, f"duplicate event field: {key}")
        values[key] = value
    return parts[0], values


def run_trace(name, pool_size, clients, max_fetches, allocation, eviction,
              extra_args=()):
    cmd = [
        str(BIN), str(pool_size), f"test_tr/{name}", str(clients),
        "-a", allocation, "-e", eviction, "-f", str(max_fetches),
        *extra_args,
    ]
    result = subprocess.run(
        cmd, cwd=ROOT, capture_output=True, text=True, timeout=10,
    )
    require(result.returncode == 0,
            f"{name} ({allocation}/{eviction}, c={clients}, f={max_fetches}) "
            f"exited {result.returncode}: {result.stderr.strip()}")

    lines = result.stdout.splitlines()
    require(lines and lines[-1].startswith("STATS "),
            f"{name}: missing final STATS line")
    kind, stats = fields(lines[-1])
    require(kind == "STATS", f"{name}: last event is not STATS")

    requests, _backing = parse_trace_file(name)
    has_shutdown = any(r[0] == "SHUTDOWN" for r in requests)

    events = []
    per_req = {}
    active_fetches = 0
    peak_fetches = 0
    inflight_keys = {}
    active_cache = {}

    hits = coalesced_hits = misses = fetches = fetch_fails = 0
    evictions = rejects = 0
    shutdown_seen = False

    for idx, line in enumerate(lines[:-1]):
        kind, data = fields(line)
        require("req" in data and "client" in data,
                f"{name}: event missing req/client: {line}")
        seq = int(data["req"])
        cid = int(data["client"])
        require(0 <= seq < len(requests) and 0 <= cid < clients,
                f"{name}: invalid req/client in: {line}")

        events.append((idx, kind, data))
        per_req.setdefault(seq, []).append((kind, data))

        if kind == "HIT":
            hits += 1
        elif kind == "COALESCED_WAIT":
            pass
        elif kind == "COALESCED_HIT":
            coalesced_hits += 1
        elif kind == "COALESCED_FAIL":
            pass
        elif kind == "MISS":
            misses += 1
        elif kind == "FETCH_START":
            key = data["key"]
            require(key not in inflight_keys,
                    f"{name}: duplicate concurrent FETCH_START for key={key}")
            inflight_keys[key] = seq
            active_fetches += 1
            fetches += 1
            if active_fetches > peak_fetches:
                peak_fetches = active_fetches
            require(active_fetches <= max_fetches,
                    f"{name}: active_fetches={active_fetches} exceeds {max_fetches}")
        elif kind == "FETCH_DONE":
            key = data["key"]
            require(inflight_keys.get(key) == seq,
                    f"{name}: FETCH_DONE without matching FETCH_START for {key}")
            del inflight_keys[key]
            active_fetches -= 1
            require(active_fetches >= 0, f"{name}: negative active_fetches")
        elif kind == "FETCH_FAIL":
            key = data["key"]
            require(inflight_keys.get(key) == seq,
                    f"{name}: FETCH_FAIL without matching FETCH_START for {key}")
            del inflight_keys[key]
            active_fetches -= 1
            fetch_fails += 1
        elif kind == "PUT":
            pass
        elif kind == "UPDATE":
            key = data["key"]
            require(key in active_cache,
                    f"{name}: UPDATE for non-active key {key}")
            del active_cache[key]
        elif kind == "EVICT":
            victim = data["key"]
            require(victim in active_cache,
                    f"{name}: EVICT for non-active key {victim}")
            require(str(active_cache[victim]) == data["size"],
                    f"{name}: EVICT size mismatch for {victim}")
            del active_cache[victim]
            evictions += 1
        elif kind == "COALESCE":
            pass
        elif kind == "ALLOC":
            key = data["key"]
            sz = int(data["size"])
            addr = int(data["addr"])
            require(0 <= addr < pool_size, f"{name}: ALLOC addr out of range")
            active_cache[key] = sz
            require(sum(active_cache.values()) <= pool_size,
                    f"{name}: pool occupancy exceeds pool_size")
        elif kind == "REJECT":
            rejects += 1
        elif kind == "SHUTDOWN":
            shutdown_seen = True
        elif kind == "ABORT":
            key = data.get("key")
            if key in inflight_keys and inflight_keys[key] == seq:
                del inflight_keys[key]
                active_fetches -= 1
        else:
            raise AssertionError(f"{name}: unknown event {kind}")

    if not has_shutdown:
        require(set(per_req.keys()) == set(range(len(requests))),
                f"{name}: not all requests were executed")

    terminal_kinds = {
        "HIT", "COALESCED_HIT", "COALESCED_FAIL",
        "ALLOC", "REJECT", "FETCH_FAIL", "ABORT", "SHUTDOWN"
    }
    for seq, req_events in per_req.items():
        last_kind = req_events[-1][0]
        require(last_kind in terminal_kinds,
                f"{name}: req {seq} ended with non-terminal {last_kind}")

    expected_stats = {
        "hits": str(hits),
        "coalesced_hits": str(coalesced_hits),
        "misses": str(misses),
        "fetches": str(fetches),
        "fetch_fails": str(fetch_fails),
        "peak_fetches": f"{peak_fetches}/{max_fetches}",
        "evictions": str(evictions),
        "rejects": str(rejects),
        "final_occupancy": f"{sum(active_cache.values())}/{pool_size}",
    }
    for k, v in expected_stats.items():
        require(stats.get(k) == v,
                f"{name}: STATS {k} expected {v}, got {stats.get(k)}")

    if has_shutdown:
        require(shutdown_seen, f"{name}: expected SHUTDOWN event")

    return events, per_req, stats


def main():
    stress_mode = "--stress" in sys.argv[1:]

    # 1. Single-client functional & policy checks
    for alloc in ("first", "best"):
        for evict in ("fifo", "clock"):
            run_trace("basic_fetch.trace", 1024, 1, 2, alloc, evict)

    _ev_fifo, req_fifo, _ = run_trace("evict_on_fetch.trace", 1024, 1, 2, "first", "fifo")
    _ev_clock, req_clock, _ = run_trace("evict_on_fetch.trace", 1024, 1, 2, "first", "clock")
    fifo_victims = [d["key"] for s in (4, 6) for k, d in req_fifo[s] if k == "EVICT"]
    clock_victims = [d["key"] for s in (4, 6) for k, d in req_clock[s] if k == "EVICT"]
    require(fifo_victims == ["A", "B"], f"FIFO victims expected ['A', 'B'], got {fifo_victims}")
    require(clock_victims == ["A", "C"], f"Clock victims expected ['A', 'C'], got {clock_victims}")

    # 2. Stampede coalescing check (multiple clients hitting key A at once)
    _ev, _req, st = run_trace("stampede.trace", 1024, 4, 2, "first", "clock",
                              extra_args=("--barrier-start",))
    require(int(st["fetches"]) == 2,
            f"stampede.trace should only fetch A and B once each, got fetches={st['fetches']}")
    require(int(st["coalesced_hits"]) >= 2,
            f"stampede.trace expected coalesced_hits >= 2, got {st['coalesced_hits']}")

    # 3. Throttle & concurrent fetch barrier check
    _ev, _req, st = run_trace("throttle.trace", 1024, 6, 2, "best", "clock",
                              extra_args=("--barrier-start", "--sync-fetches", "2"))
    require(st["peak_fetches"] == "2/2" and st["fetches"] == "6",
            f"throttle.trace expected peak_fetches=2/2 and fetches=6, got {st}")

    # 4. Parallel hits during slow fetch check
    events, _req, _st = run_trace("parallel_hits.trace", 1024, 4, 2, "first", "fifo",
                                  extra_args=("--barrier-start",))
    fetch_start_idx = next(i for i, k, d in events if k == "FETCH_START" and d["key"] == "slow_obj")
    fetch_done_idx = next(i for i, k, d in events if k == "FETCH_DONE" and d["key"] == "slow_obj")
    fast_hit_indices = [i for i, k, d in events if k == "HIT" and d["key"] == "fast_obj"]
    require(any(fetch_start_idx < h < fetch_done_idx for h in fast_hit_indices),
            f"parallel_hits.trace: fast_obj HIT did not complete while slow_obj fetch was in flight: {events}")

    # 5. Fetch failure & oversized fetch wakeup check
    _ev, _req, st = run_trace("fetch_fail.trace", 1024, 4, 2, "first", "clock",
                              extra_args=("--barrier-start",))
    require(int(st["fetch_fails"]) >= 1 and int(st["rejects"]) >= 1,
            f"fetch_fail.trace expected fetch_fails>=1 and rejects>=1, got {st}")

    # 6. Shutdown liveness check
    run_trace("shutdown.trace", 1024, 4, 2, "first", "clock",
              extra_args=("--barrier-start",))

    # 7. Multi-client stress runs
    iterations = 20 if stress_mode else 2
    for _ in range(iterations):
        for clients in (2, 4, 8):
            for max_f in (1, 2, 4):
                run_trace("stress.trace", 1024, clients, max_f, "best", "clock",
                          extra_args=("--jitter",))

    print(f"All P3 cache concurrency checks passed ({'stress' if stress_mode else 'standard'} mode).")


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, ValueError, subprocess.TimeoutExpired) as err:
        print(f"P3 check failed: {err}", file=sys.stderr)
        sys.exit(1)
