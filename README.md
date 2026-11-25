# async-cachelab

A multithreaded C11 concurrent fetch-on-miss cache (`cachelab`) with request coalescing, fetch throttling, FIFO/CLOCK eviction, first-fit/best-fit pool allocation, and graceful shutdown[cite: 13, 14, 17, 19, 20].

## Architecture & Concurrency

- **`cache.c` (`cache_init`, `cache_shutdown`, `cache_destroy`, `handle_get`, `handle_put`)**:
  - Uses `cache_sync_t` (`pthread_mutex_t lock`, `pthread_cond_t fetch_slot_available`, `shutting_down`, `active_fetches`, and an `inflight_fetch_t` list) to coordinate concurrent client threads[cite: 17, 20].
  - **Fast path (`GET`)**: Checks `index_find()` under the lock; on hit, sets `reference_bit = 1`, increments `hits`, and logs `HIT`[cite: 17].
  - **Coalescing (`GET` & `PUT`)**: Concurrent `GET` requests for an already in-flight key wait on `fetch->completed` (`COALESCED_WAIT`), then resolve to `COALESCED_HIT` or `COALESCED_FAIL` (or `ABORT` on shutdown)[cite: 17]. `PUT` requests wait for any active in-flight fetch on the same key before mutating or replacing entries[cite: 17].
  - **Throttling & Non-Blocking I/O**: Enforces `max_fetches` via `fetch_slot_available`, tracks `peak_fetches`, and releases `sync->lock` while calling `backing_fetch()` so slow backing I/O does not block cache hits, coalesced waiters, or other fetches[cite: 17].
  - **Eviction & Allocation**: Supports `EVICT_FIFO` and `EVICT_CLOCK` (`evict_fifo_select`, `evict_clock_select`) alongside `ALLOC_FIRST_FIT` and `ALLOC_BEST_FIT` in `memory_pool_t`, rejecting oversized or zero-byte objects (`REJECT`)[cite: 17, 18, 20].
- **`index.c`**: Implements a 257-bucket 64-bit FNV-1a hash table (`index_find`, `index_insert`, `index_remove`) and doubly linked policy list (`policy_append`, `policy_remove`, `policy_successor`) with `clock_hand` maintenance[cite: 20, 21].
- **`backing.c`**: Simulates backing-store catalog lookups, configurable latency, optional jitter (`sched_yield` + microsecond sleep), optional rendezvous barriers (`sync_fetches_target`), and assertions/counters for throttle and stampede violations[cite: 15].
- **`pool.c`, `trace.c`, `main.c`**: Manage the byte memory pool, parse trace files (`GET`, `PUT`, `BACKING`, `SHUTDOWN`), and drive worker client threads with optional start barriers[cite: 19, 20].

## Requirements

- **C11 compiler** (`gcc` with `-std=c11 -Wall -Wextra -Werror -g -O2 -pthread`) and `make`[cite: 13]
- **Python 3** (for `make test` / `make test-stress` via `tests/check_p3.py`)[cite: 13]
- **Valgrind** (optional, for `make test-helgrind` and `make test-valgrind`)[cite: 13]

## Building and Running

### Build

```bash
make
```

### Run `cachelab`

```bash
./cachelab <pool_size> <trace_file> <num_clients> -a first|best -e fifo|clock [-f max_fetches] [--latency-ms N] [--jitter] [--sync-fetches N] [--barrier-start]
```

Example:

```bash
./cachelab 1024 traces/sample.trace 4 -a best -e clock -f 2 --latency-ms 5 --jitter
```

### Test & Sanitizer Targets

```bash
make test           # Runs python3 tests/check_p3.py
make test-stress    # Runs python3 tests/check_p3.py --stress
make test-helgrind  # Runs Helgrind race checks on stampede and stress traces
make test-valgrind  # Runs Valgrind full leak checks on stress and shutdown traces
make clean          # Removes object files and cachelab binary
```[cite: 13]

## Project Structure

```text
.
├── Makefile     # Builds cachelab and defines test, stress, helgrind, valgrind, and clean targets
├── cachelab.h   # Shared data structures, enums, constants, and module interfaces
├── main.c       # CLI parsing, trace setup, and client thread orchestration
├── trace.c      # Trace file parser
├── backing.c    # Simulated backing store with latency, jitter, and violation tracking
├── pool.c       # Block allocator and memory pool management
├── index.c      # FNV-1a hash index and eviction policy doubly linked list
└── cache.c      # Cache lifecycle, synchronization, coalescing, eviction, and GET/PUT handlers
```[cite: 12, 13, 15, 17, 20, 21]
```
