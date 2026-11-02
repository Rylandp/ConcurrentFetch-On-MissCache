#include "cachelab.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

static pthread_mutex_t output_lock = PTHREAD_MUTEX_INITIALIZER;

/* ---------- Provided thread-safe event output ---------- */

void output_event(event_context_t *ev, const char *fmt, ...) {
    (void)ev;
    pthread_mutex_lock(&output_lock);
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    putchar('\n');
    fflush(stdout);
    pthread_mutex_unlock(&output_lock);
}

#define UNFINISHED(name) do { \
    fprintf(stderr, "TODO cache.c: implement %s\n", name); \
    exit(EXIT_FAILURE); \
} while (0)

/* ---------- Provided eviction and insertion helpers (caller holds cache lock) ---------- */

cache_entry_t *evict_fifo_select(cache_t *cache) {
    return cache->policy_head;
}

cache_entry_t *evict_clock_select(cache_t *cache) {
    if (!cache->clock_hand)
        return NULL;

    for (;;) {
        cache_entry_t *candidate = cache->clock_hand;
        cache_entry_t *succ = policy_successor(cache, candidate);

        if (candidate->reference_bit == 1) {
            candidate->reference_bit = 0;
            cache->clock_hand = succ;
            continue;
        }

        cache->clock_hand = succ;
        return candidate;
    }
}

static void evict_one(cache_t *cache, cache_entry_t *victim, event_context_t *ev) {
    output_event(ev, "EVICT req=%zu client=%d key=%s addr=%zu size=%zu",
                 ev->req_seq, ev->client_id, victim->key,
                 pool_offset(&cache->pool, victim->payload),
                 victim->requested_size);

    index_remove(cache, victim);
    policy_remove(cache, victim);
    cache->occupancy -= victim->requested_size;
    cache->evictions++;
    pool_free(&cache->pool, victim->payload, ev);
}

/* Allocate space in pool (evicting victims as needed) and insert entry.
 * Caller must hold the cache lock and ensure size <= pool.size - HEADER_SIZE. */
static int allocate_and_insert(cache_t *cache, request_t *req, size_t size,
                               event_context_t *ev) {
    void *payload = pool_try_alloc(&cache->pool, size);
    while (!payload) {
        cache_entry_t *victim = (cache->eviction_policy == EVICT_FIFO)
                                    ? evict_fifo_select(cache)
                                    : evict_clock_select(cache);
        if (!victim)
            break;
        evict_one(cache, victim, ev);
        payload = pool_try_alloc(&cache->pool, size);
    }

    if (!payload)
        return -1;

    cache_entry_t *entry = &req->entry_storage;
    memcpy(entry->key, req->key, KEY_MAX);
    entry->key[KEY_MAX] = '\0';
    entry->requested_size = size;
    entry->payload = payload;
    entry->reference_bit = 1;
    entry->active = 0;

    index_insert(cache, entry);
    policy_append(cache, entry);
    cache->occupancy += size;

    output_event(ev, "ALLOC req=%zu client=%d key=%s addr=%zu size=%zu",
                 ev->req_seq, ev->client_id, req->key,
                 pool_offset(&cache->pool, payload), size);
    return 0;
}

/* ---------- Student work: synchronization and in-flight state ---------- */

struct cache_sync {
    /* TODO: Define your mutex, condition variable(s), in-flight table,
     * active fetch counter, and shutdown flag here. */
    int unused;
};

/* ---------- Student work: cache lifecycle ---------- */

int cache_init(cache_t *cache, size_t pool_size, size_t max_fetches,
               alloc_policy_t alloc_pol, eviction_policy_t evict_pol) {
    /* TODO: Zero cache, initialize cache->pool via pool_init, allocate
     * BUCKET_COUNT hash_buckets, set bucket_count, allocation_policy,
     * eviction_policy, and max_fetches (at least 1), and allocate and
     * initialize cache->sync. Return 0 on success or -1 on error. */
    (void)cache;
    (void)pool_size;
    (void)max_fetches;
    (void)alloc_pol;
    (void)evict_pol;
    UNFINISHED("cache_init");
}

void cache_shutdown(cache_t *cache, event_context_t *ev) {
    /* TODO: Under your synchronization lock, mark the cache as shutting
     * down, emit SHUTDOWN, and broadcast to all threads waiting on the
     * fetch throttle or any in-flight key condition variable. */
    (void)cache;
    (void)ev;
    UNFINISHED("cache_shutdown");
}

void cache_destroy(cache_t *cache) {
    /* TODO: Destroy and free all synchronization primitives and any
     * remaining in-flight entries in cache->sync, call pool_destroy,
     * and free cache->hash_buckets. */
    (void)cache;
    UNFINISHED("cache_destroy");
}

/* ---------- Student work: GET request handler ---------- */

void handle_get(cache_t *cache, request_t *req, event_context_t *ev) {
    /* TODO: Under your cache lock:
     * 1. If shutting down, emit ABORT and return.
     * 2. Fast path: if index_find locates req->key, set reference_bit = 1,
     *    increment hits, emit HIT, unlock, and return.
     * 3. Coalesced path: if req->key is already in flight, emit
     *    COALESCED_WAIT and wait on its condition variable until the fetch
     *    completes or shutdown occurs. Then emit ABORT (on shutdown),
     *    COALESCED_HIT (and increment coalesced_hits / set reference_bit),
     *    or COALESCED_FAIL (if the fetch failed or was rejected).
     * 4. Fetcher path: increment misses, emit MISS, register req->key as
     *    in-flight, and wait on the throttle condition variable while
     *    active_fetches >= cache->max_fetches (checking for shutdown).
     *    Once a slot is claimed, increment active_fetches and fetches,
     *    update peak_fetches, emit FETCH_START, and UNLOCK the cache
     *    before calling backing_fetch(req->key, &fetched_size).
     *    Re-lock after backing_fetch returns, decrement active_fetches,
     *    signal the throttle CV, unlink the in-flight entry, handle
     *    shutdown (ABORT), fetch failure (FETCH_FAIL + fetch_fails++), or
     *    success (FETCH_DONE + size check: REJECT + rejects++ if 0 or
     *    > pool.size - HEADER_SIZE, else allocate_and_insert), broadcast
     *    to coalesced waiters, and unlock. */
    (void)cache;
    (void)req;
    (void)ev;
    (void)allocate_and_insert;
    UNFINISHED("handle_get");
}

/* ---------- Student work: PUT request handler ---------- */

void handle_put(cache_t *cache, request_t *req, event_context_t *ev) {
    /* TODO: Under your cache lock, if an in-flight fetch for req->key is
     * currently active, wait for it to finish (or for shutdown) before
     * mutating the key. If shutting down, emit ABORT and return.
     * Otherwise emit PUT, reject sizes > pool.size - HEADER_SIZE (emit
     * REJECT and increment rejects before touching any existing entry),
     * handle UPDATE if index_find locates an existing entry (emit UPDATE,
     * subtract occupancy, unlink from index and policy list, pool_free),
     * and call allocate_and_insert(cache, req, req->size, ev). */
    (void)cache;
    (void)req;
    (void)ev;
    (void)allocate_and_insert;
    UNFINISHED("handle_put");
}
