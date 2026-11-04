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

typedef enum{
    FETCH_PENDING,
    FETCH_SUCCEEDED,
    FETCH_FAILED
} fetch_result_t;

typedef struct inflight_fetch {
    char key[KEY_BUF];
    pthread_cond_t completed;
    fetch_result_t result;
    size_t references;
    struct inflight_fetch *next;
} inflight_fetch_t;

struct cache_sync {
    /* TODO: Define your mutex, condition variable(s), in-flight table,
     * active fetch counter, and shutdown flag here. */
    pthread_mutex_t lock;
    pthread_cond_t fetch_slot_available;
    inflight_fetch_t *inflight;
    size_t active_fetches;
    int shutting_down;
};

static inflight_fetch_t *find_inflight(cache_sync_t *sync, const char *key) {
    for (inflight_fetch_t *fetch = sync->inflight;
        fetch != NULL;
        fetch = fetch->next) {
            if (strcmp(fetch->key, key) == 0) {
                return fetch;
            }
        }
    return NULL;
}

static void unlink_inflight(cache_sync_t *sync, inflight_fetch_t *target) {
    inflight_fetch_t **link = &sync->inflight;

    while (*link != NULL && *link != target) {
        link = &(*link)->next;
    }

    if (*link == target) {
        *link = target->next;
        target->next = NULL;
    }
}

static void release_inflight(inflight_fetch_t *fetch) {
    fetch->references--;

    if (fetch->references == 0) {
        pthread_cond_destroy(&fetch->completed);
        free(fetch);
    }
}

/* ---------- Student work: cache lifecycle ---------- */

int cache_init(cache_t *cache, size_t pool_size, size_t max_fetches,
               alloc_policy_t alloc_pol, eviction_policy_t evict_pol) {
    /* TODO: Zero cache, initialize cache->pool via pool_init, allocate
     * BUCKET_COUNT hash_buckets, set bucket_count, allocation_policy,
     * eviction_policy, and max_fetches (at least 1), and allocate and
     * initialize cache->sync. Return 0 on success or -1 on error. */
    memset(cache, 0, sizeof(*cache));

    if(pool_init(&cache->pool, pool_size, alloc_pol) != 0) {
        return -1;
    }
    
    cache->hash_buckets = calloc(BUCKET_COUNT, sizeof(*cache->hash_buckets));
    if(!cache->hash_buckets) {
        pool_destroy(&cache->pool);
        return -1;
    }
    cache->bucket_count = BUCKET_COUNT;

    cache->sync = calloc(1, sizeof(*cache->sync));
    if(!cache->sync) {
        free(cache->hash_buckets);
        cache->hash_buckets = NULL;
        pool_destroy(&cache->pool);
        return -1;
    }

    int rc = pthread_mutex_init(&cache->sync->lock, NULL);
    if (rc != 0) {
        free(cache->sync);
        cache->sync = NULL;
        free(cache->hash_buckets);
        cache->hash_buckets = NULL;
        pool_destroy(&cache->pool);
        return -1;
    }

    rc = pthread_cond_init(&cache->sync->fetch_slot_available, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&cache->sync->lock);
        free(cache->sync);
        cache->sync = NULL;
        free(cache->hash_buckets);
        cache->hash_buckets = NULL;
        pool_destroy(&cache->pool);
        return -1;
    }

    cache->allocation_policy = alloc_pol;
    cache->eviction_policy = evict_pol;
    cache->max_fetches = max_fetches > 0 ? max_fetches : 1;

    return 0;
}

void cache_shutdown(cache_t *cache, event_context_t *ev) {
    /* TODO: Under your synchronization lock, mark the cache as shutting
     * down, emit SHUTDOWN, and broadcast to all threads waiting on the
     * fetch throttle or any in-flight key condition variable. */
    pthread_mutex_lock(&cache->sync->lock);
    cache->sync->shutting_down = 1;
    output_event(ev, "SHUTDOWN req=%zu client=%d", ev->req_seq, ev->client_id);
    pthread_cond_broadcast(&cache->sync->fetch_slot_available);
    for (inflight_fetch_t *fetch = cache->sync->inflight; fetch != NULL; fetch = fetch->next) {
        pthread_cond_broadcast(&fetch->completed);
    }
    pthread_mutex_unlock(&cache->sync->lock);
    
}

void cache_destroy(cache_t *cache) {
    /* TODO: Destroy and free all synchronization primitives and any
     * remaining in-flight entries in cache->sync, call pool_destroy,
     * and free cache->hash_buckets. */
    if (cache->sync) {
        pthread_mutex_destroy(&cache->sync->lock);
        pthread_cond_destroy(&cache->sync->fetch_slot_available);
        inflight_fetch_t *fetch = cache->sync->inflight;
        while (fetch) {
            inflight_fetch_t *next = fetch->next;
            pthread_cond_destroy(&fetch->completed);
            free(fetch);
            fetch = next;
        }
        free(cache->sync);
        cache->sync = NULL;
    }
    if (cache->hash_buckets) {
        free(cache->hash_buckets);
        cache->hash_buckets = NULL;
    }
    pool_destroy(&cache->pool);
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
    pthread_mutex_lock(&cache->sync->lock);

    if (cache->sync->shutting_down) {
        output_event(ev, "ABORT req=%zu client=%d key=%s",
                     ev->req_seq, ev->client_id, req->key);
        pthread_mutex_unlock(&cache->sync->lock);
        return;
    }

    cache_entry_t *entry = index_find(cache, req->key);
    if (entry != NULL) {
        entry->reference_bit = 1;
        cache->hits++;
        output_event(ev, "HIT req=%zu client=%d key=%s",
                     ev->req_seq, ev->client_id, req->key);
        pthread_mutex_unlock(&cache->sync->lock);
        return;
    }

    inflight_fetch_t *fetch = find_inflight(cache->sync, req->key);
    if (fetch != NULL) {
        fetch->references++;

        output_event(ev, "COALESCED_WAIT req=%zu client=%d key=%s",
                     ev->req_seq, ev->client_id, req->key);

        while (fetch->result == FETCH_PENDING &&
               !cache->sync->shutting_down) {
            pthread_cond_wait(&fetch->completed, &cache->sync->lock);
        }

        if (cache->sync->shutting_down) {
            output_event(ev, "ABORT req=%zu client=%d key=%s",
                         ev->req_seq, ev->client_id, req->key);
        } else if (fetch->result == FETCH_SUCCEEDED) {
            entry = index_find(cache, req->key);
            if (entry != NULL) {
                entry->reference_bit = 1;
            }

            cache->coalesced_hits++;
            output_event(ev, "COALESCED_HIT req=%zu client=%d key=%s",
                         ev->req_seq, ev->client_id, req->key);
        } else {
            output_event(ev, "COALESCED_FAIL req=%zu client=%d key=%s",
                         ev->req_seq, ev->client_id, req->key);
        }

        release_inflight(fetch);
        pthread_mutex_unlock(&cache->sync->lock);
        return;
    }

    cache->misses++;
    output_event(ev, "MISS req=%zu client=%d key=%s",
                 ev->req_seq, ev->client_id, req->key);

    fetch = calloc(1, sizeof(*fetch));
    if (fetch == NULL) {
        cache->fetch_fails++;
        output_event(ev, "FETCH_FAIL req=%zu client=%d key=%s",
                     ev->req_seq, ev->client_id, req->key);
        pthread_mutex_unlock(&cache->sync->lock);
        return;
    }

    snprintf(fetch->key, sizeof(fetch->key), "%s", req->key);
    fetch->result = FETCH_PENDING;
    fetch->references = 1; /* The fetcher owns this reference. */

    int rc = pthread_cond_init(&fetch->completed, NULL);
    if (rc != 0) {
        free(fetch);
        cache->fetch_fails++;
        output_event(ev, "FETCH_FAIL req=%zu client=%d key=%s",
                     ev->req_seq, ev->client_id, req->key);
        pthread_mutex_unlock(&cache->sync->lock);
        return;
    }

    fetch->next = cache->sync->inflight;
    cache->sync->inflight = fetch;

    while (cache->sync->active_fetches >= cache->max_fetches &&
           !cache->sync->shutting_down) {
        pthread_cond_wait(&cache->sync->fetch_slot_available,
                          &cache->sync->lock);
    }

    if (cache->sync->shutting_down) {
        fetch->result = FETCH_FAILED;
        unlink_inflight(cache->sync, fetch);
        pthread_cond_broadcast(&fetch->completed);

        output_event(ev, "ABORT req=%zu client=%d key=%s",
                     ev->req_seq, ev->client_id, req->key);

        release_inflight(fetch);
        pthread_mutex_unlock(&cache->sync->lock);
        return;
    }

    cache->sync->active_fetches++;
    cache->fetches++;

    if (cache->sync->active_fetches > cache->peak_fetches) {
        cache->peak_fetches = cache->sync->active_fetches;
    }

    output_event(ev, "FETCH_START req=%zu client=%d key=%s",
                 ev->req_seq, ev->client_id, req->key);

    pthread_mutex_unlock(&cache->sync->lock);

    size_t fetched_size = 0;
    int fetch_rc = backing_fetch(req->key, &fetched_size);

    pthread_mutex_lock(&cache->sync->lock);

    cache->sync->active_fetches--;
    pthread_cond_signal(&cache->sync->fetch_slot_available);

    if (cache->sync->shutting_down) {
        fetch->result = FETCH_FAILED;
        output_event(ev, "ABORT req=%zu client=%d key=%s",
                     ev->req_seq, ev->client_id, req->key);
    } else if (fetch_rc != 0) {
        fetch->result = FETCH_FAILED;
        cache->fetch_fails++;
        output_event(ev, "FETCH_FAIL req=%zu client=%d key=%s",
                     ev->req_seq, ev->client_id, req->key);
    } else {
        output_event(ev, "FETCH_DONE req=%zu client=%d key=%s size=%zu",
                     ev->req_seq, ev->client_id, req->key, fetched_size);

        if (fetched_size == 0 ||
            fetched_size > cache->pool.size - HEADER_SIZE) {
            fetch->result = FETCH_FAILED;
            cache->rejects++;
            output_event(ev, "REJECT req=%zu client=%d key=%s size=%zu",
                         ev->req_seq, ev->client_id, req->key, fetched_size);
        } else if (allocate_and_insert(cache, req, fetched_size, ev) != 0) {
            fetch->result = FETCH_FAILED;
            cache->rejects++;
            output_event(ev, "REJECT req=%zu client=%d key=%s size=%zu",
                         ev->req_seq, ev->client_id, req->key, fetched_size);
        } else {
            fetch->result = FETCH_SUCCEEDED;
        }
    }

    unlink_inflight(cache->sync, fetch);
    pthread_cond_broadcast(&fetch->completed);
    release_inflight(fetch);

    pthread_mutex_unlock(&cache->sync->lock);
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
    pthread_mutex_lock(&cache->sync->lock);

    while (!cache->sync->shutting_down) {
        inflight_fetch_t *fetch = find_inflight(cache->sync, req->key);
        if (fetch == NULL) {
            break;
        }

        fetch->references++;

        while (fetch->result == FETCH_PENDING &&
               !cache->sync->shutting_down) {
            pthread_cond_wait(&fetch->completed, &cache->sync->lock);
        }

        release_inflight(fetch);
    }

    if (cache->sync->shutting_down) {
        output_event(ev, "ABORT req=%zu client=%d key=%s",
                     ev->req_seq, ev->client_id, req->key);
        pthread_mutex_unlock(&cache->sync->lock);
        return;
    }

    output_event(ev, "PUT req=%zu client=%d key=%s size=%zu",
                 ev->req_seq, ev->client_id, req->key, req->size);

    if (req->size > cache->pool.size - HEADER_SIZE) {
        cache->rejects++;
        output_event(ev, "REJECT req=%zu client=%d key=%s size=%zu",
                     ev->req_seq, ev->client_id, req->key, req->size);
        pthread_mutex_unlock(&cache->sync->lock);
        return;
    }

    cache_entry_t *entry = index_find(cache, req->key);
    if (entry != NULL) {
        output_event(ev, "UPDATE req=%zu client=%d key=%s size=%zu",
                     ev->req_seq, ev->client_id, req->key, req->size);

        cache->occupancy -= entry->requested_size;
        index_remove(cache, entry);
        policy_remove(cache, entry);
        pool_free(&cache->pool, entry->payload, ev);
    }

    if (allocate_and_insert(cache, req, req->size, ev) != 0) {
        cache->rejects++;
        output_event(ev, "REJECT req=%zu client=%d key=%s size=%zu",
                     ev->req_seq, ev->client_id, req->key, req->size);
    }

    pthread_mutex_unlock(&cache->sync->lock);
}
