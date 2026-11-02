#define _POSIX_C_SOURCE 200809L

#include "cachelab.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>

/* ---------- Global dispatch state ---------- */

static request_t *dispatch_requests;
static size_t dispatch_count;
static size_t dispatch_next;
static int dispatch_stop;
static int use_start_barrier;
static pthread_mutex_t dispatch_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_barrier_t start_barrier;

/* ---------- Worker routine ---------- */

typedef struct {
    cache_t *cache;
    int client_id;
} worker_arg_t;

static void *worker_run(void *arg) {
    worker_arg_t *wa = (worker_arg_t *)arg;
    cache_t *cache = wa->cache;
    int cid = wa->client_id;

    if (use_start_barrier) {
        pthread_barrier_wait(&start_barrier);
    }

    for (;;) {
        pthread_mutex_lock(&dispatch_lock);
        if (dispatch_stop || dispatch_next >= dispatch_count) {
            pthread_mutex_unlock(&dispatch_lock);
            break;
        }
        request_t *req = &dispatch_requests[dispatch_next++];
        if (req->type == REQUEST_SHUTDOWN) {
            dispatch_stop = 1;
        }
        pthread_mutex_unlock(&dispatch_lock);

        event_context_t ev;
        ev.req_seq = req->sequence;
        ev.client_id = cid;

        if (req->type == REQUEST_GET) {
            handle_get(cache, req, &ev);
        } else if (req->type == REQUEST_PUT) {
            handle_put(cache, req, &ev);
        } else if (req->type == REQUEST_SHUTDOWN) {
            cache_shutdown(cache, &ev);
            break;
        }
    }
    return NULL;
}

static void print_usage(const char *prog) {
    fprintf(stderr,
            "usage: %s <pool_size> <trace_file> <num_clients> "
            "-a first|best -e fifo|clock [-f max_fetches] "
            "[--latency-ms N] [--jitter] [--sync-fetches N] [--barrier-start]\n",
            prog);
}

int main(int argc, char **argv) {
    if (argc < 7) {
        print_usage(argv[0]);
        return 1;
    }

    char *end = NULL;
    errno = 0;
    unsigned long long pool_val = strtoull(argv[1], &end, 10);
    if (end == argv[1] || *end != '\0' || pool_val == 0 ||
        errno == ERANGE || pool_val > SIZE_MAX) {
        fprintf(stderr, "invalid pool_size\n");
        return 1;
    }
    size_t pool_size = (size_t)pool_val;
    char *trace_path = argv[2];

    errno = 0;
    long nc_long = strtol(argv[3], &end, 10);
    if (end == argv[3] || *end != '\0' || nc_long < 1 || nc_long > 16) {
        fprintf(stderr, "invalid num_clients (must be 1-16)\n");
        return 1;
    }
    int num_clients = (int)nc_long;

    alloc_policy_t alloc_pol = ALLOC_FIRST_FIT;
    eviction_policy_t evict_pol = EVICT_FIFO;
    int parsed_alloc = 0, parsed_evict = 0;
    size_t max_fetches = 4;
    int default_latency_ms = DEFAULT_LATENCY_MS;
    int enable_jitter = 0;
    size_t sync_fetches = 0;
    use_start_barrier = 0;

    for (int i = 4; i < argc; i++) {
        if (strcmp(argv[i], "-a") == 0) {
            if (i + 1 >= argc || parsed_alloc) {
                fprintf(stderr, "invalid or duplicate -a option\n");
                return 1;
            }
            const char *val = argv[++i];
            if (strcmp(val, "first") == 0) {
                alloc_pol = ALLOC_FIRST_FIT;
            } else if (strcmp(val, "best") == 0) {
                alloc_pol = ALLOC_BEST_FIT;
            } else {
                fprintf(stderr, "unknown allocation policy: %s\n", val);
                return 1;
            }
            parsed_alloc = 1;
        } else if (strcmp(argv[i], "-e") == 0) {
            if (i + 1 >= argc || parsed_evict) {
                fprintf(stderr, "invalid or duplicate -e option\n");
                return 1;
            }
            const char *val = argv[++i];
            if (strcmp(val, "fifo") == 0) {
                evict_pol = EVICT_FIFO;
            } else if (strcmp(val, "clock") == 0) {
                evict_pol = EVICT_CLOCK;
            } else {
                fprintf(stderr, "unknown eviction policy: %s\n", val);
                return 1;
            }
            parsed_evict = 1;
        } else if (strcmp(argv[i], "-f") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "-f requires max_fetches\n");
                return 1;
            }
            errno = 0;
            unsigned long val = strtoul(argv[++i], &end, 10);
            if (*end != '\0' || val < 1 || val > 64 || errno == ERANGE) {
                fprintf(stderr, "invalid max_fetches\n");
                return 1;
            }
            max_fetches = (size_t)val;
        } else if (strcmp(argv[i], "--latency-ms") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--latency-ms requires a value\n");
                return 1;
            }
            errno = 0;
            long val = strtol(argv[++i], &end, 10);
            if (*end != '\0' || val < 0 || val > 5000 || errno == ERANGE) {
                fprintf(stderr, "invalid --latency-ms\n");
                return 1;
            }
            default_latency_ms = (int)val;
        } else if (strcmp(argv[i], "--jitter") == 0) {
            enable_jitter = 1;
        } else if (strcmp(argv[i], "--sync-fetches") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--sync-fetches requires a value\n");
                return 1;
            }
            errno = 0;
            unsigned long val = strtoul(argv[++i], &end, 10);
            if (*end != '\0' || val < 1 || val > 64 || errno == ERANGE) {
                fprintf(stderr, "invalid --sync-fetches\n");
                return 1;
            }
            sync_fetches = (size_t)val;
        } else if (strcmp(argv[i], "--barrier-start") == 0) {
            use_start_barrier = 1;
        } else {
            fprintf(stderr, "unknown option: %s\n", argv[i]);
            return 1;
        }
    }

    if (!parsed_alloc || !parsed_evict) {
        fprintf(stderr, "missing -a or -e policy\n");
        return 1;
    }

    backing_init(max_fetches, default_latency_ms, enable_jitter, sync_fetches);

    request_t *requests = NULL;
    size_t req_count = 0;
    char *errmsg = NULL;
    if (parse_trace(trace_path, &requests, &req_count, &errmsg) != 0) {
        fprintf(stderr, "%s\n", errmsg ? errmsg : "trace parse error");
        free(errmsg);
        backing_destroy();
        return 1;
    }

    cache_t cache;
    if (cache_init(&cache, pool_size, max_fetches, alloc_pol, evict_pol) != 0) {
        fprintf(stderr, "cache_init failed\n");
        free(requests);
        backing_destroy();
        return 1;
    }

    dispatch_requests = requests;
    dispatch_count = req_count;
    dispatch_next = 0;
    dispatch_stop = 0;

    if (use_start_barrier) {
        pthread_barrier_init(&start_barrier, NULL, (unsigned int)num_clients);
    }

    pthread_t *threads = malloc((size_t)num_clients * sizeof(pthread_t));
    worker_arg_t *worker_args = malloc((size_t)num_clients * sizeof(worker_arg_t));
    if (!threads || !worker_args) {
        fprintf(stderr, "thread allocation failed\n");
        free(threads);
        free(worker_args);
        cache_destroy(&cache);
        free(requests);
        backing_destroy();
        return 1;
    }

    int threads_created = 0;
    for (int i = 0; i < num_clients; i++) {
        worker_args[i].cache = &cache;
        worker_args[i].client_id = i;
        int rc = pthread_create(&threads[i], NULL, worker_run, &worker_args[i]);
        if (rc != 0) {
            fprintf(stderr, "thread create failed: %s\n", strerror(rc));
            break;
        }
        threads_created++;
    }

    if (threads_created < num_clients) {
        pthread_mutex_lock(&dispatch_lock);
        dispatch_stop = 1;
        pthread_mutex_unlock(&dispatch_lock);
        event_context_t sys_ev = { .req_seq = 0, .client_id = -1 };
        cache_shutdown(&cache, &sys_ev);
    }

    for (int i = 0; i < threads_created; i++) {
        pthread_join(threads[i], NULL);
    }

    if (use_start_barrier) {
        pthread_barrier_destroy(&start_barrier);
    }

    int exit_code = 0;
    if (threads_created == num_clients) {
        size_t throttle_v = backing_throttle_violations();
        size_t stampede_v = backing_stampede_violations();
        event_context_t stats_ev = { .req_seq = 0, .client_id = 0 };
        output_event(&stats_ev,
                     "STATS hits=%zu coalesced_hits=%zu misses=%zu fetches=%zu "
                     "fetch_fails=%zu peak_fetches=%zu/%zu evictions=%zu "
                     "rejects=%zu final_occupancy=%zu/%zu",
                     cache.hits, cache.coalesced_hits, cache.misses,
                     cache.fetches, cache.fetch_fails,
                     cache.peak_fetches, cache.max_fetches,
                     cache.evictions, cache.rejects,
                     cache.occupancy, cache.pool.size);
        if (throttle_v > 0 || stampede_v > 0) {
            exit_code = 2;
        }
    } else {
        exit_code = 1;
    }

    free(worker_args);
    free(threads);
    cache_destroy(&cache);
    free(requests);
    backing_destroy();
    return exit_code;
}
