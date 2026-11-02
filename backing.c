#define _POSIX_C_SOURCE 200809L

#include "cachelab.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <sched.h>
#include <pthread.h>

typedef struct active_fetch_node {
    char key[KEY_BUF];
    struct active_fetch_node *next;
} active_fetch_node_t;

static pthread_mutex_t backing_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t sync_cv = PTHREAD_COND_INITIALIZER;

static backing_spec_t *catalog = NULL;
static size_t catalog_count = 0;
static size_t catalog_cap = 0;

static size_t cfg_max_fetches = 1;
static int cfg_default_latency_ms = DEFAULT_LATENCY_MS;
static int cfg_enable_jitter = 0;
static size_t cfg_sync_target = 0;

static size_t active_count = 0;
static size_t throttle_errs = 0;
static size_t stampede_errs = 0;
static size_t sync_arrived = 0;
static int sync_released = 0;
static unsigned int jitter_seed = 42;

static active_fetch_node_t *active_keys_head = NULL;

static void sleep_us(long microseconds) {
    if (microseconds <= 0)
        return;
    struct timespec ts = {
        .tv_sec = microseconds / 1000000L,
        .tv_nsec = (microseconds % 1000000L) * 1000L
    };
    while (nanosleep(&ts, &ts) == -1 && errno == EINTR) {
    }
}

void backing_init(size_t max_fetches, int default_latency_ms,
                  int enable_jitter, size_t sync_fetches_target) {
    pthread_mutex_lock(&backing_lock);
    cfg_max_fetches = max_fetches > 0 ? max_fetches : 1;
    cfg_default_latency_ms = default_latency_ms >= 0 ? default_latency_ms : DEFAULT_LATENCY_MS;
    cfg_enable_jitter = enable_jitter;
    cfg_sync_target = sync_fetches_target;
    active_count = 0;
    throttle_errs = 0;
    stampede_errs = 0;
    sync_arrived = 0;
    sync_released = 0;
    pthread_mutex_unlock(&backing_lock);
}

void backing_destroy(void) {
    pthread_mutex_lock(&backing_lock);
    free(catalog);
    catalog = NULL;
    catalog_count = 0;
    catalog_cap = 0;
    while (active_keys_head) {
        active_fetch_node_t *tmp = active_keys_head;
        active_keys_head = active_keys_head->next;
        free(tmp);
    }
    pthread_mutex_unlock(&backing_lock);
}

int backing_register(const char *key, size_t size, int latency_ms, int should_fail) {
    pthread_mutex_lock(&backing_lock);
    for (size_t i = 0; i < catalog_count; i++) {
        if (strcmp(catalog[i].key, key) == 0) {
            catalog[i].size = size;
            catalog[i].latency_ms = latency_ms;
            catalog[i].should_fail = should_fail;
            pthread_mutex_unlock(&backing_lock);
            return 0;
        }
    }

    if (catalog_count >= catalog_cap) {
        size_t ncap = catalog_cap ? catalog_cap * 2 : 32;
        backing_spec_t *tmp = realloc(catalog, ncap * sizeof(*catalog));
        if (!tmp) {
            pthread_mutex_unlock(&backing_lock);
            return -1;
        }
        catalog = tmp;
        catalog_cap = ncap;
    }

    backing_spec_t *entry = &catalog[catalog_count++];
    memset(entry, 0, sizeof(*entry));
    strncpy(entry->key, key, KEY_MAX);
    entry->key[KEY_MAX] = '\0';
    entry->size = size;
    entry->latency_ms = latency_ms;
    entry->should_fail = should_fail;
    pthread_mutex_unlock(&backing_lock);
    return 0;
}

int backing_fetch(const char *key, size_t *size_out) {
    pthread_mutex_lock(&backing_lock);

    active_count++;
    if (active_count > cfg_max_fetches) {
        throttle_errs++;
        fprintf(stderr, "THROTTLE_VIOLATION: active=%zu exceeds max_fetches=%zu (key=%s)\n",
                active_count, cfg_max_fetches, key);
    }

    for (active_fetch_node_t *cur = active_keys_head; cur != NULL; cur = cur->next) {
        if (strcmp(cur->key, key) == 0) {
            stampede_errs++;
            fprintf(stderr, "STAMPEDE_VIOLATION: duplicate concurrent fetch for key=%s\n", key);
            break;
        }
    }

    active_fetch_node_t *my_node = calloc(1, sizeof(*my_node));
    if (my_node) {
        strncpy(my_node->key, key, KEY_MAX);
        my_node->key[KEY_MAX] = '\0';
        my_node->next = active_keys_head;
        active_keys_head = my_node;
    }

    /* Optional rendezvous barrier for testing concurrent fetches */
    if (cfg_sync_target > 1 && !sync_released) {
        sync_arrived++;
        if (sync_arrived >= cfg_sync_target) {
            sync_released = 1;
            pthread_cond_broadcast(&sync_cv);
        } else {
            struct timespec deadline;
            clock_gettime(CLOCK_REALTIME, &deadline);
            deadline.tv_sec += 1;
            deadline.tv_nsec += 500000000L;
            if (deadline.tv_nsec >= 1000000000L) {
                deadline.tv_sec += 1;
                deadline.tv_nsec -= 1000000000L;
            }
            while (!sync_released) {
                int rc = pthread_cond_timedwait(&sync_cv, &backing_lock, &deadline);
                if (rc == ETIMEDOUT && !sync_released) {
                    sync_released = 1;
                    throttle_errs++;
                    fprintf(stderr,
                            "SYNC_FETCH_TIMEOUT: expected %zu concurrent fetches, only %zu arrived "
                            "(is cache_lock held across backing_fetch?)\n",
                            cfg_sync_target, sync_arrived);
                    pthread_cond_broadcast(&sync_cv);
                    break;
                }
            }
        }
    }

    /* Look up key metadata in backing catalog */
    size_t obj_size = DEFAULT_FETCH_SIZE;
    int latency_ms = cfg_default_latency_ms;
    int should_fail = 0;

    for (size_t i = 0; i < catalog_count; i++) {
        if (strcmp(catalog[i].key, key) == 0) {
            obj_size = catalog[i].size;
            if (catalog[i].latency_ms >= 0)
                latency_ms = catalog[i].latency_ms;
            should_fail = catalog[i].should_fail;
            break;
        }
    }

    long jitter_us = 0;
    if (cfg_enable_jitter) {
        jitter_seed = jitter_seed * 1103515245u + 12345u;
        jitter_us = (long)((jitter_seed >> 16) % 1500) + 100;
    }

    pthread_mutex_unlock(&backing_lock);

    /* Simulate backing store I/O latency outside any lock */
    if (cfg_enable_jitter) {
        sched_yield();
        sleep_us(jitter_us);
    }
    if (latency_ms > 0) {
        sleep_us((long)latency_ms * 1000L);
    }
    if (cfg_enable_jitter) {
        sched_yield();
    }

    pthread_mutex_lock(&backing_lock);
    if (my_node) {
        active_fetch_node_t **pp = &active_keys_head;
        while (*pp) {
            if (*pp == my_node) {
                *pp = my_node->next;
                free(my_node);
                break;
            }
            pp = &(*pp)->next;
        }
    }
    active_count--;
    pthread_mutex_unlock(&backing_lock);

    if (should_fail)
        return -1;

    *size_out = obj_size;
    return 0;
}

size_t backing_throttle_violations(void) {
    pthread_mutex_lock(&backing_lock);
    size_t v = throttle_errs;
    pthread_mutex_unlock(&backing_lock);
    return v;
}

size_t backing_stampede_violations(void) {
    pthread_mutex_lock(&backing_lock);
    size_t v = stampede_errs;
    pthread_mutex_unlock(&backing_lock);
    return v;
}
