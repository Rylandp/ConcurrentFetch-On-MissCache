#include "cachelab.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdalign.h>

/* ---------- Internal helpers ---------- */

static inline size_t round_up(size_t val, size_t align) {
    return (val + align - 1) & ~(align - 1);
}

static inline block_header_t *header_from_payload(void *payload) {
    return (block_header_t *)((unsigned char *)payload - HEADER_SIZE);
}

static inline void *payload_from_header(block_header_t *h) {
    return (void *)((unsigned char *)h + HEADER_SIZE);
}

/* ---------- Pool init ---------- */

int pool_init(memory_pool_t *pool, size_t size, alloc_policy_t policy) {
    if (size < MIN_BLOCK_SIZE)
        return -1;

    pool->bytes = (unsigned char *)malloc(size);
    if (!pool->bytes)
        return -1;

    block_header_t *first = (block_header_t *)pool->bytes;
    first->size = size - HEADER_SIZE;
    first->free = 1;
    first->next = NULL;
    pool->blocks = first;
    pool->size = size;
    pool->policy = policy;
    return 0;
}

/* ---------- Block selection ---------- */

static block_header_t *first_fit(memory_pool_t *pool, size_t requested) {
    block_header_t *cur = pool->blocks;
    while (cur) {
        if (cur->free && cur->size >= requested)
            return cur;
        cur = cur->next;
    }
    return NULL;
}

static block_header_t *best_fit(memory_pool_t *pool, size_t requested) {
    block_header_t *cur = pool->blocks;
    block_header_t *best = NULL;
    size_t best_cap = 0;

    while (cur) {
        if (cur->free && cur->size >= requested) {
            if (!best || cur->size < best_cap) {
                best = cur;
                best_cap = cur->size;
            }
        }
        cur = cur->next;
    }
    return best;
}

/* ---------- Splitting ---------- */

static void split_if_possible(block_header_t *block, size_t requested) {
    size_t physical = round_up(requested, HEADER_ALIGNMENT);
    if (physical > block->size)
        return;
    if (block->size - physical < MIN_BLOCK_SIZE)
        return;

    block_header_t *new_h = (block_header_t *)((unsigned char *)payload_from_header(block) + physical);
    new_h->size = block->size - physical - HEADER_SIZE;
    new_h->free = 1;
    new_h->next = block->next;
    block->size = physical;
    block->next = new_h;
}

/* ---------- Try alloc ---------- */

void *pool_try_alloc(memory_pool_t *pool, size_t requested) {
    block_header_t *block = NULL;
    switch (pool->policy) {
        case ALLOC_FIRST_FIT:
            block = first_fit(pool, requested);
            break;
        case ALLOC_BEST_FIT:
            block = best_fit(pool, requested);
            break;
        default:
            return NULL;
    }
    if (!block)
        return NULL;

    split_if_possible(block, requested);
    block->free = 0;
    return payload_from_header(block);
}

/* ---------- Free and coalesce ---------- */

void pool_free(memory_pool_t *pool, void *payload, event_context_t *ev) {
    if (!payload)
        return;

    block_header_t *cur = header_from_payload(payload);
    cur->free = 1;

    /* Forward coalesce */
    block_header_t *nxt = cur->next;
    if (nxt && nxt->free) {
        size_t old_cap = cur->size;
        cur->size = cur->size + HEADER_SIZE + nxt->size;
        cur->next = nxt->next;
        output_event(ev, "COALESCE req=%zu client=%d addr=%zu size=%zu",
                     ev->req_seq, ev->client_id,
                     pool_offset(pool, payload_from_header(cur)),
                     cur->size);
        (void)old_cap;
    }

    /* Backward coalesce: find previous block by walking from head */
    block_header_t *prev = NULL;
    block_header_t *walk = pool->blocks;
    while (walk && walk->next != cur) {
        walk = walk->next;
    }
    prev = walk;
    if (prev && prev != cur && prev->free) {
        size_t old_cap = prev->size;
        prev->size = prev->size + HEADER_SIZE + cur->size;
        prev->next = cur->next;
        output_event(ev, "COALESCE req=%zu client=%d addr=%zu size=%zu",
                     ev->req_seq, ev->client_id,
                     pool_offset(pool, payload_from_header(prev)),
                     prev->size);
        (void)old_cap;
    }
}

/* ---------- Utility ---------- */

size_t pool_offset(memory_pool_t *pool, void *payload) {
    return (size_t)((unsigned char *)payload - pool->bytes);
}

size_t pool_payload_capacity(void *payload) {
    block_header_t *h = header_from_payload(payload);
    return h->size;
}

void pool_inspect(memory_pool_t *pool, size_t *free_payload,
                  size_t *free_blocks, size_t *largest_free) {
    *free_payload = 0;
    *free_blocks = 0;
    *largest_free = 0;
    block_header_t *cur = pool->blocks;
    while (cur) {
        if (cur->free) {
            (*free_blocks)++;
            *free_payload += cur->size;
            if (cur->size > *largest_free)
                *largest_free = cur->size;
        }
        cur = cur->next;
    }
}

void print_pool(memory_pool_t *pool) {
    fprintf(stderr, "Pool %p size=%zu\n", (void *)pool->bytes, pool->size);
    block_header_t *cur = pool->blocks;
    while (cur) {
        fprintf(stderr, "  block=%p size=%zu free=%d next=%p\n",
                (void *)cur, cur->size, cur->free, (void *)cur->next);
        cur = cur->next;
    }
}

void pool_destroy(memory_pool_t *pool) {
    free(pool->bytes);
    pool->bytes = NULL;
    pool->blocks = NULL;
}
