#define _POSIX_C_SOURCE 200809L

#include "cachelab.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <stdarg.h>
#include <errno.h>

static int set_errmsg(char **errmsg, const char *fmt, ...) {
    char buf[256];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    buf[sizeof buf - 1] = '\0';
    *errmsg = strdup(buf);
    return -1;
}

static int parse_size(const char *s, size_t *out, char **endpos) {
    unsigned long long val;
    char *ep = NULL;

    errno = 0;
    val = strtoull(s, &ep, 10);
    if (ep == s || !isdigit((unsigned char)*s) ||
        (*ep != '\0' && !isspace((unsigned char)*ep) && *ep != '\n' && *ep != '\r')) {
        *endpos = ep;
        return -1;
    }
    if (errno == ERANGE || val > (unsigned long long)(SIZE_MAX)) {
        *endpos = ep;
        return -1;
    }
    *out = (size_t)val;
    *endpos = ep;
    return 0;
}

static int valid_key(const char *s, size_t len) {
    if (len == 0 || len > KEY_MAX)
        return 0;
    for (size_t i = 0; i < len; i++) {
        char c = s[i];
        if (!((c >= 'A' && c <= 'Z') ||
              (c >= 'a' && c <= 'z') ||
              (c >= '0' && c <= '9') ||
              c == '.' || c == '_' || c == '-'))
            return 0;
    }
    return 1;
}

static request_t *append_request(request_t **requests, size_t *cap, size_t *count) {
    if (*count >= *cap) {
        size_t ncap = *cap ? (*cap * 2) : 64;
        request_t *tmp = realloc(*requests, ncap * sizeof(request_t));
        if (!tmp)
            return NULL;
        *requests = tmp;
        *cap = ncap;
    }
    request_t *rq = &(*requests)[*count];
    memset(rq, 0, sizeof(*rq));
    rq->sequence = *count;
    (*count)++;
    return rq;
}

int parse_trace(const char *path, request_t **requests_out,
                size_t *count_out, char **errmsg) {
    FILE *f = fopen(path, "r");
    if (!f) {
        set_errmsg(errmsg, "cannot open trace file");
        return -1;
    }

    request_t *requests = NULL;
    size_t cap = 0, count = 0;
    char *line = NULL;
    size_t line_cap = 0;
    int lineno = 0;

    while (1) {
        ssize_t r = getline(&line, &line_cap, f);
        if (r < 0)
            break;
        lineno++;

        size_t len = (size_t)r;
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = '\0';

        const char *p = line;
        while (*p && isspace((unsigned char)*p))
            p++;
        if (*p == '\0' || *p == '#')
            continue;

        if (strncmp(p, "BACKING ", 8) == 0 || strncmp(p, "BACKING\t", 8) == 0) {
            p += 8;
            while (*p && isspace((unsigned char)*p))
                p++;
            const char *key_start = p;
            size_t key_len = 0;
            while (*p && !isspace((unsigned char)*p)) {
                p++;
                key_len++;
            }
            if (!valid_key(key_start, key_len)) {
                set_errmsg(errmsg, "line %d: invalid key in BACKING", lineno);
                goto fail;
            }
            char key_buf[KEY_BUF];
            memcpy(key_buf, key_start, key_len);
            key_buf[key_len] = '\0';

            while (*p && isspace((unsigned char)*p))
                p++;
            size_t size_val = 0;
            char *end = NULL;
            if (parse_size(p, &size_val, &end) != 0) {
                set_errmsg(errmsg, "line %d: invalid size in BACKING", lineno);
                goto fail;
            }
            p = end;

            int latency_ms = -1;
            int should_fail = 0;

            while (*p) {
                while (*p && isspace((unsigned char)*p))
                    p++;
                if (*p == '\0')
                    break;
                if (strncmp(p, "FAIL", 4) == 0 && (p[4] == '\0' || isspace((unsigned char)p[4]))) {
                    should_fail = 1;
                    p += 4;
                } else if (isdigit((unsigned char)*p)) {
                    size_t lat = 0;
                    if (parse_size(p, &lat, &end) != 0 || lat > INT_MAX) {
                        set_errmsg(errmsg, "line %d: invalid latency in BACKING", lineno);
                        goto fail;
                    }
                    latency_ms = (int)lat;
                    p = end;
                } else {
                    set_errmsg(errmsg, "line %d: unexpected token in BACKING", lineno);
                    goto fail;
                }
            }

            if (backing_register(key_buf, size_val, latency_ms, should_fail) != 0) {
                set_errmsg(errmsg, "line %d: backing_register failed", lineno);
                goto fail;
            }

        } else if (strncmp(p, "GET ", 4) == 0 || strncmp(p, "GET\t", 4) == 0) {
            p += 4;
            while (*p && isspace((unsigned char)*p))
                p++;
            const char *key_start = p;
            size_t key_len = 0;
            while (*p && !isspace((unsigned char)*p)) {
                p++;
                key_len++;
            }
            while (*p && isspace((unsigned char)*p))
                p++;
            if (*p != '\0' || !valid_key(key_start, key_len)) {
                set_errmsg(errmsg, "line %d: malformed GET", lineno);
                goto fail;
            }
            request_t *rq = append_request(&requests, &cap, &count);
            if (!rq) {
                set_errmsg(errmsg, "allocation failure");
                goto fail;
            }
            rq->type = REQUEST_GET;
            memcpy(rq->key, key_start, key_len);
            rq->key[key_len] = '\0';

        } else if (strncmp(p, "PUT ", 4) == 0 || strncmp(p, "PUT\t", 4) == 0) {
            p += 4;
            while (*p && isspace((unsigned char)*p))
                p++;
            const char *key_start = p;
            size_t key_len = 0;
            while (*p && !isspace((unsigned char)*p)) {
                p++;
                key_len++;
            }
            if (!valid_key(key_start, key_len)) {
                set_errmsg(errmsg, "line %d: invalid key in PUT", lineno);
                goto fail;
            }
            while (*p && isspace((unsigned char)*p))
                p++;
            size_t size_val = 0;
            char *end = NULL;
            if (parse_size(p, &size_val, &end) != 0 || size_val == 0) {
                set_errmsg(errmsg, "line %d: invalid size in PUT", lineno);
                goto fail;
            }
            p = end;
            while (*p && isspace((unsigned char)*p))
                p++;
            if (*p != '\0') {
                set_errmsg(errmsg, "line %d: extra fields after PUT", lineno);
                goto fail;
            }
            request_t *rq = append_request(&requests, &cap, &count);
            if (!rq) {
                set_errmsg(errmsg, "allocation failure");
                goto fail;
            }
            rq->type = REQUEST_PUT;
            memcpy(rq->key, key_start, key_len);
            rq->key[key_len] = '\0';
            rq->size = size_val;

        } else if (strncmp(p, "SHUTDOWN", 8) == 0) {
            p += 8;
            while (*p && isspace((unsigned char)*p))
                p++;
            if (*p != '\0') {
                set_errmsg(errmsg, "line %d: extra fields after SHUTDOWN", lineno);
                goto fail;
            }
            request_t *rq = append_request(&requests, &cap, &count);
            if (!rq) {
                set_errmsg(errmsg, "allocation failure");
                goto fail;
            }
            rq->type = REQUEST_SHUTDOWN;
            strcpy(rq->key, "SHUTDOWN");

        } else {
            set_errmsg(errmsg, "line %d: unknown operation", lineno);
            goto fail;
        }
    }

    free(line);
    fclose(f);

    if (count > 0) {
        request_t *tmp = realloc(requests, count * sizeof(request_t));
        if (tmp)
            requests = tmp;
    }

    *requests_out = requests;
    *count_out = count;
    return 0;

fail:
    free(line);
    fclose(f);
    free(requests);
    return -1;
}
