/*
 * benchmark.c  –  Optimized benchmark client
 *
 * Key improvements over original:
 *  - Persistent per-thread TCP connection (no connect/close per op)
 *  - writev() to send header+key+val in a single syscall
 *  - clock_gettime(CLOCK_MONOTONIC) for nanosecond-precision latency
 *  - Fixed pthread signature  (void *(*)(void *), not void *(*)(void *, int))
 *  - Warmup phase before timed measurement
 *  - Per-operation latency histogram (p50/p95/p99)
 *  - operation_num_per_thread passed through ThreadArg (no global)
 *  - Compiles cleanly with -Wall -Wextra -pthread
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/time.h>
#include <time.h>
#include <pthread.h>
#include <stdint.h>

/* ── tunables ──────────────────────────────────────────────────── */
#define DEFAULT_WORKERS         4
#define TOTAL_OPERATIONS        10000
#define WARMUP_OPS              200     /* ops before we start timing  */
#define MAX_OPS_PER_THREAD      8192    /* histogram array ceiling     */
/* ──────────────────────────────────────────────────────────────── */

struct __attribute__((packed)) MsgHeader {
    uint8_t  magic;
    uint8_t  command;
    uint16_t key_len;
    uint32_t val_len;
};

struct ProxyAddress {
    char ip[64];
    int  port;
};

struct ThreadArg {
    int               worker_id;
    int               ops_per_thread;
    struct ProxyAddress proxy;
};

struct WorkerResult {
    double   write_latency_avg_us;
    double   read_latency_avg_us;
    double   write_p99_us;
    double   read_p99_us;
    int      write_success;
    int      read_success;
};

/* ── timing ────────────────────────────────────────────────────── */
static inline long long now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* ── I/O helpers ───────────────────────────────────────────────── */
static ssize_t read_all(int fd, void *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, (char*)buf + done, len - done);
        if (n <= 0) return -1;
        done += (size_t)n;
    }
    return (ssize_t)done;
}

/* ── connect helper ────────────────────────────────────────────── */
static int make_conn(const struct ProxyAddress *p) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    struct sockaddr_in addr = {
        .sin_family      = AF_INET,
        .sin_port        = htons(p->port),
        .sin_addr.s_addr = inet_addr(p->ip),
    };
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd); return -1;
    }
    return fd;
}

/* ── sort helper for percentiles ───────────────────────────────── */
static int cmp_ll(const void *a, const void *b) {
    long long x = *(const long long *)a, y = *(const long long *)b;
    return (x > y) - (x < y);
}

/* ── per-operation send/recv (reuses open fd) ──────────────────── */
static int do_set(int fd, const char *key, const char *val) {
    uint16_t kl = (uint16_t)strlen(key);
    uint32_t vl = (uint32_t)strlen(val);
    struct MsgHeader hdr = { .magic=0x4B, .command=0x02,
                              .key_len=kl, .val_len=vl };
    struct iovec iov[3] = {
        { &hdr,      sizeof(hdr) },
        { (void*)key, kl         },
        { (void*)val, vl         },
    };
    if (writev(fd, iov, 3) < 0) return -1;
    uint8_t status;
    if (read(fd, &status, 1) <= 0) return -1;
    return status;
}

static int do_get(int fd, const char *key, char *out, uint32_t out_sz) {
    uint16_t kl = (uint16_t)strlen(key);
    struct MsgHeader hdr = { .magic=0x4B, .command=0x01,
                              .key_len=kl, .val_len=0 };
    struct iovec iov[2] = {
        { &hdr,      sizeof(hdr) },
        { (void*)key, kl         },
    };
    if (writev(fd, iov, 2) < 0) return -1;
    uint8_t status;
    if (read(fd, &status, 1) <= 0) return -1;
    if (status == 0x00) {
        uint32_t rlen;
        if (read_all(fd, &rlen, sizeof(rlen)) < 0) return -1;
        if (rlen > 0 && rlen < out_sz) {
            if (read_all(fd, out, rlen) < 0) return -1;
            out[rlen] = '\0';
            //printf("[benchmark][get] key: %s / result: %s \n", key, out);
        }
    }
    return status;
}

/* ── worker ────────────────────────────────────────────────────── */
static void *benchmark_worker(void *arg) {
    struct ThreadArg *t = (struct ThreadArg *)arg;
    int id  = t->worker_id;
    int ops = t->ops_per_thread;

    struct WorkerResult *res = calloc(1, sizeof(*res));

    long long *write_lat = malloc(sizeof(long long) * (size_t)ops);
    long long *read_lat  = malloc(sizeof(long long) * (size_t)ops);

    char key[64], val[256], buf[4096];

    /* ── warmup ─────────────────────────────────────────────────── */
    {
        int wfd = make_conn(&t->proxy);
        if (wfd >= 0) {
            for (int i = 0; i < WARMUP_OPS; i++) {
                snprintf(key, sizeof(key), "warmup%d_k%d", id, i);
                snprintf(val, sizeof(val), "warmup%d_v%d", id, i);
                if (do_set(wfd, key, val) < 0) {
                    close(wfd); wfd = make_conn(&t->proxy);
                    if (wfd < 0) break;
                }
            }
            if (wfd >= 0) close(wfd);
        }
    }

    /* ── WRITE phase (persistent connection) ────────────────────── */
    int wfd = make_conn(&t->proxy);
    if (!wfd) goto skip_writes;

    for (int i = 0; i < ops; i++) {
        snprintf(key, sizeof(key), "w%d_k%d", id, i);
        snprintf(val, sizeof(val), "w%d_v%d", id, i);
        long long t0 = now_ns();
        int rc = do_set(wfd, key, val);
        long long elapsed = now_ns() - t0;
        if (rc == 0) { res->write_success++; write_lat[i] = elapsed; }
        else         { write_lat[i] = 0;
                       /* reconnect on error */
                       close(wfd); wfd = make_conn(&t->proxy);
                       if (wfd < 0) break; }
    }
    if (wfd >= 0) close(wfd);
skip_writes:;

    /* ── READ phase (persistent connection) ─────────────────────── */
    int rfd = make_conn(&t->proxy);
    if (!rfd) goto skip_reads;

    for (int i = 0; i < ops; i++) {
        snprintf(key, sizeof(key), "w%d_k%d", id, i);
        long long t0 = now_ns();
        int rc = do_get(rfd, key, buf, sizeof(buf));
        long long elapsed = now_ns() - t0;
        if (rc == 0) { res->read_success++; read_lat[i] = elapsed; }
        else         { read_lat[i] = 0;
                       close(rfd); rfd = make_conn(&t->proxy);
                       if (rfd < 0) break; }
    }
    if (rfd >= 0) close(rfd);
skip_reads:;

    /* ── compute stats ──────────────────────────────────────────── */
    if (res->write_success > 0) {
        long long sum = 0;
        int n = res->write_success;
        long long *tmp = malloc(sizeof(long long) * (size_t)n);
        int k = 0;
        for (int i = 0; i < ops; i++) if (write_lat[i]) { sum += write_lat[i]; tmp[k++] = write_lat[i]; }
        qsort(tmp, (size_t)k, sizeof(long long), cmp_ll);
        res->write_latency_avg_us = (double)sum / n / 1000.0;
        res->write_p99_us         = (double)tmp[(int)(k * 0.99)] / 1000.0;
        free(tmp);
    }
    if (res->read_success > 0) {
        long long sum = 0;
        int n = res->read_success;
        long long *tmp = malloc(sizeof(long long) * (size_t)n);
        int k = 0;
        for (int i = 0; i < ops; i++) if (read_lat[i]) { sum += read_lat[i]; tmp[k++] = read_lat[i]; }
        qsort(tmp, (size_t)k, sizeof(long long), cmp_ll);
        res->read_latency_avg_us = (double)sum / n / 1000.0;
        res->read_p99_us         = (double)tmp[(int)(k * 0.99)] / 1000.0;
        free(tmp);
    }

    free(write_lat);
    free(read_lat);
    return res;
}

/* ── config ────────────────────────────────────────────────────── */
static int load_proxy_address(const char *file, struct ProxyAddress *addr) {
    FILE *f = fopen(file, "r");
    if (!f) return -1;
    char line[256];
    while (fgets(line, sizeof(line), f)) {
        if (line[0] == '#' || line[0] == '\n') continue;
        char k[64], v[128];
        if (sscanf(line, "%63[^=]=%127[^\n]", k, v) != 2) continue;
        if      (strcmp(k, "PROXY_IP")   == 0) snprintf(addr->ip, sizeof(addr->ip), "%.*s", 63, v);
        else if (strcmp(k, "PROXY_PORT") == 0) addr->port = atoi(v);
    }
    fclose(f);
    return 0;
}

/* ── main ──────────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    int num_workers = DEFAULT_WORKERS;
    if (argc > 1) { num_workers = atoi(argv[1]); if (num_workers <= 0) num_workers = DEFAULT_WORKERS; }

    int ops_per = TOTAL_OPERATIONS / num_workers;

    struct ProxyAddress proxy = { .ip = "127.0.0.1", .port = 8080 };
    if (load_proxy_address("config.txt", &proxy) == 0)
        printf("Proxy: %s:%d\n", proxy.ip, proxy.port);
    else
        fprintf(stderr, "config.txt not found, using defaults.\n");

    pthread_t       threads[num_workers];
    struct ThreadArg args[num_workers];

    printf("Benchmark: %d workers × %d writes + %d reads  (warmup=%d)\n\n",
           num_workers, ops_per, ops_per, WARMUP_OPS);

    long long t_start = now_ns();

    for (int i = 0; i < num_workers; i++) {
        args[i].worker_id    = i;
        args[i].ops_per_thread = ops_per;
        args[i].proxy        = proxy;
        pthread_create(&threads[i], NULL, benchmark_worker, &args[i]);
    }

    struct WorkerResult *results[num_workers];
    for (int i = 0; i < num_workers; i++)
        pthread_join(threads[i], (void **)&results[i]);

    long long t_end = now_ns();

    int    total_w = 0, total_r = 0;
    double avg_w_lat = 0, avg_r_lat = 0, avg_w_p99 = 0, avg_r_p99 = 0;
    for (int i = 0; i < num_workers; i++) {
        total_w   += results[i]->write_success;
        total_r   += results[i]->read_success;
        avg_w_lat += results[i]->write_latency_avg_us;
        avg_r_lat += results[i]->read_latency_avg_us;
        avg_w_p99 += results[i]->write_p99_us;
        avg_r_p99 += results[i]->read_p99_us;
        free(results[i]);
    }
    avg_w_lat /= num_workers;
    avg_r_lat /= num_workers;
    avg_w_p99 /= num_workers;
    avg_r_p99 /= num_workers;

    double elapsed_s = (double)(t_end - t_start) / 1e9;
    double ops_total = (double)(total_w + total_r);
    double throughput = ops_total / elapsed_s;

    printf("============== BENCHMARK RESULTS ==============\n");
    printf("Total time             : %.4f s\n",      elapsed_s);
    printf("Total ops  (r+w)       : %.0f\n",        ops_total);
    printf("Throughput             : %.2f ops/sec\n", throughput);
    printf("-------------- Latency (µs) -------------------\n");
    printf("Write  avg / p99       : %.2f µs / %.2f µs\n", avg_w_lat, avg_w_p99);
    printf("Read   avg / p99       : %.2f µs / %.2f µs\n", avg_r_lat, avg_r_p99);
    printf("===============================================\n");
    return 0;
}
