/*
 * proxy.c  –  Optimized consistent-hashing proxy
 *
 * Key improvements over original:
 *  - Fixed thread pool (no per-connection pthread_create)
 *  - Per-storage TCP connection pool (reuse connections, avoid 3-way handshakes)
 *  - Binary search on consistent hash ring  (O(log n) vs O(n))
 *  - writev() to send header+key+val in one syscall
 *  - TCP_NODELAY on both client and storage sockets
 *  - SO_REUSEPORT on accept socket
 *  - Proper error handling on all read_all paths in handle_client
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <pthread.h>
#include <semaphore.h>

/* ── tunables ──────────────────────────────────────────────────── */
#define BUFFER_SIZE       4096
#define MAX_STORAGES      16
#define RING_VIRTUAL_NODES 150   /* more vnodes → better balance    */
#define THREAD_POOL_SIZE   32
#define QUEUE_CAPACITY    8192
#define CONN_POOL_SIZE     16    /* persistent conns per storage    */
/* ──────────────────────────────────────────────────────────────── */

/* ── wire protocol ─────────────────────────────────────────────── */
struct __attribute__((packed)) MsgHeader {
    uint8_t  magic;
    uint8_t  command;
    uint16_t key_len;
    uint32_t val_len;
};

/* ── storage config ────────────────────────────────────────────── */
struct StorageConfig {
    int  id;
    char ip[64];
    int  port;
};

/* ── connection pool ───────────────────────────────────────────── */
struct ConnPool {
    int             fds[CONN_POOL_SIZE];
    pthread_mutex_t lock;
    int             count;          /* number of available (idle) fds */
    char            ip[64];
    int             port;
};

/* ── consistent hash ring ──────────────────────────────────────── */
struct RingNode {
    uint32_t position;
    int      storage_idx;
};

/* ── proxy config ──────────────────────────────────────────────── */
struct ProxyConfig {
    char              bind_ip[64];
    int               port;
    struct StorageConfig storages[MAX_STORAGES];
    int               storage_count;
    struct RingNode   ring[MAX_STORAGES * RING_VIRTUAL_NODES];
    int               ring_size;
    struct ConnPool   pools[MAX_STORAGES];
};

static struct ProxyConfig g_config;

/* ── helpers ───────────────────────────────────────────────────── */
static uint32_t compute_hash(const char *s) {
    uint32_t h = 5381;
    unsigned char c;
    while ((c = (unsigned char)*s++)) h = ((h << 5) + h) ^ c;
    return h;
}

static ssize_t read_all(int fd, void *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, (char*)buf + done, len - done);
        if (n <= 0) return -1;
        done += (size_t)n;
    }
    return (ssize_t)done;
}

static ssize_t write_all(int fd, const void *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t n = write(fd, (const char*)buf + done, len - done);
        if (n <= 0) return -1;
        done += (size_t)n;
    }
    return (ssize_t)done;
}

/* ── consistent hash ring ──────────────────────────────────────── */
static int cmp_ring(const void *a, const void *b) {
    const struct RingNode *x = a, *y = b;
    return (x->position > y->position) - (x->position < y->position);
}

static void init_hash_ring(struct ProxyConfig *cfg) {
    cfg->ring_size = 0;
    for (int i = 0; i < cfg->storage_count; i++) {
        for (int v = 0; v < RING_VIRTUAL_NODES; v++) {
            char key[128];
            snprintf(key, sizeof(key), "node-%d-vnode-%d", cfg->storages[i].id, v);
            cfg->ring[cfg->ring_size].position    = compute_hash(key);
            cfg->ring[cfg->ring_size].storage_idx = i;
            cfg->ring_size++;
        }
    }
    qsort(cfg->ring, cfg->ring_size, sizeof(struct RingNode), cmp_ring);
}

/* Binary search – O(log n) */
static int get_storage_index(const char *key, const struct ProxyConfig *cfg) {
    uint32_t pos = compute_hash(key);
    int lo = 0, hi = cfg->ring_size - 1;
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        if (cfg->ring[mid].position < pos) lo = mid + 1;
        else                                hi = mid;
    }
    /* If pos > all positions, wrap around to first node */
    if (cfg->ring[lo].position < pos) lo = 0;
    return cfg->ring[lo].storage_idx;
}

/* ── connection pool ───────────────────────────────────────────── */
static void pool_init(struct ConnPool *p, const char *ip, int port) {
    pthread_mutex_init(&p->lock, NULL);
    p->count = 0;
    strncpy(p->ip, ip, sizeof(p->ip) - 1);
    p->port = port;
    for (int i = 0; i < CONN_POOL_SIZE; i++) p->fds[i] = -1;
}

static int new_storage_conn(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    struct sockaddr_in addr = {
        .sin_family      = AF_INET,
        .sin_port        = htons(port),
        .sin_addr.s_addr = inet_addr(ip),
    };
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd); return -1;
    }
    return fd;
}

/* Borrow a connection from the pool (or open a fresh one). */
static int pool_acquire(struct ConnPool *p) {
    pthread_mutex_lock(&p->lock);
    if (p->count > 0) {
        int fd = p->fds[--p->count];
        pthread_mutex_unlock(&p->lock);
        return fd;
    }
    pthread_mutex_unlock(&p->lock);
    return new_storage_conn(p->ip, p->port);
}

/* Return a healthy connection to the pool, or close it if pool is full. */
static void pool_release(struct ConnPool *p, int fd) {
    pthread_mutex_lock(&p->lock);
    if (p->count < CONN_POOL_SIZE) {
        p->fds[p->count++] = fd;
        pthread_mutex_unlock(&p->lock);
    } else {
        pthread_mutex_unlock(&p->lock);
        close(fd);
    }
}

/* ── forward request to storage ────────────────────────────────── */
static int forward_to_storage(int storage_idx, uint8_t cmd,
                               const char *key, const char *val,
                               char *out_buf, uint32_t *out_len) {
    struct ConnPool *pool = &g_config.pools[storage_idx];
    int fd = pool_acquire(pool);
    if (fd < 0) return -1;

    uint16_t key_len = key ? (uint16_t)strlen(key) : 0;
    uint32_t val_len = val ? (uint32_t)strlen(val) : 0;

    struct MsgHeader hdr = {
        .magic   = 0x4B,
        .command = cmd,
        .key_len = key_len,
        .val_len = val_len,
    };

    /* Send header + key + val in one writev call */
    struct iovec iov[3];
    int niov = 0;
    iov[niov++] = (struct iovec){ &hdr,        sizeof(hdr) };
    if (key_len) iov[niov++] = (struct iovec){ (void*)key, key_len  };
    if (cmd == 0x02 && val_len)
                 iov[niov++] = (struct iovec){ (void*)val, val_len  };
    if (writev(fd, iov, niov) < 0) { close(fd); return -1; }

    uint8_t status;
    if (read_all(fd, &status, 1) < 0) { close(fd); return -1; }

    if (cmd == 0x01 && status == 0x00) {
        uint32_t rlen = 0;
        if (read_all(fd, &rlen, sizeof(rlen)) < 0) { close(fd); return -1; }
        *out_len = rlen;
        if (rlen > 0 && rlen < BUFFER_SIZE) {
            if (read_all(fd, out_buf, rlen) < 0) { close(fd); return -1; }
            out_buf[rlen] = '\0';
        }
    }

    pool_release(pool, fd);   /* return healthy connection */
    return (int)status;
}

/* ── thread pool ───────────────────────────────────────────────── */
static int      q_buf[QUEUE_CAPACITY];
static int      q_head = 0, q_tail = 0;
static pthread_mutex_t q_lock = PTHREAD_MUTEX_INITIALIZER;
static sem_t           q_sem;

static void q_push(int fd) {
    pthread_mutex_lock(&q_lock);
    q_buf[q_tail] = fd;
    q_tail = (q_tail + 1) % QUEUE_CAPACITY;
    pthread_mutex_unlock(&q_lock);
    sem_post(&q_sem);
}

static int q_pop(void) {
    sem_wait(&q_sem);
    pthread_mutex_lock(&q_lock);
    int fd = q_buf[q_head];
    q_head = (q_head + 1) % QUEUE_CAPACITY;
    pthread_mutex_unlock(&q_lock);
    return fd;
}

/* ── client handler ────────────────────────────────────────────── */
static void handle_client(int client_fd) {
    int one = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    struct MsgHeader hdr;
    if (read_all(client_fd, &hdr, sizeof(hdr)) < 0) goto done;
    if (hdr.magic != 0x4B) goto done;
    if (hdr.key_len == 0 || hdr.key_len >= 256) goto done;

    char key[256]         = {0};
    char val[BUFFER_SIZE] = {0};

    if (read_all(client_fd, key, hdr.key_len) < 0) goto done;
    key[hdr.key_len] = '\0';

    if (hdr.command == 0x02) {
        if (hdr.val_len == 0 || hdr.val_len >= BUFFER_SIZE) goto done;
        if (read_all(client_fd, val, hdr.val_len) < 0)       goto done;
        val[hdr.val_len] = '\0';
    }

    int storage_idx      = get_storage_index(key, &g_config);
    char resp_buf[BUFFER_SIZE] = {0};
    uint32_t resp_len    = 0;

    int status = forward_to_storage(storage_idx, hdr.command,
                                    key, val, resp_buf, &resp_len);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
    if (status < 0) { uint8_t e = 0x01; (void)write(client_fd, &e, 1); goto done; }
#pragma GCC diagnostic pop

    if (hdr.command == 0x01 && status == 0x00) {
        struct iovec iov[3] = {
            { &status,   1               },
            { &resp_len, sizeof(uint32_t)},
            { resp_buf,  resp_len        },
        };
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
        (void)writev(client_fd, iov, 3);
#pragma GCC diagnostic pop
    } else {
        write_all(client_fd, &status, 1);
    }

done:
    close(client_fd);
}

static void *worker_thread(void *arg) {
    (void)arg;
    for (;;) handle_client(q_pop());
    return NULL;
}

/* ── config loader ─────────────────────────────────────────────── */
static int load_config(const char *filename, struct ProxyConfig *cfg) {
    FILE *f = fopen(filename, "r");
    if (!f) { perror("fopen"); return -1; }

    char line[256];
    cfg->storage_count = 0;

    while (fgets(line, sizeof(line), f)) {
        if (line[0] == '#' || line[0] == '\n' || line[0] == '\r') continue;
        char k[64], v[128];
        if (sscanf(line, "%63[^=]=%127[^\n]", k, v) != 2) continue;

        if      (strcmp(k, "PROXY_IP")   == 0) snprintf(cfg->bind_ip, sizeof(cfg->bind_ip), "%.*s", 63, v);
        else if (strcmp(k, "PROXY_PORT") == 0) cfg->port = atoi(v);
        else if (strncmp(k, "STORAGE_", 8) == 0) {
            int id = atoi(k + 8);
            char ip[64]; int port;
            if (sscanf(v, "%63[^:]:%d", ip, &port) == 2 &&
                cfg->storage_count < MAX_STORAGES) {
                int i = cfg->storage_count++;
                cfg->storages[i].id   = id;
                snprintf(cfg->storages[i].ip, sizeof(cfg->storages[i].ip), "%.*s", 63, ip);
                cfg->storages[i].port = port;
            }
        }
    }
    fclose(f);
    init_hash_ring(cfg);
    return 0;
}

/* ── main ──────────────────────────────────────────────────────── */
int main(void) {
    if (load_config("config.txt", &g_config) != 0) {
        fprintf(stderr, "Failed to load config.txt\n");
        return EXIT_FAILURE;
    }

    /* Initialise per-storage connection pools */
    for (int i = 0; i < g_config.storage_count; i++)
        pool_init(&g_config.pools[i],
                  g_config.storages[i].ip,
                  g_config.storages[i].port);

    sem_init(&q_sem, 0, 0);

    pthread_t workers[THREAD_POOL_SIZE];
    for (int i = 0; i < THREAD_POOL_SIZE; i++)
        pthread_create(&workers[i], NULL, worker_thread, NULL);

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { perror("socket"); return EXIT_FAILURE; }

    int opt = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(srv, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family      = AF_INET,
        .sin_port        = htons(g_config.port),
        .sin_addr.s_addr = inet_addr(g_config.bind_ip),
    };
    if (bind(srv, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return EXIT_FAILURE;
    }
    if (listen(srv, 4096) < 0) {
        perror("listen"); return EXIT_FAILURE;
    }

    printf("Proxy on %s:%d  storages=%d  pool=%d  vnodes=%d  workers=%d\n",
           g_config.bind_ip, g_config.port, g_config.storage_count,
           CONN_POOL_SIZE, RING_VIRTUAL_NODES, THREAD_POOL_SIZE);

    for (;;) {
        int cfd = accept(srv, NULL, NULL);
        if (cfd < 0) continue;
        q_push(cfd);
    }
}
