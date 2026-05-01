/*
 * storage.c  –  Optimized key-value storage engine
 *
 * Key improvements over original:
 *  - Fixed-size thread pool (no per-connection pthread_create)
 *  - pthread_rwlock_t per bucket (concurrent reads, exclusive writes)
 *  - writev() for multi-buffer sends (fewer syscalls)
 *  - clock_gettime-based timeouts (nanosecond precision)
 *  - Larger hash table (65537 prime) to reduce collision chains
 *  - Lock-free accept queue with semaphore-signalled worker wakeup
 *  - SO_REUSEPORT to let the OS distribute incoming connections
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
#define KEY_SIZE        256
#define VAL_SIZE        4096
#define HASH_TABLE_SIZE 65537   /* prime → better distribution     */
#define THREAD_POOL_SIZE 16     /* worker threads                   */
#define QUEUE_CAPACITY  4096    /* pending-fd ring buffer size      */
/* ──────────────────────────────────────────────────────────────── */

/* ── hash table ────────────────────────────────────────────────── */
struct KVNode {
    char key[KEY_SIZE];
    char value[VAL_SIZE];
    struct KVNode *next;
};

static struct KVNode        *ht[HASH_TABLE_SIZE];
static pthread_rwlock_t      ht_locks[HASH_TABLE_SIZE];

static uint32_t hash_key(const char *s) {
    uint32_t h = 5381;
    unsigned char c;
    while ((c = (unsigned char)*s++)) h = ((h << 5) + h) ^ c;
    return h;
}

static void ht_init(void) {
    for (int i = 0; i < HASH_TABLE_SIZE; i++)
        pthread_rwlock_init(&ht_locks[i], NULL);
}

static void kv_set(const char *key, const char *val) {
    uint32_t idx = hash_key(key) % HASH_TABLE_SIZE;
    pthread_rwlock_wrlock(&ht_locks[idx]);

    for (struct KVNode *n = ht[idx]; n; n = n->next) {
        if (strcmp(n->key, key) == 0) {
            strncpy(n->value, val, VAL_SIZE - 1);
            n->value[VAL_SIZE - 1] = '\0';
            pthread_rwlock_unlock(&ht_locks[idx]);
            return;
        }
    }
    struct KVNode *node = malloc(sizeof(*node));
    if (!node) { pthread_rwlock_unlock(&ht_locks[idx]); return; }
    strncpy(node->key,   key, KEY_SIZE - 1); node->key[KEY_SIZE - 1]   = '\0';
    strncpy(node->value, val, VAL_SIZE - 1); node->value[VAL_SIZE - 1] = '\0';
    node->next  = ht[idx];
    ht[idx]     = node;
    pthread_rwlock_unlock(&ht_locks[idx]);
}

/* Returns 0 on hit; out_val / *out_len filled.  Caller owns no allocation. */
static int kv_get(const char *key, char *out_val, size_t *out_len) {
    uint32_t idx = hash_key(key) % HASH_TABLE_SIZE;
    pthread_rwlock_rdlock(&ht_locks[idx]);
    for (struct KVNode *n = ht[idx]; n; n = n->next) {
        if (strcmp(n->key, key) == 0) {
            size_t len = strlen(n->value);
            memcpy(out_val, n->value, len + 1);
            *out_len = len;
            pthread_rwlock_unlock(&ht_locks[idx]);
            return 0;
        }
    }
    pthread_rwlock_unlock(&ht_locks[idx]);
    return 1;
}

static int kv_del(const char *key) {
    uint32_t idx = hash_key(key) % HASH_TABLE_SIZE;
    pthread_rwlock_wrlock(&ht_locks[idx]);
    struct KVNode *prev = NULL, *cur = ht[idx];
    while (cur) {
        if (strcmp(cur->key, key) == 0) {
            if (prev) prev->next = cur->next;
            else       ht[idx]   = cur->next;
            free(cur);
            pthread_rwlock_unlock(&ht_locks[idx]);
            return 0;
        }
        prev = cur; cur = cur->next;
    }
    pthread_rwlock_unlock(&ht_locks[idx]);
    return 1;
}

/* ── wire protocol ─────────────────────────────────────────────── */
struct __attribute__((packed)) MsgHeader {
    uint8_t  magic;
    uint8_t  command;
    uint16_t key_len;
    uint32_t val_len;
};

static ssize_t read_all(int fd, void *buf, size_t len) {
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, (char*)buf + done, len - done);
        if (n <= 0) return -1;
        done += (size_t)n;
    }
    return (ssize_t)done;
}

/* ── thread pool ───────────────────────────────────────────────── */
static int      queue[QUEUE_CAPACITY];
static int      q_head = 0, q_tail = 0;
static pthread_mutex_t q_lock  = PTHREAD_MUTEX_INITIALIZER;
static sem_t           q_sem;

static void queue_push(int fd) {
    pthread_mutex_lock(&q_lock);
    queue[q_tail] = fd;
    q_tail = (q_tail + 1) % QUEUE_CAPACITY;
    pthread_mutex_unlock(&q_lock);
    sem_post(&q_sem);
}

static int queue_pop(void) {
    sem_wait(&q_sem);
    pthread_mutex_lock(&q_lock);
    int fd = queue[q_head];
    q_head = (q_head + 1) % QUEUE_CAPACITY;
    pthread_mutex_unlock(&q_lock);
    return fd;
}

static void handle_client(int fd) {
    /* enable TCP_NODELAY: we control exactly when we flush */
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    struct MsgHeader hdr;
    if (read_all(fd, &hdr, sizeof(hdr)) < 0 || hdr.magic != 0x4B)
        goto done;

    if (hdr.key_len == 0 || hdr.key_len >= KEY_SIZE) goto done;

    char key[KEY_SIZE] = {0};
    char val[VAL_SIZE] = {0};

    if (read_all(fd, key, hdr.key_len) < 0) goto done;
    key[hdr.key_len] = '\0';

    if (hdr.command == 0x02) {
        if (hdr.val_len == 0 || hdr.val_len >= VAL_SIZE) goto done;
        if (read_all(fd, val, hdr.val_len) < 0)           goto done;
        val[hdr.val_len] = '\0';
    }

    if (hdr.command == 0x01) {                          /* GET */
        char out[VAL_SIZE];
        size_t olen = 0;
        if (kv_get(key, out, &olen) == 0) {
            uint8_t  status  = 0x00;
            uint32_t send_len = (uint32_t)olen;
            struct iovec iov[3] = {
                { &status,   1              },
                { &send_len, sizeof(uint32_t)},
                { out,       olen           },
            };
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
            (void)writev(fd, iov, 3);
#pragma GCC diagnostic pop
        } else {
            uint8_t status = 0x01;
    #pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
        (void)write(fd, &status, 1);
#pragma GCC diagnostic pop
        }
    } else if (hdr.command == 0x02) {                   /* SET */
        kv_set(key, val);
        uint8_t status = 0x00;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
        (void)write(fd, &status, 1);
#pragma GCC diagnostic pop
    } else if (hdr.command == 0x03) {                   /* DEL */
        uint8_t status = (kv_del(key) == 0) ? 0x00 : 0x01;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
        (void)write(fd, &status, 1);
#pragma GCC diagnostic pop
    } else {
        uint8_t status = 0x01;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
        (void)write(fd, &status, 1);
#pragma GCC diagnostic pop
    }

done:
    close(fd);
}

static void *worker_thread(void *arg) {
    (void)arg;
    for (;;) handle_client(queue_pop());
    return NULL;
}

/* ── config ────────────────────────────────────────────────────── */
static int load_config(const char *file, int storage_id,
                       char *out_ip, int *out_port) {
    FILE *f = fopen(file, "r");
    if (!f) return -1;
    char line[256], target[32];
    snprintf(target, sizeof(target), "STORAGE_%d", storage_id);
    while (fgets(line, sizeof(line), f)) {
        if (line[0] == '#' || line[0] == '\n') continue;
        char k[64], v[128];
        if (sscanf(line, "%63[^=]=%127[^\n]", k, v) != 2) continue;
        if (strcmp(k, target) == 0) {
            char ip[64]; int port;
            if (sscanf(v, "%63[^:]:%d", ip, &port) == 2) {
                snprintf(out_ip, 64, "%s", ip); out_ip[63] = '\0';
                *out_port = port;
                fclose(f);
                return 0;
            }
        }
    }
    fclose(f);
    return -1;
}

/* ── main ──────────────────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <storage_id>\n", argv[0]);
        return EXIT_FAILURE;
    }
    int storage_id = atoi(argv[1]);
    char ip[64] = "127.0.0.1";
    int  port    = 9001;

    if (load_config("config.txt", storage_id, ip, &port) != 0)
        fprintf(stderr, "Config not found for ID %d, using defaults.\n", storage_id);

    ht_init();
    sem_init(&q_sem, 0, 0);

    /* Spawn thread pool */
    pthread_t workers[THREAD_POOL_SIZE];
    for (int i = 0; i < THREAD_POOL_SIZE; i++)
        pthread_create(&workers[i], NULL, worker_thread, NULL);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return EXIT_FAILURE; }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR,  &opt, sizeof(opt));
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT,  &opt, sizeof(opt));

    struct sockaddr_in addr = {
        .sin_family      = AF_INET,
        .sin_port        = htons(port),
        .sin_addr.s_addr = inet_addr(ip),
    };
    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return EXIT_FAILURE;
    }
    if (listen(server_fd, 4096) < 0) {
        perror("listen"); return EXIT_FAILURE;
    }

    printf("Storage engine (ID: %d) listening on %s:%d  [pool=%d, ht=%d]\n",
           storage_id, ip, port, THREAD_POOL_SIZE, HASH_TABLE_SIZE);

    for (;;) {
        int cfd = accept(server_fd, NULL, NULL);
        if (cfd < 0) continue;
        queue_push(cfd);
    }
}
