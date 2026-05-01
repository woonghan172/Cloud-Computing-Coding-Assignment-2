#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdint.h>

#define NUM_WORKERS 4
#define NUM_WRITES 500
#define NUM_READS 500
#define TOTAL_OPERATION_NUM 1000
int operation_num_per_thread;

struct __attribute__((packed)) MsgHeader {
    uint8_t magic;
    uint8_t command;
    uint16_t key_len;
    uint32_t val_len;
};

struct WorkerResult {
    double write_latency_ms;
    double read_latency_ms;
    int write_success;
    int read_success;
};

struct ProxyAddress {
    char ip[64];
    int port;
};

struct ThreadArg {
    int worker_id;
    struct ProxyAddress proxy;
};

int load_proxy_address(const char *filename, struct ProxyAddress *addr) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        return -1;
    }

    char line[256];
    while (fgets(line, sizeof(line), file)) {
        if (line[0] == '#' || line[0] == '\n' || line[0] == '\r') continue;

        char key[64], value[128];
        if (sscanf(line, "%63[^=]=%127[^\n]", key, value) == 2) {
            if (strcmp(key, "PROXY_IP") == 0) {
                strncpy(addr->ip, value, sizeof(addr->ip) - 1);
            } else if (strcmp(key, "PROXY_PORT") == 0) {
                addr->port = atoi(value);
            }
        }
    }
    fclose(file);
    return 0;
}

long long get_time_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (((long long)tv.tv_sec) * 1000) + (tv.tv_usec / 1000);
}
ssize_t read_all(int socket_fd, void *buffer, size_t length) {
    size_t total_read = 0;
    while (total_read < length) {
        ssize_t n = read(socket_fd, (char *)buffer + total_read, length - total_read);
        if (n <= 0) return -1;
        total_read += n;
    }
    return total_read;
}

void *benchmark_worker(void *arg, int operation_num) {
    struct ThreadArg *t_arg = (struct ThreadArg *)arg;
    int worker_id = t_arg->worker_id;
    struct ProxyAddress proxy = t_arg->proxy;
    
    struct WorkerResult *res = calloc(1, sizeof(struct WorkerResult));

    char key[64];
    char val[256];

    // --- WRITE PHASE ---
    long long total_write_time = 0;
    for (int i = 0; i < operation_num_per_thread; i++) {
        int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (sock_fd < 0) continue;

        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(proxy.port);
        addr.sin_addr.s_addr = inet_addr(proxy.ip);

        if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            close(sock_fd);
            continue;
        }
        snprintf(key, sizeof(key), "worker%d_key%d", worker_id, i);
        snprintf(val, sizeof(val), "worker%d_val%d", worker_id, i);
        //printf("[put request] key: %s / val: %s \n", key, val);
        
        struct MsgHeader header = {
            .magic = 0x4B,
            .command = 0x02, // SET
            .key_len = strlen(key),
            .val_len = strlen(val)
        };
        
        long long start = get_time_ms();
        
        write(sock_fd, &header, sizeof(header));
        write(sock_fd, key, header.key_len);
        write(sock_fd, val, header.val_len);
        
        uint8_t status;
        if (read(sock_fd, &status, 1) > 0 && status == 0x00) {
            res->write_success++;
        }
        
        total_write_time += (get_time_ms() - start);
        close(sock_fd);
    }
    res->write_latency_ms = (res->write_success > 0) ? ((double)total_write_time / res->write_success) : 0.0;
    
    // --- READ PHASE ---
    long long total_read_time = 0;
    for (int i = 0; i < operation_num_per_thread; i++) {
        int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (sock_fd < 0) continue;
        
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(proxy.port);
        addr.sin_addr.s_addr = inet_addr(proxy.ip);
        
        if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            close(sock_fd);
            continue;
        }
        
        snprintf(key, sizeof(key), "worker%d_key%d", worker_id, i);
        
        struct MsgHeader header = {
            .magic = 0x4B,
            .command = 0x01, // GET
            .key_len = strlen(key),
            .val_len = 0
        };

        long long start = get_time_ms();

        write(sock_fd, &header, sizeof(header));
        write(sock_fd, key, header.key_len);

        uint8_t status;
        if (read(sock_fd, &status, 1) > 0 && status == 0x00) {
            uint32_t val_len;
            if (read_all(sock_fd, &val_len, sizeof(val_len)) > 0) {
                char buffer[4096] = {0};
                if (val_len < sizeof(buffer)) {
                    if (read_all(sock_fd, buffer, val_len) > 0) {
                        res->read_success++;
                        //printf("[get request] key: %s / result: %s\n", key, buffer);
                    }
                }
            }
        }
        total_read_time += (get_time_ms() - start);
        close(sock_fd);
    }
    res->read_latency_ms = (res->read_success > 0) ? ((double)total_read_time / res->read_success) : 0.0;

    return (void *)res;
}
int main(int argc, char *argv[]) {
    int num_workers = NUM_WORKERS;

    if (argc > 1) {
        num_workers = atoi(argv[1]);
        if (num_workers <= 0) {
            num_workers = NUM_WORKERS;
        }
    }

    operation_num_per_thread = TOTAL_OPERATION_NUM / num_workers;

    struct ProxyAddress proxy;
    if (load_proxy_address("config.txt", &proxy) != 0) {
        fprintf(stderr, "Failed to parse config.txt. Using defaults (127.0.0.1:8080).\n");
        strcpy(proxy.ip, "127.0.0.1");
        proxy.port = 8080;
    } else {
        printf("Loaded Proxy Address from config.txt -> %s:%d\n", proxy.ip, proxy.port);
    }

    pthread_t threads[num_workers];
    struct ThreadArg args[num_workers];

    printf("Starting benchmark with %d workers, %d writes and %d reads per worker...\n\n",
           num_workers, operation_num_per_thread, operation_num_per_thread);

    long long total_start_time = get_time_ms();

    for (int i = 0; i < num_workers; i++) {
        args[i].worker_id = i;
        args[i].proxy = proxy; // Pass pre-parsed address
        pthread_create(&threads[i], NULL, benchmark_worker, &args[i]);
    }

    struct WorkerResult *results[num_workers];
    for (int i = 0; i < num_workers; i++) {
        pthread_join(threads[i], (void **)&results[i]);
    }

    long long total_elapsed = get_time_ms() - total_start_time;

    int total_writes = 0, total_reads = 0;
    double avg_write_lat = 0, avg_read_lat = 0;

    for (int i = 0; i < num_workers; i++) {
        total_writes += results[i]->write_success;
        total_reads += results[i]->read_success;
        avg_write_lat += results[i]->write_latency_ms;
        avg_read_lat += results[i]->read_latency_ms;
        free(results[i]);
    }

    avg_write_lat /= num_workers;
    avg_read_lat /= num_workers;

    double total_time_sec = (double)total_elapsed / 1000.0;
    double total_ops = (double)(total_writes + total_reads);
    double throughput = total_ops / total_time_sec;

    printf("================ BENCHMARK RESULTS ================\n");
    printf("Total execution time   : %.4f s\n", total_time_sec);
    printf("Total operations (r+w) : %.0f\n", total_ops);
    printf("Throughput             : %.2f requests/sec\n", throughput);
    printf("----------------- Latency Breakdown ----------------\n");
    printf("Avg write latency      : %.2f ms\n", avg_write_lat);
    printf("Avg read latency       : %.2f ms\n", avg_read_lat);
    printf("====================================================\n");

    return 0;
}