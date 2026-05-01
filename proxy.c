#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <pthread.h>

#define BUFFER_SIZE 4096
#define MAX_STORAGES 3
#define RING_VIRTUAL_NODES 10 // Number of virtual nodes per physical storage

struct StorageConfig {
    int id;
    char ip[64];
    int port;
};

struct ProxyConfig config;

struct RingNode {
    uint32_t position;
    int storage_idx;
};

struct ProxyConfig {
    char bind_ip[64];
    int port;
    struct StorageConfig storages[MAX_STORAGES];
    int storage_count;
    struct RingNode ring[MAX_STORAGES * RING_VIRTUAL_NODES];
    int ring_size;
};

struct __attribute__((packed)) MsgHeader {
    uint8_t magic;
    uint8_t command;
    uint16_t key_len;
    uint32_t val_len;
};

// Compute hash value
uint32_t compute_hash(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return (uint32_t)hash;
}

// Compare nodes for sorting the hash ring
int compare_nodes(const void *a, const void *b) {
    struct RingNode *nodeA = (struct RingNode *)a;
    struct RingNode *nodeB = (struct RingNode *)b;
    if (nodeA->position < nodeB->position) return -1;
    if (nodeA->position > nodeB->position) return 1;
    return 0;
}

// Initialize consistent hash ring
void init_hash_ring(struct ProxyConfig *config) {
    config->ring_size = 0;
    for (int i = 0; i < config->storage_count; i++) {
        for (int v = 0; v < RING_VIRTUAL_NODES; v++) {
            char vnode_key[128];
            snprintf(vnode_key, sizeof(vnode_key), "node-%d-vnode-%d", config->storages[i].id, v);
            
            config->ring[config->ring_size].position = compute_hash(vnode_key);
            config->ring[config->ring_size].storage_idx = i;
            config->ring_size++;
        }
    }
    // Sort ring by position
    qsort(config->ring, config->ring_size, sizeof(struct RingNode), compare_nodes);
}

// Get the closest node on the consistent hash ring
int get_storage_index(const char *key, const struct ProxyConfig *config) {
    uint32_t key_pos = compute_hash(key);
    
    // Binary or linear search on the ring
    for (int i = 0; i < config->ring_size; i++) {
        if (config->ring[i].position >= key_pos) {
            return config->ring[i].storage_idx;
        }
    }
    
    // Wrap around the ring
    return config->ring[0].storage_idx;
}

int load_config(const char *filename, struct ProxyConfig *config) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Failed to open config.txt");
        return -1;
    }

    char line[256];
    config->storage_count = 0;

    while (fgets(line, sizeof(line), file)) {
        if (line[0] == '#' || line[0] == '\n' || line[0] == '\r') continue;

        char key[64], value[128];
        if (sscanf(line, "%63[^=]=%127[^\n]", key, value) == 2) {
            if (strcmp(key, "PROXY_IP") == 0) {
                strncpy(config->bind_ip, value, sizeof(config->bind_ip) - 1);
            } else if (strcmp(key, "PROXY_PORT") == 0) {
                config->port = atoi(value);
            } else if (strncmp(key, "STORAGE_", 8) == 0) {
                int id = atoi(key + 8);
                char ip[64];
                int port;

                if (sscanf(value, "%63[^:]:%d", ip, &port) == 2) {
                    if (config->storage_count < MAX_STORAGES) {
                        config->storages[config->storage_count].id = id;
                        strncpy(config->storages[config->storage_count].ip, ip, sizeof(config->storages[config->storage_count].ip) - 1);
                        config->storages[config->storage_count].port = port;
                        config->storage_count++;
                    }
                }
            }
        }
    }
    fclose(file);
    init_hash_ring(config);
    return 0;
}

int forward_to_storage(const struct StorageConfig *storage, uint8_t cmd, const char *key, const char *val, char *out_buf, size_t *out_len) {
    int sock_fd;
    struct sockaddr_in addr;

    if ((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        return -1;
    }

    addr.sin_family = AF_INET;
    addr.sin_port = htons(storage->port);
    addr.sin_addr.s_addr = inet_addr(storage->ip);

    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        close(sock_fd);
        return -1;
    }

    uint16_t key_len = key ? strlen(key) : 0;
    uint32_t val_len = val ? strlen(val) : 0;

    struct MsgHeader header = {
        .magic = 0x4B,
        .command = cmd,
        .key_len = key_len,
        .val_len = val_len
    };

    write(sock_fd, &header, sizeof(header));
    if (key_len > 0) write(sock_fd, key, key_len);
    if (cmd == 0x02 && val_len > 0) write(sock_fd, val, val_len);

    uint8_t status;
    if (read(sock_fd, &status, 1) <= 0) {
        close(sock_fd);
        return -1;
    }

    if (cmd == 0x01 && status == 0x00) {
        uint32_t read_val_len = 0;
        if (read(sock_fd, &read_val_len, sizeof(read_val_len)) == sizeof(read_val_len)) {
            *out_len = read_val_len;
            if (read_val_len > 0 && read_val_len < BUFFER_SIZE) {
                ssize_t n = read(sock_fd, out_buf, read_val_len);
                if (n > 0) out_buf[n] = '\0';
                //printf("[proxy][get request] key: %s / get output: %s \n", key, out_buf);
            }
        }
    }

    close(sock_fd);
    return status;
}
void *handle_client(void *arg){
    int client_fd = *(int *)arg;
    free(arg);

    struct MsgHeader header;
    if (read(client_fd, &header, sizeof(header)) == sizeof(header)) {
        char key[256] = {0};
        char val[4096] = {0};

        if (header.key_len > 0 && header.key_len < sizeof(key)) {
            read(client_fd, key, header.key_len);
        }
        if (header.command == 0x02 && header.val_len > 0 && header.val_len < sizeof(val)) {
            read(client_fd, val, header.val_len);
        }

        int storage_idx = get_storage_index(key, &config);
        char resp_buf[4096] = {0};
        uint32_t resp_len = 0;

        int status = forward_to_storage(&config.storages[storage_idx], header.command, key, val, resp_buf, &resp_len);

        write(client_fd, &status, 1);
        if (header.command == 0x01 && status == 0x00) {
            write(client_fd, &resp_len, sizeof(resp_len));
            write(client_fd, resp_buf, resp_len);
        }
    }

    close(client_fd);
    return;
}
int main() {
    if (load_config("config.txt", &config) != 0) {
        fprintf(stderr, "Failed to load config.txt\n");
        exit(EXIT_FAILURE);
    }

    int server_fd;
    struct sockaddr_in addr;

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("Proxy: Socket failed");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(config.bind_ip);
    addr.sin_port = htons(config.port);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("Proxy: Bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) == -1) {
        perror("Proxy: Listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    printf("Proxy listening on port %d with consistent hashing...\n", config.port);

    while (1) {
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) continue;

        int *pclient = malloc(sizeof(int));
        *pclient = client_fd;

        pthread_t tid;
        pthread_create(&tid, NULL, handle_client, pclient);
        pthread_detach(tid);
    }

    close(server_fd);
    return 0;
}