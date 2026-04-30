#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <errno.h>

#define BUFFER_SIZE 4096
#define MAX_STORAGES 3
#define RING_VIRTUAL_NODES 10
#define MAX_EVENTS 100

struct StorageConfig {
    int id;
    char ip[64];
    int port;
};

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

// Set socket to non-blocking mode
int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

uint32_t compute_hash(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return (uint32_t)hash;
}

int compare_nodes(const void *a, const void *b) {
    struct RingNode *nodeA = (struct RingNode *)a;
    struct RingNode *nodeB = (struct RingNode *)b;
    if (nodeA->position < nodeB->position) return -1;
    if (nodeA->position > nodeB->position) return 1;
    return 0;
}

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
    qsort(config->ring, config->ring_size, sizeof(struct RingNode), compare_nodes);
}

int get_storage_index(const char *key, const struct ProxyConfig *config) {
    uint32_t key_pos = compute_hash(key);
    for (int i = 0; i < config->ring_size; i++) {
        if (config->ring[i].position >= key_pos) {
            return config->ring[i].storage_idx;
        }
    }
    return config->ring[0].storage_idx;
}

int load_config(const char *filename, struct ProxyConfig *config) {
    FILE *file = fopen(filename, "r");
    if (!file) return -1;

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

// Handler for the event loop connection
void handle_client(int client_fd, struct ProxyConfig *config) {
    struct MsgHeader header;
    ssize_t n = read(client_fd, &header, sizeof(header));
    if (n != sizeof(header)) return;

    char key[256] = {0};
    char val[4096] = {0};

    if (header.key_len > 0 && header.key_len < sizeof(key)) {
        read(client_fd, key, header.key_len);
    }
    if (header.command == 0x02 && header.val_len > 0 && header.val_len < sizeof(val)) {
        read(client_fd, val, header.val_len);
    }

    int storage_idx = get_storage_index(key, config);
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) return;

    struct sockaddr_in s_addr;
    s_addr.sin_family = AF_INET;
    s_addr.sin_port = htons(config->storages[storage_idx].port);
    s_addr.sin_addr.s_addr = inet_addr(config->storages[storage_idx].ip);

    if (connect(sock_fd, (struct sockaddr *)&s_addr, sizeof(s_addr)) < 0) {
        close(sock_fd);
        return;
    }

    write(sock_fd, &header, sizeof(header));
    if (header.key_len > 0) write(sock_fd, key, header.key_len);
    if (header.command == 0x02 && header.val_len > 0) write(sock_fd, val, header.val_len);

    uint8_t status;
    if (read(sock_fd, &status, 1) <= 0) {
        close(sock_fd);
        return;
    }

    write(client_fd, &status, 1);

    if (header.command == 0x01 && status == 0x00) {
        uint32_t read_val_len = 0;
        if (read(sock_fd, &read_val_len, sizeof(read_val_len)) == sizeof(read_val_len)) {
            char resp_buf[4096] = {0};
            ssize_t r_len = read(sock_fd, resp_buf, read_val_len);
            if (r_len > 0) {
                write(client_fd, &read_val_len, sizeof(read_val_len));
                write(client_fd, resp_buf, r_len);
            }
        }
    }
    close(sock_fd);
    close(client_fd);
}

int main() {
    struct ProxyConfig config;
    if (load_config("config.txt", &config) != 0) {
        fprintf(stderr, "Failed to load config.txt\n");
        exit(EXIT_FAILURE);
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("Proxy: Socket failed");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(config.bind_ip);
    addr.sin_port = htons(config.port);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("Proxy: Bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, SOMAXCONN) == -1) {
        perror("Proxy: Listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    set_nonblocking(server_fd);

    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        perror("epoll_create1");
        exit(EXIT_FAILURE);
    }

    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = server_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) == -1) {
        perror("epoll_ctl: server_fd");
        exit(EXIT_FAILURE);
    }

    printf("Non-blocking epoll Proxy listening on port %d with consistent hashing...\n", config.port);

    while (1) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == server_fd) {
                while (1) {
                    struct sockaddr_in client_addr;
                    socklen_t client_len = sizeof(client_addr);
                    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
                    if (client_fd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break; // Flushed all incoming connections
                        }
                        break;
                    }
                    set_nonblocking(client_fd);
                    handle_client(client_fd, &config);
                }
            }
        }
    }

    close(server_fd);
    close(epoll_fd);
    return 0;
}