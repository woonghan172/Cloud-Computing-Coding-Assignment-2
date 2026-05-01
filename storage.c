#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <pthread.h>

#define KEY_SIZE 256
#define VAL_SIZE 4096
#define HASH_TABLE_SIZE 10000

struct KeyValueNode {
    char key[KEY_SIZE];
    char value[VAL_SIZE];
    struct KeyValueNode *next;
};

struct KeyValueNode *hash_table[HASH_TABLE_SIZE];
pthread_mutex_t bucket_locks[HASH_TABLE_SIZE];

uint32_t compute_hash(const char *str) {
    uint32_t hash = 5381;
    int c;

    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + (uint32_t)c;
    }

    return hash;
}

void init_hash_table(void) {
    for (int i = 0; i < HASH_TABLE_SIZE; i++) {
        pthread_mutex_init(&bucket_locks[i], NULL);
    }
}

struct __attribute__((packed)) MsgHeader {
    uint8_t magic;
    uint8_t command;
    uint16_t key_len;
    uint32_t val_len;
};

void kv_set(const char *key, const char *value) {
    uint32_t idx = compute_hash(key) % HASH_TABLE_SIZE;

    pthread_mutex_lock(&bucket_locks[idx]);

    struct KeyValueNode *cur = hash_table[idx];

    while (cur != NULL) {
        if (strcmp(cur->key, key) == 0) {
            strncpy(cur->value, value, VAL_SIZE - 1);
            cur->value[VAL_SIZE - 1] = '\0';
            pthread_mutex_unlock(&bucket_locks[idx]);
            return;
        }
        cur = cur->next;
    }

    struct KeyValueNode *new_node = malloc(sizeof(struct KeyValueNode));
    if (new_node == NULL) {
        pthread_mutex_unlock(&bucket_locks[idx]);
        return;
    }

    strncpy(new_node->key, key, KEY_SIZE - 1);
    new_node->key[KEY_SIZE - 1] = '\0';

    strncpy(new_node->value, value, VAL_SIZE - 1);
    new_node->value[VAL_SIZE - 1] = '\0';

    new_node->next = hash_table[idx];
    hash_table[idx] = new_node;

    pthread_mutex_unlock(&bucket_locks[idx]);
}

int kv_get(const char *key, char *out_val, size_t *out_len) {
    uint32_t idx = compute_hash(key) % HASH_TABLE_SIZE;

    pthread_mutex_lock(&bucket_locks[idx]);

    struct KeyValueNode *cur = hash_table[idx];

    while (cur != NULL) {
        if (strcmp(cur->key, key) == 0) {
            size_t len = strlen(cur->value);

            strncpy(out_val, cur->value, VAL_SIZE - 1);
            out_val[VAL_SIZE - 1] = '\0';

            *out_len = len;

            pthread_mutex_unlock(&bucket_locks[idx]);
            return 0;
        }

        cur = cur->next;
    }

    pthread_mutex_unlock(&bucket_locks[idx]);
    return 1;
}

int kv_del(const char *key) {
    uint32_t idx = compute_hash(key) % HASH_TABLE_SIZE;

    pthread_mutex_lock(&bucket_locks[idx]);

    struct KeyValueNode *cur = hash_table[idx];
    struct KeyValueNode *prev = NULL;

    while (cur != NULL) {
        if (strcmp(cur->key, key) == 0) {
            if (prev == NULL) {
                hash_table[idx] = cur->next;
            } else {
                prev->next = cur->next;
            }

            free(cur);

            pthread_mutex_unlock(&bucket_locks[idx]);
            return 0;
        }

        prev = cur;
        cur = cur->next;
    }

    pthread_mutex_unlock(&bucket_locks[idx]);
    return 1;
}

int load_storage_config(const char *filename, int storage_id, char *out_ip, int *out_port) {
    FILE *file = fopen(filename, "r");
    if (!file) return -1;

    char line[256];
    char target_key[32];
    snprintf(target_key, sizeof(target_key), "STORAGE_%d", storage_id);

    while (fgets(line, sizeof(line), file)) {
        if (line[0] == '#' || line[0] == '\n') continue;

        char key[64], value[128];
        if (sscanf(line, "%63[^=]=%127[^\n]", key, value) == 2) {
            if (strcmp(key, target_key) == 0) {
                char ip[64];
                int port;
                if (sscanf(value, "%63[^:]:%d", ip, &port) == 2) {
                    strncpy(out_ip, ip, 63);
                    *out_port = port;
                    fclose(file);
                    return 0;
                }
            }
        }
    }
    fclose(file);
    return -1;
}

int main(int argc, char *argv[]) {
    init_hash_table();

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <storage_id>\nExample: %s 1\n", argv[0], argv[0]);
        exit(EXIT_FAILURE);
    }

    int storage_id = atoi(argv[1]);
    char ip[64] = "127.0.0.1";
    int port = 9001;

    if (load_storage_config("config.txt", storage_id, ip, &port) != 0) {
        fprintf(stderr, "Failed to find storage ID %d in config.txt. Using defaults.\n", storage_id);
    }

    int server_fd;
    struct sockaddr_in addr;

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("Storage: Socket failed");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(ip);
    addr.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("Storage: Bind failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) == -1) {
        perror("Storage: Listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    printf("Storage engine (ID: %d) listening on %s:%d\n", storage_id, ip, port);

    while (1) {
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) continue;

        struct MsgHeader header;
        if (read(client_fd, &header, sizeof(header)) == sizeof(header)) {
            char key[KEY_SIZE] = {0};
            char val[VAL_SIZE] = {0};

            if (header.key_len > 0 && header.key_len < sizeof(key)) {
                read(client_fd, key, header.key_len);
            }
            if (header.command == 0x02 && header.val_len > 0 && header.val_len < sizeof(val)) {
                read(client_fd, val, header.val_len);
            }

            if (header.command == 0x01) {
                char out_val[VAL_SIZE];
                size_t out_len = 0;
                int res = kv_get(key, out_val, &out_len);
                //printf("[storage] get request - key: %s / val: %s \n",key, out_val);
                if (res == 0) {
                    uint8_t status = 0x00;
                    write(client_fd, &status, 1);
                    uint32_t send_len = (uint32_t)out_len;
                    write(client_fd, &send_len, sizeof(send_len));
                    write(client_fd, out_val, send_len);
                } else {
                    uint8_t status = 0x01;
                    write(client_fd, &status, 1);
                }
            } else if (header.command == 0x02) {
                kv_set(key, val);
                uint8_t status = 0x00;
                write(client_fd, &status, 1);
            } else if (header.command == 0x03) {
                int res = kv_del(key);
                uint8_t status = (res == 0) ? 0x00 : 0x01;
                write(client_fd, &status, 1);
            }
        }
        close(client_fd);
    }
    close(server_fd);
    return 0;
}