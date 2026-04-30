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
#define TABLE_SIZE 10000 // Number of buckets in the hash table

struct KeyValue {
    char key[KEY_SIZE];
    char value[VAL_SIZE];
    struct KeyValue *next;
};

// Protect the hash table across multiple worker threads
pthread_mutex_t hash_mutex = PTHREAD_MUTEX_INITIALIZER;
struct KeyValue *hash_table[TABLE_SIZE];

struct __attribute__((packed)) MsgHeader {
    uint8_t magic;
    uint8_t command;
    uint16_t key_len;
    uint32_t val_len;
};

unsigned long hash_function(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; 
    }
    return hash % TABLE_SIZE;
}

void kv_set(const char *key, const char *value) {
    pthread_mutex_lock(&hash_mutex);
    unsigned long index = hash_function(key);
    struct KeyValue *entry = hash_table[index];

    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            strncpy(entry->value, value, VAL_SIZE - 1);
            pthread_mutex_unlock(&hash_mutex);
            return;
        }
        entry = entry->next;
    }

    struct KeyValue *new_entry = (struct KeyValue *)malloc(sizeof(struct KeyValue));
    strncpy(new_entry->key, key, KEY_SIZE - 1);
    strncpy(new_entry->value, value, VAL_SIZE - 1);
    new_entry->next = hash_table[index];
    hash_table[index] = new_entry;
    pthread_mutex_unlock(&hash_mutex);
}

int kv_get(const char *key, char *out_val, size_t *out_len) {
    pthread_mutex_lock(&hash_mutex);
    unsigned long index = hash_function(key);
    struct KeyValue *entry = hash_table[index];

    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            size_t len = strlen(entry->value);
            strncpy(out_val, entry->value, VAL_SIZE - 1);
            *out_len = len;
            pthread_mutex_unlock(&hash_mutex);
            return 0; // Found
        }
        entry = entry->next;
    }
    pthread_mutex_unlock(&hash_mutex);
    return 1; // Not found
}

int kv_del(const char *key) {
    pthread_mutex_lock(&hash_mutex);
    unsigned long index = hash_function(key);
    struct KeyValue *entry = hash_table[index];
    struct KeyValue *prev = NULL;

    while (entry != NULL) {
        if (strcmp(entry->key, key) == 0) {
            if (prev == NULL) {
                hash_table[index] = entry->next;
            } else {
                prev->next = entry->next;
            }
            free(entry);
            pthread_mutex_unlock(&hash_mutex);
            return 0; // Deleted
        }
        prev = entry;
        entry = entry->next;
    }
    pthread_mutex_unlock(&hash_mutex);
    return 1; // Not found
}

// Thread-worker to handle each client
void *handle_client(void *arg) {
    int client_fd = *((int *)arg);
    free(arg);

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
    return NULL;
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
    for (int i = 0; i < TABLE_SIZE; i++) {
        hash_table[i] = NULL;
    }

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

    if (listen(server_fd, SOMAXCONN) == -1) {
        perror("Storage: Listen failed");
        close(server_fd);
        exit(EXIT_FAILURE);
    }

    printf("Threaded Storage Engine (ID: %d) listening on %s:%d\n", storage_id, ip, port);

    while (1) {
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) continue;

        int *client_sock = malloc(sizeof(int));
        *client_sock = client_fd;

        pthread_t thread_id;
        pthread_create(&thread_id, NULL, handle_client, client_sock);
        pthread_detach(thread_id); // Clean up thread automatically when finished
    }
    close(server_fd);
    return 0;
}