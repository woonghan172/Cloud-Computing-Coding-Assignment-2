#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdint.h>

struct __attribute__((packed)) MsgHeader {
    uint8_t magic;
    uint8_t command;
    uint16_t key_len;
    uint32_t val_len;
};

struct ProxyAddress {
    char ip[64];
    int port;
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

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage:\n");
        fprintf(stderr, "  Write: %s put <key> <payload>\n", argv[0]);
        fprintf(stderr, "  Read:  %s get <key>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    char *op = argv[1];
    char *key = argv[2];
    char *payload = NULL;
    uint8_t command = 0;

    if (strcmp(op, "put") == 0) {
        if (argc < 4) {
            fprintf(stderr, "Error: Payload required for 'put' operation.\n");
            exit(EXIT_FAILURE);
        }
        command = 0x02; // SET
        payload = argv[3];
    } else if (strcmp(op, "get") == 0) {
        command = 0x01; // GET
    } else {
        fprintf(stderr, "Invalid command. Use 'put' or 'get'.\n");
        exit(EXIT_FAILURE);
    }

    // Load configuration
    struct ProxyAddress proxy;
    if (load_proxy_address("config.txt", &proxy) != 0) {
        strcpy(proxy.ip, "127.0.0.1");
        proxy.port = 8080;
    }

    // Connect to Proxy
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(proxy.port);
    addr.sin_addr.s_addr = inet_addr(proxy.ip);

    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("Connection to proxy failed");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    uint16_t key_len = strlen(key);
    uint32_t val_len = (command == 0x02 && payload) ? strlen(payload) : 0;

    struct MsgHeader header = {
        .magic = 0x4B,
        .command = command,
        .key_len = key_len,
        .val_len = val_len
    };

    // Send Header
    write(sock_fd, &header, sizeof(header));
    
    // Send Key
    write(sock_fd, key, key_len);

    // If 'put', send payload
    if (command == 0x02 && payload) {
        write(sock_fd, payload, val_len);
    }

    // Get status response
    uint8_t status;
    if (read(sock_fd, &status, 1) <= 0) {
        perror("Failed to read status");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    if (status == 0x00) {
        printf("[SUCCESS] Operation '%s' completed on proxy.\n", op);

        if (command == 0x01) { // GET
            uint32_t read_val_len = 0;
            if (read(sock_fd, &read_val_len, sizeof(read_val_len)) == sizeof(read_val_len)) {
                if (read_val_len > 0) {
                    char out_buf[4096] = {0};
                    ssize_t n = read(sock_fd, out_buf, read_val_len);
                    if (n > 0) {
                        printf("Payload -> %s\n", out_buf);
                    }
                }
            }
        }
    } else {
        printf("[FAILURE] Operation failed with status code: 0x%02x\n", status);
    }

    close(sock_fd);
    return 0;
}