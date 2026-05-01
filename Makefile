CC = gcc
CFLAGS = -Wall -Wextra -O2 -w

all: proxy storage benchmark

proxy: proxy.c
	$(CC) $(CFLAGS) proxy.c -o proxy -pthread

storage: storage.c
	$(CC) $(CFLAGS) storage.c -o storage -pthread

benchmark: benchmark.c
	$(CC) $(CFLAGS) benchmark.c -o benchmark

benchmark_one_request: benchmark_one_request.c
	$(CC) $(CFLAGS) benchmark_one_request.c -o benchmark_one_request

clean:
	rm -f proxy storage benchmark benchmark_one_request