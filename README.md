# C version of centralized KB store

## How to Run

1. Edit **config.txt** according to the IP and port of each module.
2. Compile the files
```
make
```
3. Run three storage nodes
```
./storage 1
./storage 2
./storage 3
```
4. Run the proxy
```
./proxy
```
5. Run benchmark
```
./benchmark 
:8083: 127.0.0.1
Benchmark: 4 workers × (2500 writes + 2500 reads)  (warmup=200)

============== BENCHMARK RESULTS ==============
Total time             : 0.4860 s
Total ops  (r+w)       : 5850
Throughput             : 12037.09 ops/sec
-------------- Latency (µs) -------------------
Write  avg / p99       : 147.85 µs / 495.56 µs
Read   avg / p99       : 129.92 µs / 367.33 µs
===============================================
```
5-1. Also can run benchmark with setting number of worker threads.

```
./benchmark 32
:8083: 127.0.0.1
Benchmark: 32 workers × (312 writes + 312 reads)  (warmup=200)

============== BENCHMARK RESULTS ==============
Total time             : 0.1925 s
Total ops  (r+w)       : 6073
Throughput             : 31553.98 ops/sec
-------------- Latency (µs) -------------------
Write  avg / p99       : 383.55 µs / 733.42 µs
Read   avg / p99       : 335.30 µs / 710.24 µs
===============================================
```

