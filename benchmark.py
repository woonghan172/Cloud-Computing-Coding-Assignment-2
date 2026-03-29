import argparse
import queue
import random
import threading
import time
from statistics import mean

import requests


def build_operations(total_ops: int, key_space: int, do_shuffle: bool):
    operations = []
    for i in range(total_ops // 2):
        key = f"key_{i % key_space}"
        value = f"value_{i % key_space}"
        operations.append(("set", key, value))

    for i in range(total_ops - len(operations)):
        key = f"key_{i % key_space}"
        operations.append(("get", key, None))

    if do_shuffle:
        random.shuffle(operations)
    return operations


def call_kv(base_url: str, op_type: str, key: str, value: str | None):
    if op_type == "set":
        response = requests.post(f"{base_url}/{key}", json={"value": value}, timeout=5)
    elif op_type == "get":
        response = requests.get(f"{base_url}/{key}", timeout=5)
    else:
        raise ValueError("Unsupported operation type")
    response.raise_for_status()


def run_once(base_url: str, num_threads: int, ops_per_thread: int, key_space: int, do_shuffle: bool):
    operations = build_operations(num_threads * ops_per_thread, key_space, do_shuffle)
    
    operations_queue = queue.Queue()
    for op in operations:
        operations_queue.put(op)

    latencies = []
    lat_lock = threading.Lock()
    errors = []
    start_event = threading.Event()

    def worker():
        start_event.wait()
        while True:
            try:
                op_type, key, value = operations_queue.get_nowait()
            except queue.Empty:
                return

            begin = time.time()
            try:
                call_kv(base_url, op_type, key, value)
                elapsed = time.time() - begin
                with lat_lock:
                    latencies.append(elapsed)
            except Exception as exc:
                with lat_lock:
                    errors.append(str(exc))

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    for thread in threads:
        thread.start()

    bench_start = time.time()
    start_event.set()
    for thread in threads:
        thread.join()

    total_time = time.time() - bench_start
    success_ops = len(latencies)
    throughput = success_ops / total_time if total_time > 0 else 0.0
    avg_latency = mean(latencies) if latencies else float("nan")
    p95_latency = sorted(latencies)[int(0.95 * len(latencies)) - 1] if latencies else float("nan")

    return {
        "success_ops": success_ops,
        "failed_ops": len(errors),
        "total_time": total_time,
        "throughput": throughput,
        "avg_latency": avg_latency,
        "p95_latency": p95_latency,
    }


def main():
    parser = argparse.ArgumentParser(description="KV benchmark for 1->3 node experiments")
    parser.add_argument("--base-url", default="http://127.0.0.1:8080", help="Entry URL for requests")
    parser.add_argument("--threads", type=int, default=8, help="Worker threads")
    parser.add_argument("--ops-per-thread", type=int, default=200, help="Operations per thread")
    parser.add_argument("--key-space", type=int, default=300, help="Unique key count")
    parser.add_argument("--runs", type=int, default=3, help="How many times to repeat")
    parser.add_argument("--shuffle", action="store_true", help="Shuffle set/get operations (default: disabled)")
    args = parser.parse_args()

    print("Benchmark configuration:")
    print(
        f"base_url={args.base_url}, threads={args.threads}, ops_per_thread={args.ops_per_thread}, "
        f"key_space={args.key_space}, runs={args.runs}, shuffle={args.shuffle}"
    )

    results = []
    for run_idx in range(args.runs):
        result = run_once(
            base_url=args.base_url,
            num_threads=args.threads,
            ops_per_thread=args.ops_per_thread,
            key_space=args.key_space,
            do_shuffle=args.shuffle,
        )
        results.append(result)
        print(
            f"Run {run_idx + 1}: success={result['success_ops']} failed={result['failed_ops']} "
            f"throughput={result['throughput']:.2f} ops/s avg_latency={result['avg_latency']:.5f}s "
            f"p95={result['p95_latency']:.5f}s"
        )

    avg_throughput = mean([r["throughput"] for r in results])
    avg_latency = mean([r["avg_latency"] for r in results])
    avg_p95 = mean([r["p95_latency"] for r in results])

    print("\nSummary (average across runs):")
    print(f"throughput={avg_throughput:.2f} ops/s")
    print(f"avg_latency={avg_latency:.5f} s/op")
    print(f"p95_latency={avg_p95:.5f} s/op")


if __name__ == "__main__":
    main()