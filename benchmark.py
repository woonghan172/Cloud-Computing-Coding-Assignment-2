import argparse
import concurrent.futures
import statistics
import threading
import time
from collections import Counter

import requests
from requests.adapters import HTTPAdapter

from consistent_hash import ConsistentHashRing

NUM_KEYS = 300
# for check if the read from the same kv-pair
READ_CYCLES = 3

ROUTE_TABLE = {}
HASH_RING = None

_thread_local = threading.local()


def get_session():
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=200, pool_maxsize=200)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _thread_local.session = session
    return session


def init_routing(entry_nodes: int, base_port: int, virtual_nodes: int):
    if entry_nodes < 1:
        raise ValueError("entry_nodes must be >= 1")

    nodes = [f"http://kv{i}:8080" for i in range(1, entry_nodes + 1)]
    entry_urls = [f"http://127.0.0.1:{base_port + (i - 1)}" for i in range(1, entry_nodes + 1)]

    global ROUTE_TABLE, HASH_RING
    ROUTE_TABLE = dict(zip(nodes, entry_urls))
    HASH_RING = ConsistentHashRing(nodes=nodes, virtual_nodes=virtual_nodes)


def route_for_key(key: str) -> str:
    if HASH_RING is None:
        raise RuntimeError("Hash ring not initialized. Call init_routing first.")

    owner = HASH_RING.get_node(key)
    try:
        return ROUTE_TABLE[owner]
    except KeyError as exc:
        raise RuntimeError(f"Missing entry URL for owner {owner}") from exc

def put_key(i, timeout: float):
    key = f"key_{i}"
    data = {"value": f"value_{i}"}
    base_url = route_for_key(key)
    start = time.time()
    try:
        response = get_session().post(f"{base_url}/{key}", json=data, timeout=timeout)
        latency = time.time() - start
        success = response.status_code == 200
        response.close()
        return success, latency, base_url
    except Exception:
        return False, 0, base_url

def get_key(i, timeout: float):
    key = f"key_{i}"
    base_url = route_for_key(key)
    start = time.time()
    try:
        response = get_session().get(f"{base_url}/{key}", timeout=timeout)
        latency = time.time() - start
        success = response.status_code == 200
        response.close()
        return success, latency, base_url
    except Exception:
        return False, 0, base_url


def avg(values):
    return (sum(values) / len(values)) if values else 0.0


def run_benchmark(timeout: float, workers: int, read_cycles: int):
    # 1. WRITE PHASE
    print(f"--- Phase 1: Writing {NUM_KEYS} keys ---")
    start_time = time.time()
    latencies = []
    success_count = 0
    write_entry_count = Counter()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(lambda i: put_key(i, timeout), range(NUM_KEYS)))
    
    for success, lat, entry in results:
        write_entry_count[entry] += 1
        if success:
            success_count += 1
            latencies.append(lat)
    
    end_time = time.time()
    write_total_time = end_time - start_time
    
    print(f"Writes Completed: {success_count}/{NUM_KEYS}")
    print(f"Write Throughput: {success_count / write_total_time:.2f} req/s")
    print(f"Avg Write Latency: {avg(latencies):.4f}s\n")
    print("Write entry distribution:")
    for entry, cnt in sorted(write_entry_count.items()):
        print(f"  {entry}: {cnt}")
    print()

    # 2. READ PHASE
    total_reads = NUM_KEYS * read_cycles
    print(f"--- Phase 2: Reading {NUM_KEYS} keys {read_cycles} times ({total_reads} total) ---")
    
    read_latencies = []
    read_success = 0
    start_time = time.time()
    read_entry_count = Counter()
    
    # Flatten the read cycles into one list of keys to read
    tasks = list(range(NUM_KEYS)) * read_cycles

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(lambda key: get_key(key, timeout), tasks))
        
    for success, lat, entry in results:
        read_entry_count[entry] += 1
        if success:
            read_success += 1
            read_latencies.append(lat)
            
    end_time = time.time()
    read_total_time = end_time - start_time

    print(f"Reads Completed: {read_success}/{total_reads}")
    print(f"Read Throughput: {read_success / read_total_time:.2f} req/s")
    print(f"Avg Read Latency: {avg(read_latencies):.4f}s\n")
    print("Read entry distribution:")
    for entry, cnt in sorted(read_entry_count.items()):
        print(f"  {entry}: {cnt}")
    print()

    print("--- Distribution Verification ---")
    print("Requests are routed via consistent hashing directly to owner nodes.")
    print("Check docker compose logs to validate owner-side distribution between kv1/kv2/kv3.")

    total_success = success_count + read_success
    total_time_all = max(write_total_time + read_total_time, 1e-9)
    return {
        "write_success": success_count,
        "write_total": NUM_KEYS,
        "write_fail": NUM_KEYS - success_count,
        "write_throughput": success_count / max(write_total_time, 1e-9),
        "write_avg_latency": avg(latencies),
        "read_success": read_success,
        "read_total": total_reads,
        "read_fail": total_reads - read_success,
        "read_throughput": read_success / max(read_total_time, 1e-9),
        "read_avg_latency": avg(read_latencies),
        "overall_throughput": total_success / total_time_all,
        "overall_avg_latency": avg(latencies + read_latencies),
    }


def summarize(values):
    if len(values) == 1:
        return values[0], 0.0
    return statistics.median(values), statistics.stdev(values)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark distributed KV with configurable entry node count")
    parser.add_argument(
        "--entry-nodes",
        type=int,
        default=3,
        help="How many entry nodes to distribute requests across (1..N, mapped to ports 8081..)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=10,
        help="How many iterations to run",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="HTTP timeout per request in seconds",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        help="Thread pool worker count",
    )
    parser.add_argument(
        "--read-cycles",
        type=int,
        default=3,
        help="How many read cycles to run after writes",
    )
    parser.add_argument(
        "--base-port",
        type=int,
        default=8081,
        help="Host port mapped to kv1 (kvN assumed at base_port + N - 1)",
    )
    parser.add_argument(
        "--virtual-nodes",
        type=int,
        default=50,
        help="Virtual node count to mirror server-side hash ring",
    )
    args = parser.parse_args()

    init_routing(
        entry_nodes=max(1, args.entry_nodes),
        base_port=args.base_port,
        virtual_nodes=max(1, args.virtual_nodes),
    )
    print("Entry routing table:")
    for owner, entry in sorted(ROUTE_TABLE.items()):
        print(f"  {owner} -> {entry}")

    overall_tp = []
    overall_lat = []

    for run_idx in range(1, args.runs + 1):
        print(f"\n=== Run {run_idx}/{args.runs} ===")
        result = run_benchmark(timeout=args.timeout, workers=args.workers, read_cycles=args.read_cycles)
        overall_tp.append(result["overall_throughput"])
        overall_lat.append(result["overall_avg_latency"])
        print(
            f"Run {run_idx} summary: overall_throughput={result['overall_throughput']:.2f} req/s, "
            f"overall_avg_latency={result['overall_avg_latency']:.5f}s, "
            f"write_fail={result['write_fail']}, read_fail={result['read_fail']}"
        )

    tp_med, tp_sd = summarize(overall_tp)
    lat_med, lat_sd = summarize(overall_lat)
    print("\n=== Final Summary ===")
    print(f"overall_throughput median={tp_med:.2f} req/s, sd={tp_sd:.2f}")
    print(f"overall_avg_latency median={lat_med:.5f}s, sd={lat_sd:.5f}s")
