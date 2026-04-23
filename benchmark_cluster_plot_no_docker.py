import argparse
import concurrent.futures
import json
import math
import os
import random
import statistics
import subprocess
import threading
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter

from consistent_hash import ConsistentHashRing

DEFAULT_VIRTUAL_NODES = 50
READ_MODES = {"hash", "random-hop"}
NODES_FILE = "nodes.json"

def load_nodes_from_file(path: str):
    with open(path, "r") as f:
        parsed = json.load(f)

    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict) and isinstance(parsed.get("nodes"), list):
        return parsed["nodes"]

    raise ValueError("nodes file must be a JSON list or an object with a 'nodes' list")

NODES = load_nodes_from_file(NODES_FILE)

# compute which entry URL to send the request for a given key
def resolve_entry_url(ring, key: str) -> str:
    try:
        return ring.get_node(key)
    except KeyError as exc:
        raise RuntimeError(f"Missing entry URL for owner of {key}") from exc


def resolve_read_target(ring, key: str, read_mode: str) -> str:
    if read_mode == "hash":
        return resolve_entry_url(ring, key)

    if read_mode == "random-hop":
        if len(NODES) == 0:
            raise RuntimeError("Node table is empty")
        return random.choice(NODES)

    raise ValueError(f"Unsupported read mode: {read_mode}")


_thread_local = threading.local()

# avoid creating too many sessions across threads which can exhaust system resources
# which also could avoid everytime rebuild TCP connection
def get_session():
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=200, pool_maxsize=200)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _thread_local.session = session
    return session

# write a key-value pair
def put_key(ring, idx: int, timeout: float):
    key = f"key_{idx}"
    data = {"value": f"value_{idx}"}
    base_url = resolve_entry_url(ring, key)
    start = time.time()
    try:
        response = get_session().post(f"{base_url}/{key}", json=data, timeout=timeout)
        latency = time.time() - start
        success = response.status_code == 200
        response.close()
        return success, latency
    except Exception:
        return False, 0.0

# get the kv pair
def get_key(ring, idx: int, timeout: float, read_mode: str):
    key = f"key_{idx}"
    base_url = resolve_read_target(ring, key, read_mode)
    start = time.time()
    try:
        response = get_session().get(f"{base_url}/{key}", timeout=timeout)
        latency = time.time() - start
        success = response.status_code == 200
        response.close()
        return success, latency
    except Exception:
        return False, 0.0

# two phases: write phase and read phase, both can be parallelized with ThreadPoolExecutor
def run_workload(ring, num_keys: int, read_cycles: int, max_workers: int, timeout: float, read_mode: str):
    # Phase 1 (write) and Phase 2 (read) follow benchmark.py's flow.
    write_latencies = []
    write_success = 0
    write_start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        write_results = list(executor.map(lambda i: put_key(ring, i, timeout), range(num_keys)))

    for success, latency in write_results:
        if success:
            write_success += 1
            write_latencies.append(latency)

    write_total_time = max(time.time() - write_start, 1e-9)

    read_latencies = []
    read_success = 0
    read_start = time.time()
    tasks = list(range(num_keys)) * read_cycles

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        read_results = list(
            executor.map(lambda key: get_key(ring, key, timeout, read_mode), tasks)
        )

    for success, latency in read_results:
        if success:
            read_success += 1
            read_latencies.append(latency)

    read_total_time = max(time.time() - read_start, 1e-9)

    total_success = write_success + read_success
    total_time = max(write_total_time + read_total_time, 1e-9)
    all_latencies = write_latencies + read_latencies

    return {
        "write_success": write_success,
        "write_total": num_keys,
        "write_fail": num_keys - write_success,
        "write_throughput": write_success / write_total_time,
        "write_avg_latency": statistics.mean(write_latencies) if write_latencies else float("nan"),
        "read_success": read_success,
        "read_total": len(tasks),
        "read_fail": len(tasks) - read_success,
        "read_throughput": read_success / read_total_time,
        "read_avg_latency": statistics.mean(read_latencies) if read_latencies else float("nan"),
        "overall_throughput": total_success / total_time,
        "overall_avg_latency": statistics.mean(all_latencies) if all_latencies else float("nan"),
    }

# compute median and standard deviation, ignoring non-finite values
def stats(values):
    filtered = [v for v in values if isinstance(v, (int, float)) and math.isfinite(v)]
    if not filtered:
        return float("nan"), 0.0
    if len(filtered) == 1:
        return filtered[0], 0.0
    return statistics.median(filtered), statistics.stdev(filtered)

def main():

    # before this, the kv stores should be up

    parser = argparse.ArgumentParser(description="Run benchmark for 1..N nodes and draw result plots")
    parser.add_argument("--base-port", type=int, default=8081, help="Host port of kv1 entrypoint")
    parser.add_argument("--max-nodes", type=int, default=3, help="Maximum node count to test")
    parser.add_argument("--runs", type=int, default=10, help="Benchmark repeats per node count")
    parser.add_argument("--num-keys", type=int, default=300, help="Number of keys in write phase")
    parser.add_argument("--read-cycles", type=int, default=1, help="Read cycles after write phase")
    parser.add_argument(
        "--read-mode",
        choices=sorted(READ_MODES),
        default="random-hop",
        help="Read routing strategy: hash routes directly to the owner, random-hop picks any entry node first",
    )
    parser.add_argument("--workers", type=int, default=6, help="ThreadPool max workers")
    parser.add_argument("--timeout", type=float, default=5.0, help="HTTP timeout per request")
    parser.add_argument("--settle-seconds", type=float, default=2.0, help="Wait time after compose up")
    parser.add_argument("--output", default="benchmark_nodes_1_to_3.png", help="Output plot filename")
    parser.add_argument("--output-json", default="benchmark_nodes_1_to_3.json", help="Output raw result JSON")
    args = parser.parse_args()

    work_dir = Path(__file__).resolve().parent

    x_nodes = []
    throughput_medians = []
    throughput_sds = []
    latency_medians = []
    latency_sds = []
    raw = {}
    print(f"Read mode: {args.read_mode}")

    #for node_count in range(1, args.max_nodes + 1):
    node_count = 3

    print(f"\n=== Node count: {node_count} ===")

    ring = ConsistentHashRing(NODES, virtual_nodes=DEFAULT_VIRTUAL_NODES)

    throughputs = []
    latencies = []
    node_runs = []

    for run_idx in range(1, args.runs + 1):
        result = run_workload(
            ring=ring,
            num_keys=args.num_keys,
            read_cycles=args.read_cycles,
            max_workers=args.workers,
            timeout=args.timeout,
            read_mode=args.read_mode,
        )
        node_runs.append(result)
        throughputs.append(result["overall_throughput"])
        latencies.append(result["overall_avg_latency"])
        print(
            f"Run {run_idx}: overall_throughput={result['overall_throughput']:.2f} req/s, "
            f"overall_avg_latency={result['overall_avg_latency']:.5f}s, "
            f"write_fail={result['write_fail']}, read_fail={result['read_fail']}"
        )

    t_med, t_sd = stats(throughputs)
    l_med, l_sd = stats(latencies)

    print(
        f"Summary n={node_count}: throughput median={t_med:.2f} sd={t_sd:.2f}, "
        f"latency median={l_med:.5f}s sd={l_sd:.5f}s"
    )

if __name__ == "__main__":
    main()
