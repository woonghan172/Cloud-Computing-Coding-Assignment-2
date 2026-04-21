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

try:
    import matplotlib.pyplot as plt
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "matplotlib is required for plotting. Install it with: pip install matplotlib"
    ) from exc

# ring: to find the owner node for a key
# route_table: to find the entry URL for an owner node
def build_routing_table(node_count: int, base_port: int, virtual_nodes: int):
    if node_count < 1:
        raise ValueError("node_count must be >= 1")

    nodes = [f"http://kv{i}:8080" for i in range(1, node_count + 1)]
    entry_urls = [f"http://127.0.0.1:{base_port + (i - 1)}" for i in range(1, node_count + 1)]
    ring = ConsistentHashRing(nodes=nodes, virtual_nodes=virtual_nodes)
    route_table = dict(zip(nodes, entry_urls))
    return ring, route_table

# compute which entry URL to send the request for a given key
def resolve_entry_url(route_table, ring, key: str) -> str:
    owner = ring.get_node(key)
    try:
        return route_table[owner]
    except KeyError as exc:
        raise RuntimeError(f"Missing entry URL for owner {owner}") from exc


def resolve_read_target(route_table, ring, key: str, read_mode: str) -> str:
    if read_mode == "hash":
        return resolve_entry_url(route_table, ring, key)

    if read_mode == "random-hop":
        if not route_table:
            raise RuntimeError("Route table is empty")
        return random.choice(list(route_table.values()))

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
def put_key(route_table, ring, idx: int, timeout: float):
    key = f"key_{idx}"
    data = {"value": f"value_{idx}"}
    base_url = resolve_entry_url(route_table, ring, key)
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
def get_key(route_table, ring, idx: int, timeout: float, read_mode: str):
    key = f"key_{idx}"
    base_url = resolve_read_target(route_table, ring, key, read_mode)
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
def run_workload(route_table, ring, num_keys: int, read_cycles: int, max_workers: int, timeout: float, read_mode: str):
    # Phase 1 (write) and Phase 2 (read) follow benchmark.py's flow.
    write_latencies = []
    write_success = 0
    write_start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        write_results = list(executor.map(lambda i: put_key(route_table, ring, i, timeout), range(num_keys)))

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
            executor.map(lambda key: get_key(route_table, ring, key, timeout, read_mode), tasks)
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

# control the docker command
def compose_up(work_dir: Path, node_count: int):
    services = [f"kv{i}" for i in range(1, node_count + 1)]
    nodes_list = ",".join([f"http://kv{i}:8080" for i in range(1, node_count + 1)])

    env = os.environ.copy()
    env["NODES_LIST"] = nodes_list

    cmd = ["docker", "compose", "up", "-d", *services]
    subprocess.run(cmd, cwd=work_dir, env=env, check=True)


# control the docker command
def compose_down(work_dir: Path, node_count: int):
    nodes_list = ",".join([f"http://kv{i}:8080" for i in range(1, node_count + 1)])
    env = os.environ.copy()
    env["NODES_LIST"] = nodes_list

    cmd = ["docker", "compose", "down"]
    subprocess.run(cmd, cwd=work_dir, env=env, check=False)

# compute median and standard deviation, ignoring non-finite values
def stats(values):
    filtered = [v for v in values if isinstance(v, (int, float)) and math.isfinite(v)]
    if not filtered:
        return float("nan"), 0.0
    if len(filtered) == 1:
        return filtered[0], 0.0
    return statistics.median(filtered), statistics.stdev(filtered)


# for the graph
def plot_results(output_png: Path, x_nodes, throughput_med, throughput_sd, latency_med, latency_sd):
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    axes[0].errorbar(
        x_nodes,
        throughput_med,
        yerr=throughput_sd,
        fmt="o-",
        color="blue",
        ecolor="blue",
        capsize=6,
        label="Median + SD",
    )
    axes[0].set_title("Throughput vs Cluster Size")
    axes[0].set_xlabel("Number of Nodes")
    axes[0].set_ylabel("Requests / Second")
    axes[0].grid(True, linestyle="--", alpha=0.5)
    axes[0].legend()

    axes[1].errorbar(
        x_nodes,
        latency_med,
        yerr=latency_sd,
        fmt="s-",
        color="red",
        ecolor="red",
        capsize=6,
        label="Median + SD",
    )
    axes[1].set_title("Latency vs Cluster Size")
    axes[1].set_xlabel("Number of Nodes")
    axes[1].set_ylabel("Avg Latency (Seconds)")
    axes[1].grid(True, linestyle="--", alpha=0.5)
    axes[1].legend()

    fig.tight_layout()
    fig.savefig(output_png, dpi=150)


def suffix_output_name(path: Path, suffix: str) -> Path:
    return path.with_name(f"{path.stem}_{suffix}{path.suffix}")



def main():
    parser = argparse.ArgumentParser(description="Run benchmark for 1..N nodes and draw result plots")
    parser.add_argument("--base-port", type=int, default=8081, help="Host port of kv1 entrypoint")
    parser.add_argument("--max-nodes", type=int, default=3, help="Maximum node count to test")
    parser.add_argument("--runs", type=int, default=10, help="Benchmark repeats per node count")
    parser.add_argument("--num-keys", type=int, default=300, help="Number of keys in write phase")
    parser.add_argument("--read-cycles", type=int, default=1, help="Read cycles after write phase")
    parser.add_argument(
        "--read-mode",
        choices=sorted(READ_MODES),
        default="hash",
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

    for node_count in range(1, args.max_nodes + 1):
        print(f"\n=== Node count: {node_count} ===")
        compose_up(work_dir, node_count)
        time.sleep(args.settle_seconds)

        ring, route_table = build_routing_table(node_count, args.base_port, DEFAULT_VIRTUAL_NODES)
        print("Routing table:")
        for owner, entry in sorted(route_table.items()):
            print(f"  {owner} -> {entry}")
        throughputs = []
        latencies = []
        node_runs = []

        try:
            for run_idx in range(1, args.runs + 1):
                result = run_workload(
                    route_table=route_table,
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
        finally:
            compose_down(work_dir, node_count)

        t_med, t_sd = stats(throughputs)
        l_med, l_sd = stats(latencies)

        x_nodes.append(node_count)
        throughput_medians.append(t_med)
        throughput_sds.append(t_sd)
        latency_medians.append(l_med)
        latency_sds.append(l_sd)
        raw[str(node_count)] = node_runs

        print(
            f"Summary n={node_count}: throughput median={t_med:.2f} sd={t_sd:.2f}, "
            f"latency median={l_med:.5f}s sd={l_sd:.5f}s"
        )

    output_png = work_dir / args.output
    output_json = work_dir / args.output_json

    if args.output == "benchmark_nodes_1_to_3.png":
        output_png = suffix_output_name(output_png, args.read_mode.replace("-", "_"))

    if args.output_json == "benchmark_nodes_1_to_3.json":
        output_json = suffix_output_name(output_json, args.read_mode.replace("-", "_"))

    plot_results(
        output_png=output_png,
        x_nodes=x_nodes,
        throughput_med=throughput_medians,
        throughput_sd=throughput_sds,
        latency_med=latency_medians,
        latency_sd=latency_sds,
    )

    with open(output_json, "w") as f:
        json.dump(
            {
                "x_nodes": x_nodes,
                "throughput_median": throughput_medians,
                "throughput_sd": throughput_sds,
                "latency_median": latency_medians,
                "latency_sd": latency_sds,
                "raw": raw,
            },
            f,
            indent=2,
        )

    print(f"\nSaved plot: {output_png}")
    print(f"Saved raw stats: {output_json}")


if __name__ == "__main__":
    main()
