import subprocess
import time
import requests
import matplotlib.pyplot as plt
import concurrent.futures
import statistics
import sys

# --- CONFIGURATION ---
NODE_COUNTS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
ITERATIONS = 10       # How many times to repeat the test per node count
KEYS_PER_ITER = 300    # Keys to write/read in each iteration
PROXY_URL = "http://localhost:8080"

def run_command(cmd):
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr.decode()}")
        sys.exit(1)

def wait_for_proxy():
    """Polls the proxy until it's ready to handle requests."""
    print("Waiting for Proxy to be ready...", end="", flush=True)
    for _ in range(30):
        try:
            res = requests.get(f"{PROXY_URL}/health_check_non_existent", timeout=1)
            # Even a 404 from the proxy means the server is up
            print(" Ready!")
            return True
        except:
            print(".", end="", flush=True)
            time.sleep(1)
    print(" Failed.")
    return False

def setup_cluster(num_nodes):
    print(f"\n>>> Starting {num_nodes} nodes cluster...")
    run_command(["python3", "controller.py", "1", str(num_nodes), "-f"])
    wait_for_proxy()
    time.sleep(2) # Final stabilization

def teardown_cluster():
    print(">>> Shutting down cluster gracefully...")
    run_command(["docker", "compose", "down", "--volumes"])

def run_single_benchmark():
    """Runs one cycle of RW tests and returns (throughput, avg_latency)."""
    latencies = []
    start_time = time.time()
    
    def rw_task(i):
        key = f"key_{time.time()}_{i}" # Unique keys per iteration
        try:
            # Write
            t0 = time.time()
            requests.post(f"{PROXY_URL}/{key}", json={"value": "test"}, timeout=2)
            # Read
            requests.get(f"{PROXY_URL}/{key}", timeout=2)
            return True, (time.time() - t0)
        except:
            return False, 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(rw_task, range(KEYS_PER_ITER)))
    
    total_duration = time.time() - start_time
    success_lats = [lat for success, lat in results if success]
    
    throughput = len(success_lats) / total_duration
    avg_lat = statistics.mean(success_lats) if success_lats else 0
    return throughput, avg_lat

def plot_results(counts, t_stats, l_stats):
    """
    t_stats: list of (median, std_dev) for throughput
    l_stats: list of (median, std_dev) for latency
    """
    plt.figure(figsize=(12, 5))

    # Extract data
    t_medians = [s[0] for s in t_stats]
    t_errors = [s[1] for s in t_stats]
    l_medians = [s[0] for s in l_stats]
    l_errors = [s[1] for s in l_stats]

    # Subplot 1: Throughput
    plt.subplot(1, 2, 1)
    plt.errorbar(counts, t_medians, yerr=t_errors, fmt='-o', capsize=5, color='blue', label='Median + SD')
    plt.title('Throughput vs Cluster Size')
    plt.xlabel('Number of Nodes')
    plt.ylabel('Requests / Second')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()

    # Subplot 2: Latency
    plt.subplot(1, 2, 2)
    plt.errorbar(counts, l_medians, yerr=l_errors, fmt='-s', capsize=5, color='red', label='Median + SD')
    plt.title('Latency vs Cluster Size')
    plt.xlabel('Number of Nodes')
    plt.ylabel('Avg Latency (Seconds)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()

    plt.tight_layout()
    plt.savefig('scaling_statistics.png')
    print("\nPlot saved as 'scaling_statistics.png'")
    plt.show()

if __name__ == "__main__":
    final_throughput_stats = []
    final_latency_stats = []

    for count in NODE_COUNTS:
        setup_cluster(count)
        
        iter_throughputs = []
        iter_latencies = []

        print(f"Running {ITERATIONS} iterations for {count} nodes...")
        for i in range(ITERATIONS):
            t, l = run_single_benchmark()
            iter_throughputs.append(t)
            iter_latencies.append(l)
            print(f"  Iteration {i+1}: {t:.2f} req/s, {l:.4f}s")

        # Calculate Stats
        t_med = statistics.median(iter_throughputs)
        t_std = statistics.stdev(iter_throughputs) if ITERATIONS > 1 else 0
        l_med = statistics.median(iter_latencies)
        l_std = statistics.stdev(iter_latencies) if ITERATIONS > 1 else 0

        final_throughput_stats.append((t_med, t_std))
        final_latency_stats.append((l_med, l_std))
        
        teardown_cluster()

    plot_results(NODE_COUNTS, final_throughput_stats, final_latency_stats)