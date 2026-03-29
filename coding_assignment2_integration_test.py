import requests
import time
import concurrent.futures
from collections import Counter

BASE_URL = "http://localhost:8080"
NUM_KEYS = 60
READ_CYCLES = 3

def put_key(i):
    key = f"key_{i}"
    data = {"value": f"value_{i}"}
    start = time.time()
    try:
        response = requests.post(f"{BASE_URL}/{key}", json=data, timeout=2)
        latency = time.time() - start
        # The proxy/node returns the status. 
        # In a real setup, we'd return which node handled it.
        # For now, we assume the request was successful.
        return True, latency
    except Exception as e:
        return False, 0

def get_key(i):
    key = f"key_{i}"
    start = time.time()
    try:
        response = requests.get(f"{BASE_URL}/{key}", timeout=2)
        latency = time.time() - start
        if response.status_code == 200:
            # We assume your proxy might return some metadata or we check logs
            return True, latency
        return False, latency
    except Exception as e:
        return False, 0

def run_benchmark():
    # 1. WRITE PHASE
    print(f"--- Phase 1: Writing {NUM_KEYS} keys ---")
    start_time = time.time()
    latencies = []
    success_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(put_key, range(NUM_KEYS)))
    
    for success, lat in results:
        if success:
            success_count += 1
            latencies.append(lat)
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"Writes Completed: {success_count}/{NUM_KEYS}")
    print(f"Write Throughput: {success_count / total_time:.2f} req/s")
    print(f"Avg Write Latency: {sum(latencies)/len(latencies):.4f}s\n")

    # 2. READ PHASE
    total_reads = NUM_KEYS * READ_CYCLES
    print(f"--- Phase 2: Reading {NUM_KEYS} keys {READ_CYCLES} times ({total_reads} total) ---")
    
    read_latencies = []
    read_success = 0
    start_time = time.time()
    
    # Flatten the 3 cycles into one list of keys to read
    tasks = list(range(NUM_KEYS)) * READ_CYCLES
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(get_key, tasks))
        
    for success, lat in results:
        if success:
            read_success += 1
            read_latencies.append(lat)
            
    end_time = time.time()
    total_time = end_time - start_time

    print(f"Reads Completed: {read_success}/{total_reads}")
    print(f"Read Throughput: {read_success / total_time:.2f} req/s")
    print(f"Avg Read Latency: {sum(read_latencies)/len(read_latencies):.4f}s\n")

    print("--- Distribution Verification ---")
    print("Check Docker logs (docker compose logs) to see the balance between kv-1, kv-2, and kv-3.")

if __name__ == "__main__":
    run_benchmark()