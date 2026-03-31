import sys
import yaml
import subprocess
import os
import shutil

def create_docker_compose(num_nodes):
    nodes_list = [f"http://kv-{i}:8080" for i in range(1, num_nodes + 1)]
    
    compose_dict = {
        "services": {
            "proxy": {
                "build": {"context": ".", "dockerfile": "Dockerfile.proxy"},
                "ports": ["8080:8080"],
                "environment": {
                    "NODE_LIST": ",".join(nodes_list)
                },
                "depends_on": [f"kv-{i}" for i in range(1, num_nodes + 1)]
            }
        }
    }

    for i in range(1, num_nodes + 1):
        node_name = f"kv-{i}"
        node_dir = f"./data/{node_name}"
        
        os.makedirs(node_dir, exist_ok=True)
        for f_name in ["data.json", "logger.log"]:
            path = os.path.join(node_dir, f_name)
            if not os.path.exists(path):
                with open(path, "w") as f:
                    f.write("{}" if f_name == "data.json" else "")

        compose_dict["services"][node_name] = {
            "build": ".",
            "volumes": [
                f"{node_dir}/data.json:/app/data.json",
                f"{node_dir}/logger.log:/app/logger.log"
            ]
        }

    with open("docker-compose.yml", "w") as f:
        yaml.dump(compose_dict, f)

def start_nodes(num_nodes, fresh=False):
    if fresh:
        print("--- Cleaning up old data files (-f) ---")
        if os.path.exists("./data"):
            shutil.rmtree("./data")
        # Ensure we also clean up Docker's internal state
        subprocess.run(["docker", "compose", "down", "-v"], capture_output=True)

    print(f"--- Initializing {num_nodes} nodes ---")
    create_docker_compose(num_nodes)
    
    # --build ensures the NODE_LIST env is fresh
    subprocess.run(["docker", "compose", "up", "--build", "-d"])
    print(f"--- Cluster is UP at http://localhost:8080 ---")

def stop_nodes():
    print("--- Gracefully shutting down all nodes ---")
    subprocess.run(["docker", "compose", "down"])
    print("--- System Offline ---")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 controller.py 1 <num_nodes> [-f] (Initiate, -f for fresh)")
        print("  python3 controller.py 2                 (Shutdown)")
        sys.exit(1)

    command = sys.argv[1]

    if command == "1":
        if len(sys.argv) < 3:
            print("Error: Specify number of nodes.")
        else:
            num = int(sys.argv[2])
            is_fresh = "-f" in sys.argv
            start_nodes(num, fresh=is_fresh)
    
    elif command == "2":
        stop_nodes()