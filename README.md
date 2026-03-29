## KV Store Quickstart

### Problem Now
If writing the same key, it would just overwrite the value.

### Setup
```bash
python3 -m venv venv
source venv/bin/activate
pip install fastapi uvicorn requests
```

### Run the server
```bash
python main.py
```
Uvicorn listens on `0.0.0.0:8080`.

### Manual test (new terminal, same venv)
```bash
curl http://127.0.0.1:8080/
curl -X POST http://127.0.0.1:8080/foo -H "Content-Type: application/json" -d '{"value":"bar"}'
curl http://127.0.0.1:8080/foo
curl -X DELETE http://127.0.0.1:8080/foo
curl http://127.0.0.1:8080/all
```

### Benchmark
Keep the server running, then in another terminal:
```bash
python benchmark.py
```
It uses `BASE_URL = http://127.0.0.1:8080` and prints throughput/latency stats.

### Scale From 1 To 3 KV Nodes (Consistent Hashing)

The same `main.py` now supports multi-node routing with consistent hashing.

Environment variables:
- `SELF_NODE`: this node URL, for example `http://127.0.0.1:8081`
- `NODES`: comma-separated node list used by the hash ring
- `PORT`: local listening port
- `DATA_FILE`: persistence file per node

Use the same `NODES` list for all nodes in one experiment.

#### 1 node
```bash
NODES="http://127.0.0.1:8081" \
SELF_NODE="http://127.0.0.1:8081" \
PORT=8081 DATA_FILE=data1.json \
python main.py
```

Run benchmark against node1:
```bash
python benchmark.py --base-url http://127.0.0.1:8081 --runs 3
```

#### 2 nodes
Terminal 1:
```bash
NODES="http://127.0.0.1:8081,http://127.0.0.1:8082" \
SELF_NODE="http://127.0.0.1:8081" \
PORT=8081 DATA_FILE=data1.json \
python main.py
```

Terminal 2:
```bash
NODES="http://127.0.0.1:8081,http://127.0.0.1:8082" \
SELF_NODE="http://127.0.0.1:8082" \
PORT=8082 DATA_FILE=data2.json \
python main.py
```

Benchmark:
```bash
python benchmark.py --base-url http://127.0.0.1:8081 --runs 3
```

#### 3 nodes
Terminal 1:
```bash
NODES="http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083" \
SELF_NODE="http://127.0.0.1:8081" \
PORT=8081 DATA_FILE=data1.json \
python main.py
```

Terminal 2:
```bash
NODES="http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083" \
SELF_NODE="http://127.0.0.1:8082" \
PORT=8082 DATA_FILE=data2.json \
python main.py
```

Terminal 3:
```bash
NODES="http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083" \
SELF_NODE="http://127.0.0.1:8083" \
PORT=8083 DATA_FILE=data3.json \
python main.py
```

Benchmark:
```bash
python benchmark.py --base-url http://127.0.0.1:8081 --runs 3
```

### Integratio Test
Keep the server running, then in another terminal:
```bash
python integration_test.py
```
Expected result
```bash
Ran 2 tests in x.xxs

OK
```

### Docker Compose (1 -> 3 nodes)

You can now run the same experiment with containers using `docker-compose.yml`.

Build once:
```bash
docker compose build
```

#### 1 node
```bash
NODES_LIST="http://kv1:8080" docker compose up -d kv1
python benchmark.py --base-url http://127.0.0.1:8081 --runs 3
docker compose down
```

#### 2 nodes
```bash
NODES_LIST="http://kv1:8080,http://kv2:8080" docker compose up -d kv1 kv2
python benchmark.py --base-url http://127.0.0.1:8081 --runs 3
docker compose down
```

#### 3 nodes
```bash
NODES_LIST="http://kv1:8080,http://kv2:8080,http://kv3:8080" docker compose up -d kv1 kv2 kv3
python benchmark.py --base-url http://127.0.0.1:8081 --runs 3
docker compose down
```

Notes:
- Keep `NODES_LIST` identical for all running nodes in one experiment stage.
- Use `docker compose logs -f kv1` to inspect forwarding and errors.
- Port mapping is `kv1->8081`, `kv2->8082`, `kv3->8083` on host.

### Benchmark Experiment Guide (1 -> 3 nodes)

Use exactly the same benchmark parameters for all stages so the results are comparable.

Recommended command template:
```bash
python benchmark.py --base-url <ENTRY_URL> --threads 8 --ops-per-thread 200 --key-space 300 --runs 3
```

By default, benchmark does **not** shuffle operations (set first, then get), which avoids GET-before-SET failures.
Add `--shuffle` only if you want a mixed workload stress test.

#### Stage A: 1 node baseline
```bash
NODES_LIST="http://kv1:8080" docker compose up -d kv1
python benchmark.py --base-url http://127.0.0.1:8081 --threads 8 --ops-per-thread 200 --key-space 300 --runs 3
docker compose down
```

#### Stage B: 2 nodes
```bash
NODES_LIST="http://kv1:8080,http://kv2:8080" docker compose up -d kv1 kv2
python benchmark.py --base-url http://127.0.0.1:8081 --threads 8 --ops-per-thread 200 --key-space 300 --runs 3
docker compose down
```

#### Stage C: 3 nodes
```bash
NODES_LIST="http://kv1:8080,http://kv2:8080,http://kv3:8080" docker compose up -d kv1 kv2 kv3
python benchmark.py --base-url http://127.0.0.1:8081 --threads 8 --ops-per-thread 200 --key-space 300 --runs 3
docker compose down
```

Optional clean run between stages (remove old data volumes):
```bash
docker compose down -v
```

Record these metrics from the benchmark summary for each stage:
- average throughput
- average latency
- average p95 latency