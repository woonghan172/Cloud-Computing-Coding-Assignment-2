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