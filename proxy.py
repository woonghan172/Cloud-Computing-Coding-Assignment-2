import os, asyncio, httpx, time, sys, logging
from fastapi import FastAPI, HTTPException
from consistent_hashing import ConsistentHashRing

# Unbuffered logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PROXY] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

app = FastAPI()
ring = ConsistentHashRing()
active_nodes = set()
ALL_NODES = os.getenv("NODE_LIST", "").split(",")

async def heartbeat_checker():
    async with httpx.AsyncClient() as client:
        while True:
            for node in ALL_NODES:
                try:
                    res = await client.get(f"{node}/", timeout=0.2)
                    if res.status_code == 200 and node not in active_nodes:
                        logging.info(f"Node ONLINE: {node}")
                        ring.add_node(node)
                        active_nodes.add(node)
                except:
                    if node in active_nodes:
                        logging.error(f"Node OFFLINE: {node}")
                        ring.remove_node(node)
                        active_nodes.remove(node)
            await asyncio.sleep(1)

@app.on_event("startup")
async def startup():
    logging.info("Proxy starting heartbeat...")
    asyncio.create_task(heartbeat_checker())

@app.get("/{key}")
async def get_key(key: str):
    targets = ring.get_nodes(key, count=2)
    if not targets:
        raise HTTPException(status_code=503, detail="No nodes active")

    async with httpx.AsyncClient() as client:
        # Fetch from Primary and Successor in parallel
        tasks = [client.get(f"{node}/{key}", timeout=0.5) for node in targets]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
    
    valid_results = []
    for i, res in enumerate(responses):
        if isinstance(res, httpx.Response) and res.status_code == 200:
            data = res.json()
            valid_results.append(data)
            logging.info(f"Fetch {key} from {targets[i]} (TS: {data['timestamp']})")
    
    if not valid_results:
        logging.info(f"GET {key} - Not found on any targets {targets}")
        raise HTTPException(status_code=404, detail="Key not found")

    # Conflict Resolution: Return the newest one
    newest = max(valid_results, key=lambda x: x.get("timestamp", 0))
    logging.info(f"GET {key} - Returning newest (TS: {newest['timestamp']})")
    return newest

@app.post("/{key}")
async def put_key(key: str, payload: dict):
    # Proxy generates the master timestamp
    payload["timestamp"] = time.time()
    target = ring.get_node(key)
    
    if not target:
        raise HTTPException(status_code=503, detail="No nodes active")

    async with httpx.AsyncClient() as client:
        try:
            logging.info(f"PUT {key} -> Routing to {target}")
            res = await client.post(f"{target}/{key}", json=payload, timeout=0.5)
            return res.json()
        except Exception as e:
            logging.error(f"PUT {key} failed at {target}: {str(e)}")
            raise HTTPException(status_code=503, detail="Primary node write failed")