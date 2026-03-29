from fastapi import FastAPI
from fastapi import HTTPException
import uvicorn
from storage import store, lock
import json
import os
import requests

from pydantic import BaseModel
from consistent_hash import ConsistentHashRing

import logging


logging.basicConfig(
    filename=os.getenv("LOG_FILE", "logger.log"),
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
)

# to ensure the data type
class Item(BaseModel):
    value: str

# get the id of SELF_NODE or default http://127.0.0.1:8080
SELF_NODE = os.getenv("SELF_NODE", "http://127.0.0.1:8080").rstrip("/")
NODES_ENV = os.getenv("NODES", SELF_NODE)
NODES = [node.strip().rstrip("/") for node in NODES_ENV.split(",") if node.strip()]
# ensure the current node is in the list of nodes, if not add it
if SELF_NODE not in NODES:
    NODES.append(SELF_NODE)

# default 50, if bigger, the load will be more balanced but the hash ring will consume more memory and CPU
VIRTUAL_NODES = int(os.getenv("VIRTUAL_NODES", "50"))
ring = ConsistentHashRing(nodes=NODES, virtual_nodes=VIRTUAL_NODES)

DATA_FILE = os.getenv("DATA_FILE", "data.json")


def save_to_disk():
    with open(DATA_FILE, "w") as f:
        json.dump(store, f)


def key_owner(key: str) -> str:
    return ring.get_node(key)

# Forward the request to the owner node if the current node is not the owner
def forward_to_owner(method: str, key: str, item: Item = None):
    owner = key_owner(key)
    
    # If the owner is the current node, return None to indicate local handling
    if owner == SELF_NODE:
        return None

    # Forward the request to the owner node
    target_url = f"{owner}/{key}"
    try:
        if method == "GET":
            response = requests.get(target_url, timeout=3)
        elif method == "POST":
            response = requests.post(target_url, json={"value": item.value}, timeout=3)
        elif method == "DELETE":
            response = requests.delete(target_url, timeout=3)
        else:
            raise ValueError("Unsupported method")
    # Log the forwarding action if there is an exception (e.g., owner node is down)
    except requests.RequestException as exc:
        logging.error(f"FORWARD {method} {key} -> {owner} failed: {exc}")
        raise HTTPException(status_code=503, detail=f"Owner node unavailable: {owner}")

    # Log the forwarding action if successful
    if response.status_code >= 400:
        detail = response.text
        try:
            detail = response.json().get("detail", response.text)
        except ValueError:
            pass
        raise HTTPException(status_code=response.status_code, detail=detail)

    try:
        return response.json()
    except ValueError:
        return {"status": "OK"}

app = FastAPI()

# for the requirements of Persistence
# Load existing data if file exists and contains valid JSON
if os.path.exists(DATA_FILE) and os.path.getsize(DATA_FILE) > 0:
    with open(DATA_FILE, "r") as f:
        try:
            store.update(json.load(f))
        except json.JSONDecodeError:
            logging.warning("data.json is invalid JSON; starting with empty store")

logging.info(
    "Node started: self=%s nodes=%s virtual_nodes=%s data_file=%s",
    SELF_NODE,
    ",".join(NODES),
    VIRTUAL_NODES,
    DATA_FILE,
)

@app.get("/")
def root():
    return {"message": "KV Store Running"}

# need to be above the @app.get("/{key}")
@app.get("/all")
def get_all():
    with lock:
        logging.info("GET ALL - SUCCESS")
        return store

@app.get("/{key}")
def get_value(key: str):
    forwarded = forward_to_owner("GET", key)
    # If forwarded is not None, 
    # it means the request has been forwarded to the owner 
    # and we should return the response from the owner
    if forwarded is not None:
        return forwarded

    # prevent the race condition
    with lock:
        if key not in store:
            logging.info(f"GET {key} - NOT FOUND")
            raise HTTPException(status_code=404, detail="Key not found")
        
        logging.info(f"GET {key} - SUCCESS")
        return {"value": store[key]}

@app.post("/{key}")
def put_value(key: str, item: Item):
    forwarded = forward_to_owner("POST", key, item)
    if forwarded is not None:
        return forwarded

    # prevent the race condition by locking the store during the update
    with lock:
        store[key] = item.value
        save_to_disk()
        
    logging.info(f"PUT key: {key}, value: {item.value}")
    return {"status": "OK"}

@app.delete("/{key}")
def delete_value(key: str):
    forwarded = forward_to_owner("DELETE", key)
    if forwarded is not None:
        return forwarded

    # prevent the race condition
    with lock:
        if key not in store:
            logging.info(f"DELETE {key} - NOT FOUND")
            raise HTTPException(status_code=404, detail="Key not found")
        del store[key]
        save_to_disk()
        
    logging.info(f"DELETE {key} - SUCCESS")
    return {"status": "Deleted"}


if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=False,
    )
