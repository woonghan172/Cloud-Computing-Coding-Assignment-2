from fastapi import FastAPI
from fastapi import HTTPException
import uvicorn
from storage import store, lock
import json
import os
import requests
import threading
import time

from pydantic import BaseModel
from consistent_hash import ConsistentHashRing

import logging

SELF_NODE_IP_HOST = "127.0.0.1"
SELF_NODE_IP = "http://" + SELF_NODE_IP_HOST
SELF_NODE_PORT = "8083"
SELF_NODE_DEFAULT = SELF_NODE_IP + ":" + SELF_NODE_PORT
node_ips = ["http://127.0.0.1", "http://127.0.0.1", "http://127.0.0.1"]
node_ports = ["8081", "8082", "8083"]

logging.basicConfig(
    filename=os.getenv("LOG_FILE", "logger.log"),
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
)

# to ensure the data type
class Item(BaseModel):
    value: str

# clean the string and remove the trailing slash if exists, also remove duplicates while preserving order
def normalize_nodes(raw_nodes):
    nodes = [node.strip().rstrip("/") for node in raw_nodes if isinstance(node, str) and node.strip()]
    deduped = list(dict.fromkeys(nodes))
    if SELF_NODE not in deduped:
        deduped.append(SELF_NODE)
    return deduped

# read the ""NODE_FILE.json" and return the list of nodes. It supports both a JSON list and a JSON object with a "nodes" field.
def load_nodes_from_file(path: str):
    with open(path, "r") as f:
        parsed = json.load(f)

    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict) and isinstance(parsed.get("nodes"), list):
        return parsed["nodes"]

    raise ValueError("nodes file must be a JSON list or an object with a 'nodes' list")


# get the id of SELF_NODE or default http://127.0.0.1:8080
#SELF_NODE = os.getenv("SELF_NODE", SELF_NODE_DEFAULT).rstrip("/")
SELF_NODE = SELF_NODE_DEFAULT
#NODES_ENV = os.getenv("NODES", SELF_NODE)
#NODES = normalize_nodes(NODES_ENV.split(","))
NODES = [f"{ip}:{port}" for ip, port in zip(node_ips, node_ports)]

NODES_FILE = os.getenv("NODES_FILE", "").strip()
NODES_REFRESH_INTERVAL = max(1, int(os.getenv("NODES_REFRESH_INTERVAL", "5")))

# default 50, if bigger, the load will be more balanced but the hash ring will consume more memory and CPU
VIRTUAL_NODES = int(os.getenv("VIRTUAL_NODES", "50"))

ring_lock = threading.Lock()
ring = ConsistentHashRing(nodes=NODES, virtual_nodes=VIRTUAL_NODES)
current_nodes = list(NODES)

DATA_FILE = os.getenv("DATA_FILE", f"data_{SELF_NODE_PORT}.json")

# to define if we need to update the has ring
def rebuild_ring_if_changed(new_nodes):
    global ring, current_nodes

    normalized = normalize_nodes(new_nodes)
    # avoid unnecessary rebuild if the nodes are the same as current
    if normalized == current_nodes:
        return False

    new_ring = ConsistentHashRing(nodes=normalized, virtual_nodes=VIRTUAL_NODES)
    with ring_lock:
        ring = new_ring
        current_nodes = normalized

    logging.info("Hash ring updated: nodes=%s", ",".join(current_nodes))
    return True


# Refresh the node list 
def refresh_nodes_once():
    if not NODES_FILE:
        return False

    if not os.path.exists(NODES_FILE):
        logging.warning("NODES_FILE not found: %s", NODES_FILE)
        return False

    try:
        file_nodes = load_nodes_from_file(NODES_FILE)
        return rebuild_ring_if_changed(file_nodes)
    except Exception as exc:
        logging.error("Failed to refresh nodes from %s: %s", NODES_FILE, exc)
        return False

# use polling to see if the nodes fild has changed
def nodes_watcher_loop():
    logging.info("Node watcher started: file=%s interval=%ss", NODES_FILE, NODES_REFRESH_INTERVAL)
    while True:
        refresh_nodes_once()
        time.sleep(NODES_REFRESH_INTERVAL)


def save_to_disk():
    with open(DATA_FILE, "w") as f:
        json.dump(store, f)


def key_owner(key: str) -> str:
    with ring_lock:
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

# if dynamic node discovery is enabled, start the watcher thread to monitor the nodes file and refresh the hash ring accordingly
# @app.on_event("startup")
# def startup_hook():
    # refresh_nodes_once()
    # if NODES_FILE:
    #     watcher = threading.Thread(target=nodes_watcher_loop, daemon=True)
    #     watcher.start()
        
# for check the cluster nodes and the hash ring status
@app.get("/cluster/nodes")
def get_cluster_nodes():
    return {
        "self": SELF_NODE,
        "nodes": current_nodes,
        "nodes_file": NODES_FILE if NODES_FILE else None,
        "refresh_interval_seconds": NODES_REFRESH_INTERVAL,
    }

# for the requirements of Persistence
# Load existing data if file exists and contains valid JSON
if os.path.exists(DATA_FILE) and os.path.getsize(DATA_FILE) > 0:
    with open(DATA_FILE, "r") as f:
        try:
            store.update(json.load(f))
        except json.JSONDecodeError:
            logging.warning("data.json is invalid JSON; starting with empty store")
            

logging.info(
    "Node started: self=%s nodes=%s virtual_nodes=%s data_file=%s nodes_file=%s refresh_interval=%ss",
    SELF_NODE,
    ",".join(current_nodes),
    VIRTUAL_NODES,
    DATA_FILE,
    NODES_FILE if NODES_FILE else "disabled",
    NODES_REFRESH_INTERVAL,
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
    # host = os.getenv("HOST", "127.0.0.1")
    host = SELF_NODE_IP_HOST
    # port = int(os.getenv("PORT", "8080"))
    port = SELF_NODE_PORT
    uvicorn.run(
        "main_no_docker_3:app",
        host=host,
        port=int(port),
        reload=False,
    )
