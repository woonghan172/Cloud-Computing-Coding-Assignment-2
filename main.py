import os, json, logging, time, sys
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from storage import store, lock

# Ensure logs show up immediately in Docker
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

DATA_FILE = "data.json"

class Item(BaseModel):
    value: str
    timestamp: float

def save_to_disk():
    with open(DATA_FILE, "w") as f:
        json.dump(store, f)

app = FastAPI()

# Initial Load from Disk
if os.path.exists(DATA_FILE) and os.path.getsize(DATA_FILE) > 0:
    with open(DATA_FILE, "r") as f:
        try:
            store.update(json.load(f))
            logging.info(f"Disk data loaded. Current keys: {list(store.keys())}")
        except:
            logging.error("Failed to parse data.json")

@app.get("/{key}")
def get_value(key: str):
    with lock:
        if key not in store:
            logging.info(f"GET {key} - 404 NOT FOUND")
            raise HTTPException(status_code=404, detail="Key not found")
        logging.info(f"GET {key} - SUCCESS (TS: {store[key]['timestamp']})")
        return store[key]

@app.post("/{key}")
def put_value(key: str, item: Item):
    with lock:
        existing = store.get(key)
        # LWW logic: Only update if no existing data OR incoming TS is newer
        if not existing or item.timestamp > existing.get("timestamp", 0):
            store[key] = {"value": item.value, "timestamp": item.timestamp}
            save_to_disk()
            logging.info(f"PUT {key}, value: {item.value} - UPDATED (New TS: {item.timestamp})")
            return {"status": "OK"}
        
        logging.warning(f"PUT {key}, value: {item.value} - IGNORED (Stale TS: {item.timestamp} < Current: {existing['timestamp']})")
        return {"status": "Ignored", "reason": "Stale data"}

@app.delete("/{key}")
def delete_value(key: str):
     # prevent the race condition
    with lock:
        if key not in store:
            logging.info(f"DELETE {key} - NOT FOUND")
            raise HTTPException(status_code=404, detail="Key not found")
        del store[key]
        save_to_disk()
        
    logging.info(f"DELETE {key} - SUCCESS")
    return {"status": "Deleted"}

@app.get("/")
def health_check():
    return {"status": "healthy"}