from fastapi import FastAPI
from fastapi import HTTPException
import uvicorn
from storage import store, lock
import json
import os

from pydantic import BaseModel

import logging


logging.basicConfig(
    filename="logger.log",
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
)

# to ensure the data type
class Item(BaseModel):
    value: str
   
DATA_FILE = "data.json" 
def save_to_disk():
    with open(DATA_FILE, "w") as f:
        json.dump(store, f)

app = FastAPI()

# for the requirements of Persistence
# Load existing data if file exists and contains valid JSON
if os.path.exists(DATA_FILE) and os.path.getsize(DATA_FILE) > 0:
    with open(DATA_FILE, "r") as f:
        try:
            store.update(json.load(f))
        except json.JSONDecodeError:
            logging.warning("data.json is invalid JSON; starting with empty store")

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
     # prevent the race condition
    with lock:
        if key not in store:
            logging.info(f"GET {key} - NOT FOUND")
            raise HTTPException(status_code=404, detail="Key not found")
        
        logging.info(f"GET {key} - SUCCESS")
        return {"value": store[key]}

@app.post("/{key}")
def put_value(key: str, item: Item):
    
    # prevent the race condition by locking the store during the update
    with lock:
        store[key] = item.value
        save_to_disk()
        
    logging.info(f"PUT key: {key}, value: {item.value}")
    return {"status": "OK"}

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


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8080,
        reload=False,
    )
