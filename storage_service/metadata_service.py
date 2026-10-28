
from datetime import datetime, timezone
from threading import Lock

from fastapi import FastAPI
from pydantic import BaseModel, Field

import metadata_store
from placement_api import router as placement_router

app = FastAPI(title="Distributed Storage Metadata Service")
app.include_router(placement_router)

nodes = {}
nodes_lock = Lock()


class Heartbeat(BaseModel):
    node_id: str
    free_space_bytes: int = Field(ge=0)
    chunk_count: int = Field(ge=0)


@app.on_event("startup")
def init_db():
    metadata_store.init_schema()


@app.get("/health")
def health():
    return {"status": "healthy", "service": "metadata"}


@app.post("/heartbeat")
def receive_heartbeat(heartbeat: Heartbeat):
    received_at = datetime.now(timezone.utc).isoformat()

    with nodes_lock:
        nodes[heartbeat.node_id] = {
            "node_id": heartbeat.node_id,
            "free_space_bytes": heartbeat.free_space_bytes,
            "chunk_count": heartbeat.chunk_count,
            "last_heartbeat": received_at,
        }

    return {"status": "received", "node_id": heartbeat.node_id}


@app.get("/nodes")
def list_nodes():
    with nodes_lock:
        return {"nodes": list(nodes.values())}
