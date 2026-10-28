
import hashlib
import os
import re
import shutil
import threading
import time
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response

app = FastAPI(title="Distributed Storage Node")

NODE_ID = os.getenv("NODE_ID", "node1")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data/storage/node1"))
METADATA_URL = os.getenv(
    "METADATA_URL",
    "http://127.0.0.1:8000/heartbeat",
)
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "5"))

DATA_DIR.mkdir(parents=True, exist_ok=True)


def valid_hash(chunk_hash: str) -> bool:
    """Accept only lowercase SHA-256 hexadecimal hashes."""
    return re.fullmatch(r"[0-9a-f]{64}", chunk_hash) is not None


def chunk_path(chunk_hash: str) -> Path:
    return DATA_DIR / f"{chunk_hash}.chunk"


def read_verified_chunk(chunk_hash: str) -> bytes:
    """Read a chunk and reject it if its content hash is incorrect."""
    path = chunk_path(chunk_hash)

    if not path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")

    try:
        content = path.read_bytes()
    except OSError:
        raise HTTPException(status_code=500, detail="Could not read chunk")

    actual_hash = hashlib.sha256(content).hexdigest()

    if actual_hash != chunk_hash:
        raise HTTPException(
            status_code=500,
            detail="Chunk integrity verification failed",
        )

    return content


def send_heartbeat():
    """Report this node's current storage status to metadata."""
    try:
        free_space = shutil.disk_usage(DATA_DIR).free
        chunk_count = sum(
            1 for path in DATA_DIR.glob("*.chunk") if path.is_file()
        )

        payload = {
            "node_id": NODE_ID,
            "free_space_bytes": free_space,
            "chunk_count": chunk_count,
        }

        response = requests.post(
            METADATA_URL,
            json=payload,
            timeout=3,
        )
        response.raise_for_status()

    except (requests.RequestException, OSError) as exc:
        print(f"[{NODE_ID}] Heartbeat failed: {exc}", flush=True)


def heartbeat_loop():
    while True:
        send_heartbeat()
        time.sleep(HEARTBEAT_INTERVAL)


@app.on_event("startup")
def start_heartbeat():
    thread = threading.Thread(
        target=heartbeat_loop,
        name=f"{NODE_ID}-heartbeat",
        daemon=True,
    )
    thread.start()
    print(f"[{NODE_ID}] Storage directory: {DATA_DIR}", flush=True)


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "node_id": NODE_ID,
        "storage_directory": str(DATA_DIR),
    }


@app.put("/chunks/{chunk_hash}")
async def put_chunk(chunk_hash: str, request: Request):
    """Store content only when its SHA-256 matches the requested address."""
    if not valid_hash(chunk_hash):
        raise HTTPException(status_code=400, detail="Invalid SHA-256 hash")

    content = await request.body()
    actual_hash = hashlib.sha256(content).hexdigest()

    if actual_hash != chunk_hash:
        raise HTTPException(
            status_code=400,
            detail="Provided hash does not match chunk content",
        )

    destination = chunk_path(chunk_hash)
    temporary = DATA_DIR / f".{chunk_hash}.tmp"

    try:
        temporary.write_bytes(content)
        os.replace(temporary, destination)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail="Could not store chunk")

    return {
        "status": "stored",
        "node_id": NODE_ID,
        "sha256": chunk_hash,
        "size_bytes": len(content),
    }


@app.get("/chunks/{chunk_hash}")
def get_chunk(chunk_hash: str):
    """Return a chunk only after verifying its content hash."""
    if not valid_hash(chunk_hash):
        raise HTTPException(status_code=400, detail="Invalid SHA-256 hash")

    content = read_verified_chunk(chunk_hash)

    return Response(
        content=content,
        media_type="application/octet-stream",
        headers={"X-Chunk-SHA256": chunk_hash},
    )


@app.head("/chunks/{chunk_hash}")
def head_chunk(chunk_hash: str):
    """Check chunk existence and integrity without returning its content."""
    if not valid_hash(chunk_hash):
        raise HTTPException(status_code=400, detail="Invalid SHA-256 hash")

    content = read_verified_chunk(chunk_hash)

    return Response(
        status_code=200,
        headers={
            "Content-Length": str(len(content)),
            "X-Chunk-SHA256": chunk_hash,
        },
    )


@app.delete("/chunks/{chunk_hash}")
def delete_chunk(chunk_hash: str):
    """Delete a stored chunk by its content address."""
    if not valid_hash(chunk_hash):
        raise HTTPException(status_code=400, detail="Invalid SHA-256 hash")

    path = chunk_path(chunk_hash)

    if not path.is_file():
        raise HTTPException(status_code=404, detail="Chunk not found")

    try:
        path.unlink()
    except OSError:
        raise HTTPException(status_code=500, detail="Could not delete chunk")

    return {"status": "deleted", "node_id": NODE_ID, "sha256": chunk_hash}
