
BEGIN;

CREATE TABLE IF NOT EXISTS buckets (
    bucket_name TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS objects (
    object_id BIGSERIAL PRIMARY KEY,
    bucket_name TEXT NOT NULL REFERENCES buckets(bucket_name),
    object_key TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    chunking_method TEXT NOT NULL DEFAULT 'fixed'
        CHECK (chunking_method IN ('fixed', 'fastcdc')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (bucket_name, object_key, version)
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_hash CHAR(64) PRIMARY KEY
        CHECK (chunk_hash ~ '^[0-9a-f]{64}$'),
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    ref_count BIGINT NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS object_chunks (
    object_id BIGINT NOT NULL
        REFERENCES objects(object_id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
    chunk_hash CHAR(64) NOT NULL
        REFERENCES chunks(chunk_hash) ON DELETE RESTRICT,
    chunk_size_bytes BIGINT NOT NULL CHECK (chunk_size_bytes >= 0),
    PRIMARY KEY (object_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS chunk_placements (
    chunk_hash CHAR(64) NOT NULL
        REFERENCES chunks(chunk_hash) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    placement_order INTEGER NOT NULL CHECK (placement_order >= 0),
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chunk_hash, node_id),
    UNIQUE (chunk_hash, placement_order)
);

CREATE INDEX IF NOT EXISTS idx_objects_bucket_key
    ON objects(bucket_name, object_key);

CREATE INDEX IF NOT EXISTS idx_object_chunks_hash
    ON object_chunks(chunk_hash);

CREATE INDEX IF NOT EXISTS idx_chunk_placements_node
    ON chunk_placements(node_id);

COMMIT;
