-- Type metadata catalog: field-level annotations layered over protobuf
-- message types (see type_metadata.proto for the scope model).

-- One row per (scope, graph, type): the writable overlay layers.
-- scope GLOBAL_DRAFT uses graph_id = '' (a NULL would break the unique key).
CREATE TABLE type_metadata_overlays (
    scope VARCHAR(32) NOT NULL,
    graph_id VARCHAR(255) NOT NULL DEFAULT '',
    message_full_name VARCHAR(512) NOT NULL,
    -- JSON envelope (SaveTypeMetadataRequest fields map) via protobuf JsonFormat.
    fields JSONB NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (scope, graph_id, message_full_name)
);

CREATE INDEX idx_type_metadata_overlays_type ON type_metadata_overlays (message_full_name);

-- Phase-2 git-bake queue: one row per promotion, consumed by the (future)
-- automation that turns promoted annotations into a protos PR.
CREATE TABLE metadata_pending_bakes (
    id VARCHAR(36) PRIMARY KEY,
    message_full_name VARCHAR(512) NOT NULL,
    apicurio_version VARCHAR(64) NOT NULL,
    fields JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
