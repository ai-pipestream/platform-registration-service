-- Module registrations table (PostgreSQL)
CREATE TABLE modules (
    service_id VARCHAR(255) PRIMARY KEY,
    service_name VARCHAR(255) NOT NULL,
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    version VARCHAR(100),
    config_schema_id VARCHAR(255),
    metadata JSONB,
    registered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_heartbeat TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
);

-- Schema versions table with Apicurio sync columns (PostgreSQL)
CREATE TABLE config_schemas (
    schema_id VARCHAR(255) PRIMARY KEY,
    service_name VARCHAR(255) NOT NULL,
    schema_version VARCHAR(100) NOT NULL,
    json_schema JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    -- Apicurio integration
    apicurio_artifact_id VARCHAR(255),
    apicurio_global_id BIGINT,
    sync_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    last_sync_attempt TIMESTAMP,
    sync_error VARCHAR(255),
    CONSTRAINT unique_service_schema_version UNIQUE(service_name, schema_version)
);

-- Indexes for efficient lookups
CREATE INDEX idx_modules_service_name ON modules(service_name);
CREATE INDEX idx_modules_status ON modules(status);
CREATE INDEX idx_schemas_service_name ON config_schemas(service_name);
CREATE INDEX idx_schemas_sync_status ON config_schemas(sync_status);
