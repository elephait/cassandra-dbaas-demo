CREATE TABLE control_lock (
    id INTEGER PRIMARY KEY
);

INSERT INTO control_lock (id) VALUES (1);

CREATE TABLE tenants (
    tenant_id VARCHAR(24) PRIMARY KEY,
    keyspace_name VARCHAR(255) NOT NULL UNIQUE,
    state VARCHAR(32) NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE operations (
    operation_id VARCHAR(36) PRIMARY KEY,
    keyspace_name VARCHAR(255) NOT NULL,
    state VARCHAR(32) NOT NULL,
    detail VARCHAR(4000),
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE repair_lock (
    id INTEGER PRIMARY KEY,
    owner_id VARCHAR(36),
    expires_at TIMESTAMP
);

INSERT INTO repair_lock (id, owner_id, expires_at) VALUES (1, NULL, NULL);