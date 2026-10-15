CREATE TABLE clusters (
    cluster_id VARCHAR(64) PRIMARY KEY,
    datacenter VARCHAR(128) NOT NULL,
    expected_nodes INTEGER NOT NULL,
    replication_factor INTEGER NOT NULL,
    request_timeout_seconds INTEGER NOT NULL,
    reconnect_seconds INTEGER NOT NULL,
    max_keyspaces INTEGER NOT NULL,
    provisioning_lease_seconds INTEGER NOT NULL,
    enabled BOOLEAN NOT NULL,
    config_version BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE cluster_contact_points (
    cluster_id VARCHAR(64) NOT NULL REFERENCES clusters(cluster_id),
    ordinal INTEGER NOT NULL,
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    PRIMARY KEY (cluster_id, ordinal)
);

CREATE TABLE cluster_address_mappings (
    cluster_id VARCHAR(64) NOT NULL REFERENCES clusters(cluster_id),
    discovered_host VARCHAR(255) NOT NULL,
    target_host VARCHAR(255) NOT NULL,
    target_port INTEGER NOT NULL,
    PRIMARY KEY (cluster_id, discovered_host)
);

CREATE TABLE repair_policies (
    cluster_id VARCHAR(64) PRIMARY KEY REFERENCES clusters(cluster_id),
    command VARCHAR(4000) NOT NULL,
    lease_minutes INTEGER NOT NULL,
    policy_version BIGINT NOT NULL
);

CREATE TABLE repair_policy_nodes (
    cluster_id VARCHAR(64) NOT NULL REFERENCES repair_policies(cluster_id),
    ordinal INTEGER NOT NULL,
    node_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (cluster_id, ordinal)
);

CREATE TABLE keyspaces (
    cluster_id VARCHAR(64) NOT NULL REFERENCES clusters(cluster_id),
    keyspace_name VARCHAR(48) NOT NULL,
    state VARCHAR(32) NOT NULL,
    owner_id VARCHAR(36),
    expires_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (cluster_id, keyspace_name)
);

-- Covers configuration-update checks; renew/complete/owns already use the composite primary key.
CREATE INDEX keyspaces_active_provisioning ON keyspaces(cluster_id, state, owner_id, expires_at);

CREATE TABLE operations (
    operation_id VARCHAR(36) PRIMARY KEY,
    cluster_id VARCHAR(64) NOT NULL REFERENCES clusters(cluster_id),
    keyspace_name VARCHAR(48) NOT NULL,
    owner_id VARCHAR(36),
    state VARCHAR(32) NOT NULL,
    detail VARCHAR(4000),
    updated_at TIMESTAMP NOT NULL,
    request_key VARCHAR(128) NOT NULL,
    repair_policy_version BIGINT NOT NULL,
    next_attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    retry_count INTEGER DEFAULT 0 NOT NULL,
    UNIQUE (cluster_id, request_key)
);

CREATE INDEX operations_queue ON operations(state, updated_at, operation_id);
CREATE INDEX operations_cluster_state ON operations(cluster_id, state, updated_at);
CREATE INDEX operations_due_queue ON operations(state, next_attempt_at, cluster_id, updated_at, operation_id);

CREATE TABLE repair_locks (
    cluster_id VARCHAR(64) PRIMARY KEY REFERENCES clusters(cluster_id),
    owner_id VARCHAR(36),
    expires_at TIMESTAMP
);

-- Bootstrap configuration is replaced once from application configuration at startup.
INSERT INTO clusters VALUES ('default', 'dc1', 1, 1, 10, 60, 5, 60, TRUE, 0, CURRENT_TIMESTAMP);
INSERT INTO cluster_contact_points VALUES ('default', 0, '127.0.0.1', 9042);
INSERT INTO repair_policies VALUES ('default', 'nodetool repair -pr {keyspace}', 60, 1);
INSERT INTO repair_policy_nodes VALUES ('default', 0, 'cassandra1');
INSERT INTO repair_locks VALUES ('default', NULL, NULL);
