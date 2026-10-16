-- Control-plane schema. PostgreSQL in shared deployments; H2 for the single-process local demo.
-- Every time column is an instant (WITH TIME ZONE): leases compare correctly whatever the JVM or DB time zone.

-- Registered clusters. config_version is the JPA optimistic version; sessions reconnect when it changes.
CREATE TABLE clusters
(
    cluster_id                 VARCHAR(64) PRIMARY KEY,
    datacenter                 VARCHAR(128) NOT NULL,
    expected_nodes             INTEGER      NOT NULL,
    replication_factor         INTEGER      NOT NULL,
    request_timeout_seconds    INTEGER      NOT NULL,
    reconnect_seconds          INTEGER      NOT NULL,
    max_keyspaces              INTEGER      NOT NULL,
    provisioning_lease_seconds INTEGER      NOT NULL,
    enabled                    BOOLEAN      NOT NULL,
    config_version             BIGINT       NOT NULL,
    updated_at                 TIMESTAMP WITH TIME ZONE    NOT NULL
);

CREATE TABLE cluster_contact_points
(
    cluster_id VARCHAR(64)  NOT NULL REFERENCES clusters (cluster_id),
    ordinal    INTEGER      NOT NULL,
    host       VARCHAR(255) NOT NULL,
    port       INTEGER      NOT NULL,
    PRIMARY KEY (cluster_id, ordinal)
);

CREATE TABLE cluster_address_mappings
(
    cluster_id      VARCHAR(64)  NOT NULL REFERENCES clusters (cluster_id),
    discovered_host VARCHAR(255) NOT NULL,
    target_host     VARCHAR(255) NOT NULL,
    target_port     INTEGER      NOT NULL,
    PRIMARY KEY (cluster_id, discovered_host)
);

-- (cluster_id, keyspace_name) is the provisioning identity; owner/expires_at form the provisioning lease.
CREATE TABLE keyspaces
(
    cluster_id    VARCHAR(64) NOT NULL REFERENCES clusters (cluster_id),
    keyspace_name VARCHAR(48) NOT NULL,
    state         VARCHAR(32) NOT NULL,
    owner_id      VARCHAR(36),
    expires_at    TIMESTAMP WITH TIME ZONE,
    updated_at    TIMESTAMP WITH TIME ZONE   NOT NULL,
    PRIMARY KEY (cluster_id, keyspace_name)
);

CREATE INDEX keyspaces_active_provisioning ON keyspaces (cluster_id, state, expires_at);

-- Repair requests. (cluster_id, request_key) is the idempotency key.
CREATE TABLE operations
(
    operation_id          VARCHAR(36) PRIMARY KEY,
    cluster_id            VARCHAR(64)                         NOT NULL REFERENCES clusters (cluster_id),
    keyspace_name         VARCHAR(48)                         NOT NULL,
    owner_id              VARCHAR(36),
    state                 VARCHAR(32)                         NOT NULL,
    detail                VARCHAR(4000),
    updated_at            TIMESTAMP WITH TIME ZONE                           NOT NULL,
    request_key           VARCHAR(128)                        NOT NULL,
    next_attempt_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    retry_count           INTEGER   DEFAULT 0                 NOT NULL,
    delayed               BOOLEAN   DEFAULT FALSE             NOT NULL,
    UNIQUE (cluster_id, request_key)
);

CREATE INDEX operations_cluster_state ON operations (cluster_id, state, updated_at);
CREATE INDEX operations_due ON operations (state, next_attempt_at);

-- One row per cluster, created at first repair admission: the single repair reservation. A non-NULL expires_at is a short claim lease;
-- NULL with an owner is a durable reservation held by a running or UNKNOWN repair.
CREATE TABLE repair_locks
(
    cluster_id VARCHAR(64) PRIMARY KEY REFERENCES clusters (cluster_id),
    owner_id   VARCHAR(36),
    expires_at TIMESTAMP WITH TIME ZONE
);

-- Persisted node plan of a running repair. action_owner/action_expires_at lease one submit or poll;
-- submitted_at starts the node's own observation clock.
CREATE TABLE repair_node_progress
(
    operation_id       VARCHAR(36)           NOT NULL REFERENCES operations (operation_id),
    ordinal            INTEGER               NOT NULL,
    node_name          VARCHAR(255)          NOT NULL,
    service_url        VARCHAR(2000)         NOT NULL,
    expected_host_id   VARCHAR(36)           NOT NULL,
    state              VARCHAR(32)           NOT NULL,
    command_id         INTEGER,
    process_started_at BIGINT,
    submitted_at       TIMESTAMP WITH TIME ZONE,
    delayed            BOOLEAN DEFAULT FALSE NOT NULL,
    attempts           INTEGER DEFAULT 0     NOT NULL,
    action_owner       VARCHAR(36),
    action_expires_at  TIMESTAMP WITH TIME ZONE,
    next_action_at     TIMESTAMP WITH TIME ZONE             NOT NULL,
    detail             VARCHAR(3900),
    updated_at         TIMESTAMP WITH TIME ZONE             NOT NULL,
    PRIMARY KEY (operation_id, ordinal),
    UNIQUE (operation_id, node_name)
);

CREATE INDEX repair_node_due ON repair_node_progress (state, next_action_at);

-- Placeholder for the bootstrap cluster; startup replaces it from application configuration
-- (config_version 0 marks it as not yet configured).
INSERT INTO clusters
VALUES ('default', 'dc1', 1, 1, 10, 60, 5, 60, TRUE, 0, CURRENT_TIMESTAMP);
INSERT INTO cluster_contact_points
VALUES ('default', 0, '127.0.0.1', 9042);
INSERT INTO repair_locks
VALUES ('default', NULL, NULL);
