-- Run ownership is durable; these leases cover only one submit/poll action.
ALTER TABLE operations ADD COLUMN repair_transport VARCHAR(16) DEFAULT 'cli' NOT NULL;
CREATE TABLE repair_node_progress (
    operation_id VARCHAR(36) NOT NULL REFERENCES operations(operation_id),
    ordinal INTEGER NOT NULL,
    node_name VARCHAR(255) NOT NULL,
    service_url VARCHAR(2000) NOT NULL,
    expected_host_id VARCHAR(36) NOT NULL,
    state VARCHAR(32) NOT NULL,
    command_id INTEGER,
    process_started_at BIGINT,
    attempts INTEGER DEFAULT 0 NOT NULL,
    action_owner VARCHAR(36),
    action_expires_at TIMESTAMP,
    next_action_at TIMESTAMP NOT NULL,
    detail VARCHAR(3900),
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (operation_id, ordinal),
    UNIQUE (operation_id, node_name)
);
CREATE INDEX repair_node_due ON repair_node_progress(state, next_action_at);
