-- Covers configuration-update checks; renew/complete/owns already use the composite primary key.
CREATE INDEX keyspaces_active_provisioning ON keyspaces(cluster_id, state, owner_id, expires_at);
