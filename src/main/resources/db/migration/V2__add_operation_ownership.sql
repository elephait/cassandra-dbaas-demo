ALTER TABLE tenants ADD COLUMN IF NOT EXISTS owner_id VARCHAR(36);
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP;
ALTER TABLE operations ADD COLUMN IF NOT EXISTS owner_id VARCHAR(36);

UPDATE operations
SET owner_id = (SELECT owner_id FROM repair_lock WHERE id = 1)
WHERE owner_id IS NULL
  AND state IN ('QUEUED', 'RUNNING')
  AND (SELECT owner_id FROM repair_lock WHERE id = 1) IS NOT NULL;