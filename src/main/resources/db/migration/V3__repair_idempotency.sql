ALTER TABLE operations ADD COLUMN request_key VARCHAR(128);
ALTER TABLE operations ADD COLUMN request_fingerprint VARCHAR(255);
CREATE UNIQUE INDEX operations_request_key_unique ON operations(request_key);
