-- UP
ALTER TABLE usage_events ADD COLUMN manifest_id TEXT NOT NULL DEFAULT '';
-- Feed order for the cost read API: Kafka offsets are per partition, so the
-- accounting service assigns its own dense sequence at ingestion time.
ALTER TABLE usage_events ADD COLUMN ingest_sequence INTEGER NOT NULL DEFAULT 0;
UPDATE usage_events SET ingest_sequence=rowid;
CREATE UNIQUE INDEX usage_ingest_sequence ON usage_events(ingest_sequence);
CREATE INDEX usage_manifest ON usage_events(manifest_id) WHERE manifest_id <> '';
-- DOWN
DROP INDEX usage_manifest;
DROP INDEX usage_ingest_sequence;
ALTER TABLE usage_events DROP COLUMN ingest_sequence;
ALTER TABLE usage_events DROP COLUMN manifest_id;
