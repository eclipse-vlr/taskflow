BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '10s';

ALTER TABLE tasks ADD COLUMN execution_id UUID;
ALTER TABLE tasks ADD COLUMN lease_expires_at TIMESTAMPTZ;
-- Old application versions must be stopped before upgrading the schema.
UPDATE tasks SET execution_id = gen_random_uuid(), lease_expires_at = clock_timestamp()
WHERE status = 'running';
ALTER TABLE tasks ADD CONSTRAINT tasks_execution_lease_check CHECK (
    (status = 'running' AND execution_id IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR (status <> 'running' AND execution_id IS NULL AND lease_expires_at IS NULL)
);
CREATE INDEX tasks_running_lease_idx ON tasks (lease_expires_at) WHERE status = 'running';
COMMIT;
