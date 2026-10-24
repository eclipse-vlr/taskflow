BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '10s';
DROP INDEX tasks_running_lease_idx;
ALTER TABLE tasks DROP CONSTRAINT tasks_execution_lease_check;
ALTER TABLE tasks DROP COLUMN lease_expires_at;
ALTER TABLE tasks DROP COLUMN execution_id;
COMMIT;
