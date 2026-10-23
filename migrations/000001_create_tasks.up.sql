BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '10s';

CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    type TEXT NOT NULL CHECK (type <> ''),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
    next_run_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    result JSONB,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (attempts BETWEEN 0 AND max_attempts)
);

CREATE INDEX tasks_status_next_run_at_idx ON tasks (status, next_run_at);
CREATE INDEX tasks_created_at_id_idx ON tasks (created_at DESC, id DESC);
COMMIT;
