-- 0001_init.sql — the lean two-table schema from the phase-2 design contract.
-- Exactly environments and workflow_steps: no events table, no attempts table
-- (attempts live on the step row), no resources table.

-- NOTE: no transaction control in migration files — applyOne wraps each file
-- in a transaction together with its schema_migrations ledger row.
CREATE TABLE IF NOT EXISTS environments (
    id               uuid PRIMARY KEY,
    tenant_id        text        NOT NULL,
    name             text        NOT NULL,
    state            text        NOT NULL,
    idempotency_key  text        NOT NULL,
    spec             jsonb       NOT NULL DEFAULT '{}',
    metadata         jsonb       NOT NULL DEFAULT '{}',
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    expires_at       timestamptz NULL,
    UNIQUE (tenant_id, idempotency_key)
);

-- An invalid state cannot be written even by hand.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'environments_state_check'
    ) THEN
        ALTER TABLE environments ADD CONSTRAINT environments_state_check
            CHECK (state IN (
                'pending', 'provisioning', 'verifying', 'ready', 'rotating',
                'rolling_back', 'rolled_back', 'cancelling', 'cancelled',
                'deleting', 'deleted', 'failed', 'in_doubt'
            ));
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS workflow_steps (
    id                uuid PRIMARY KEY,
    environment_id    uuid        NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    step_index        int         NOT NULL,
    kind              text        NOT NULL,
    state             text        NOT NULL,
    attempts          int         NOT NULL DEFAULT 0,
    lease_owner       text,
    lease_expires_at  timestamptz,
    operation_key     text,
    metadata          jsonb       NOT NULL DEFAULT '{}',
    last_error        text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (environment_id, step_index)
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'workflow_steps_state_check'
    ) THEN
        ALTER TABLE workflow_steps ADD CONSTRAINT workflow_steps_state_check
            CHECK (state IN ('pending', 'running', 'in_doubt', 'done', 'failed', 'compensated'));
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx_workflow_steps_state_lease
    ON workflow_steps (state, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_workflow_steps_environment
    ON workflow_steps (environment_id);
