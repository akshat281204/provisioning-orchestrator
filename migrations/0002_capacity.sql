-- 0002_capacity.sql — durable capacity reservations for the phase-5 Capacity
-- adapter. One row per environment holding a reservation. The bounded limit
-- is enforced by the adapter (LOCK TABLE + count check), the rows make the
-- reservation survive crashes and releases stay idempotent.
--
-- ON DELETE CASCADE: deleting an environment frees its capacity slot, so a
-- rolled-back provision never permanently consumes capacity.

CREATE TABLE IF NOT EXISTS capacity_reservations (
    environment_id uuid PRIMARY KEY REFERENCES environments(id) ON DELETE CASCADE,
    reserved_at    timestamptz NOT NULL DEFAULT now()
);
