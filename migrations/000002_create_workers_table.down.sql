-- Reverses 000002_create_workers_table.up.sql.
--
-- The set_updated_at function is owned by migration 000001, so it is
-- deliberately NOT dropped here: doing so would break the jobs trigger.

DROP TRIGGER IF EXISTS workers_set_updated_at ON workers;
DROP TABLE IF EXISTS workers;
