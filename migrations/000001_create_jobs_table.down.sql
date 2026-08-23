-- Reverses 000001_create_jobs_table.up.sql.
--
-- Every migration in this project has a real down migration. An un-reversible
-- migration is a deployment you cannot roll back.

DROP TRIGGER IF EXISTS jobs_set_updated_at ON jobs;
DROP TABLE IF EXISTS jobs;

-- set_updated_at is created by this migration, so this migration owns dropping
-- it. Later migrations that need it must not assume it already exists.
DROP FUNCTION IF EXISTS set_updated_at();
