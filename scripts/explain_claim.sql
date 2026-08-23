-- Prove that jobs_claim_idx earns its place.
--
-- Run scripts/seed_jobs.sql first, then:
--   Get-Content scripts/explain_claim.sql | docker compose exec -T postgres psql -U taskforge -d taskforge
--
-- FOR UPDATE takes real row locks, so each experiment runs inside a
-- transaction that is rolled back. EXPLAIN ANALYZE genuinely executes the
-- query - that is the difference between it and plain EXPLAIN, which only
-- shows the planner's guess.

\echo '=============================================================='
\echo 'A. The claim query WITH the partial index'
\echo '=============================================================='
BEGIN;
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, type, payload, priority, attempts, max_retries
  FROM jobs
 WHERE status = 'pending'
   AND available_at <= now()
 ORDER BY priority DESC, available_at ASC, id ASC
   FOR UPDATE SKIP LOCKED
 LIMIT 10;
ROLLBACK;

\echo ''
\echo '=============================================================='
\echo 'B. The same query with index scans disabled (forced sequential)'
\echo '=============================================================='
BEGIN;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_bitmapscan = off;
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, type, payload, priority, attempts, max_retries
  FROM jobs
 WHERE status = 'pending'
   AND available_at <= now()
 ORDER BY priority DESC, available_at ASC, id ASC
   FOR UPDATE SKIP LOCKED
 LIMIT 10;
ROLLBACK;

\echo ''
\echo '=============================================================='
\echo 'C. Index sizes: partial vs the primary key over the same table'
\echo '=============================================================='
SELECT indexrelname AS index,
       pg_size_pretty(pg_relation_size(indexrelid)) AS size,
       idx_scan AS scans
  FROM pg_stat_user_indexes
 WHERE relname = 'jobs'
 ORDER BY pg_relation_size(indexrelid) DESC;
