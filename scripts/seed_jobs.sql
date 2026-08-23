-- Seed a realistically shaped queue for index experiments.
--
-- "Realistically shaped" is the important part: a queue in production is
-- overwhelmingly historical. Hundreds of thousands of completed rows, a few
-- hundred pending ones. Benchmarking against a table where every row is
-- pending tells you nothing about the system you will actually operate.
--
-- Usage:
--   docker compose exec -T postgres psql -U taskforge -d taskforge -f /scripts/seed_jobs.sql
-- or from the host:
--   Get-Content scripts/seed_jobs.sql | docker compose exec -T postgres psql -U taskforge -d taskforge

TRUNCATE jobs;

-- 200,000 completed jobs: the historical bulk.
INSERT INTO jobs (id, type, payload, status, priority, attempts,
                  available_at, started_at, completed_at, created_at)
SELECT
    gen_random_uuid(),
    (ARRAY['send_email', 'webhook', 'sleep'])[1 + (i % 3)],
    jsonb_build_object('seq', i),
    'completed',
    1 + (i % 100),
    1,
    now() - make_interval(secs => i),
    now() - make_interval(secs => i),
    now() - make_interval(secs => i - 1),
    now() - make_interval(secs => i)
FROM generate_series(1, 200000) AS i;

-- 500 pending jobs available now, with spread-out priorities.
INSERT INTO jobs (id, type, payload, status, priority, available_at, created_at)
SELECT
    gen_random_uuid(),
    'send_email',
    jsonb_build_object('seq', i),
    'pending',
    1 + (i % 100),
    now() - make_interval(secs => i),
    now() - make_interval(secs => i)
FROM generate_series(1, 500) AS i;

-- 100 pending jobs scheduled for the future: they must never be claimed.
INSERT INTO jobs (id, type, payload, status, priority, scheduled_at, available_at, created_at)
SELECT
    gen_random_uuid(),
    'send_email',
    jsonb_build_object('seq', i),
    'pending',
    100,
    now() + make_interval(hours => 1),
    now() + make_interval(hours => 1),
    now()
FROM generate_series(1, 100) AS i;

-- The planner chooses based on statistics, not on row contents. Without a
-- fresh ANALYZE it may still believe this table is empty and pick a plan that
-- makes no sense.
ANALYZE jobs;

SELECT status, count(*) FROM jobs GROUP BY status ORDER BY count(*) DESC;
