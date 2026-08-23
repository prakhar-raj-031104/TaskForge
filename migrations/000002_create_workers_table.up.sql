-- Worker inventory.
--
-- The jobs table already records WHICH worker holds a job. This table answers
-- the questions that one cannot: how many workers are alive right now, when did
-- each last check in, and how much capacity does the fleet actually have.
--
-- It is deliberately NOT part of the claim path. Nothing here is consulted when
-- a job is claimed, so a worker whose heartbeat is stale keeps working
-- perfectly. This table is observability, not coordination - which is why a
-- failure to write a heartbeat is logged and ignored rather than fatal.

CREATE TABLE workers (
    -- hostname-pid, or WORKER_ID when set. Text rather than UUID so the value
    -- is meaningful to a human reading it in an incident.
    id TEXT PRIMARY KEY
        CONSTRAINT workers_id_length CHECK (char_length(id) BETWEEN 1 AND 255),

    hostname TEXT NOT NULL,
    pid      INT  NOT NULL,

    status TEXT NOT NULL DEFAULT 'active'
        CONSTRAINT workers_status_valid CHECK (status IN ('active', 'draining', 'stopped')),

    -- Execution slots this process runs, so fleet capacity is
    -- sum(concurrency) over live workers rather than a guess.
    concurrency INT NOT NULL DEFAULT 1
        CONSTRAINT workers_concurrency_positive CHECK (concurrency >= 1),

    -- Jobs currently executing. A snapshot, refreshed on each heartbeat.
    active_jobs INT NOT NULL DEFAULT 0
        CONSTRAINT workers_active_jobs_non_negative CHECK (active_jobs >= 0),

    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TRIGGER workers_set_updated_at
    BEFORE UPDATE ON workers
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();

-- Finding stale workers is the only query that needs an index here: the table
-- holds one row per process, so it is tiny, and every other access is by
-- primary key.
--
-- Partial on status: a stopped worker is already known to be gone, and
-- excluding those rows keeps the index to just the fleet that claims to be
-- alive.
CREATE INDEX workers_liveness_idx
    ON workers (last_heartbeat)
    WHERE status <> 'stopped';

COMMENT ON TABLE  workers                IS 'Worker fleet inventory. Observability only; never consulted when claiming jobs.';
COMMENT ON COLUMN workers.last_heartbeat IS 'Updated periodically by each worker. A stale value means the process died without deregistering.';
COMMENT ON COLUMN workers.status         IS 'active = claiming work, draining = shutting down, stopped = exited cleanly.';
