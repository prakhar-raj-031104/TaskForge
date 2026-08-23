# TaskForge

A distributed job queue and task-processing platform built on **Go and PostgreSQL** —
no Redis, no message broker, no ORM, no web framework.

Clients submit jobs over HTTP; jobs are durably stored in PostgreSQL; a fleet of
worker processes claims and executes them concurrently using
`SELECT … FOR UPDATE SKIP LOCKED`, with retries, exponential backoff with
jitter, leases with fencing tokens, dead-lettering, and automatic recovery from
worker crashes.

```
   Client ──HTTP──▶  api  ──▶  PostgreSQL  ◀──  worker × N
                                   │              (goroutine pool,
                              source of truth      lease renewal,
                                   │               heartbeat, reaper)
                          Prometheus ─▶ Grafana
```

**Everything below that reports a number is measured, not estimated.** Commands
to reproduce each measurement are included.

---

## Contents

1. [What works](#what-works)
2. [Quick start](#quick-start)
3. [API](#api)
4. [System design](#system-design)
5. [Database design](#database-design)
6. [Failure recovery](#failure-recovery)
7. [Observability](#observability)
8. [Testing](#testing)
9. [Benchmarks](#benchmarks)
10. [Load testing](#load-testing)
11. [Configuration](#configuration)
12. [Make targets](#make-targets)
13. [Project layout](#project-layout)
14. [Design decisions and tradeoffs](#design-decisions-and-tradeoffs)

---

## What works

- [x] Durable job queue with an explicit, enforced state machine
- [x] Safe concurrent claiming — **200 jobs, 16 concurrent workers, zero duplicate claims** (test)
- [x] HTTP API on the Go standard library: request IDs, panic recovery, per-request deadlines, graceful shutdown
- [x] Idempotency keys enforced by a **partial unique index**, proven under a 16-goroutine race
- [x] Pluggable job handlers behind a one-method interface (`sleep`, `send_email`, `webhook`, `flaky`)
- [x] Automatic retries with exponential backoff and **equal jitter**
- [x] Dead-letter queue with inspection and replay endpoints
- [x] Leases with **fencing tokens** (`lease_epoch`) — a frozen worker cannot clobber the new owner
- [x] Lease renewal, so a 10-minute job runs safely under a 60-second lease
- [x] Crash recovery — SIGKILL a worker mid-job and another reclaims it (demonstrated below)
- [x] Scheduled (delayed) jobs, with `scheduled_at` and `available_at` kept strictly separate
- [x] Priority scheduling, plus optional **aging** to prevent starvation
- [x] Worker heartbeats and a fleet inventory API with derived liveness
- [x] Prometheus metrics, Grafana dashboard, alerting rules
- [x] Unit + integration + concurrency tests, all green under `-race`
- [x] Benchmarks with measured numbers
- [x] Multi-stage distroless Docker images (**27.8 MB**), full Compose stack, GitHub Actions CI

---

## Quick start

### Prerequisites

Docker Desktop. That is all — Go, `psql` and `golang-migrate` are only needed
for local development.

### The whole stack

```bash
git clone https://github.com/anjani-kr-singh-ai/taskforge.git
cd taskforge
make setup                 # create .env from .env.example
docker compose up -d --build --scale worker=3
```

That starts PostgreSQL, applies migrations, and brings up the API, three worker
processes, Prometheus and Grafana.

| Service | URL |
|---|---|
| API | http://localhost:8080 |
| Grafana dashboard | http://localhost:3000 (anonymous viewing enabled) |
| Prometheus | http://localhost:9090 |
| PostgreSQL | `localhost:5433` |

Submit some work and watch the dashboard move:

```bash
for i in $(seq 1 50); do
  curl -s -o /dev/null -X POST http://localhost:8080/api/v1/jobs \
    -H 'Content-Type: application/json' \
    -d '{"type":"sleep","payload":{"seconds":0.5}}'
done

curl -s http://localhost:8080/api/v1/workers | jq .summary
```

```json
{ "alive": 3, "dead": 0, "total_capacity": 15, "active_jobs": 4 }
```

> **Port note.** PostgreSQL is published on host port **5433**, not 5432. A
> locally installed PostgreSQL commonly owns 5432, and the failure mode is
> nasty: the connection succeeds and then fails authentication against the wrong
> server. Inside the Compose network the database is always `postgres:5432`.

### Local development

```bash
make docker-db      # PostgreSQL only
make migrate-up
make test
make run            # API on :8080
make run-worker     # a worker, in another terminal
```

---

## API

Full specification: [docs/openapi.yaml](docs/openapi.yaml). Every response
below is real output.

### Create a job

```bash
curl -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
        "type": "send_email",
        "payload": {"to": "user@example.com", "subject": "Hello"},
        "priority": 90,
        "max_retries": 5,
        "idempotency_key": "order-123-email"
      }'
```

```http
HTTP/1.1 201 Created
Location: /api/v1/jobs/8f0e6764-ae20-4b67-9e53-72a4f4e8384a
X-Request-Id: e04b5037-11b9-4fc5-9eae-042fe67c76f6
```
```json
{
  "id": "8f0e6764-ae20-4b67-9e53-72a4f4e8384a",
  "type": "send_email",
  "payload": {"to": "user@example.com", "subject": "Hello"},
  "status": "pending",
  "priority": 90,
  "attempts": 0,
  "max_retries": 5,
  "available_at": "2026-08-09T10:31:02.203257Z",
  "idempotency_key": "order-123-email",
  "created_at": "2026-08-09T10:31:02.203257Z",
  "updated_at": "2026-08-09T10:31:02.203257Z"
}
```

Send the **same request again** → `200 OK` with the *identical* job id. No
second row is created.

### Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/jobs` | Create a job (201 new, 200 idempotent hit) |
| `GET` | `/api/v1/jobs` | List, filter by `status`, `type`, paginate |
| `GET` | `/api/v1/jobs/{id}` | Fetch one |
| `GET` | `/api/v1/dead-letter-jobs` | Inspect failures |
| `POST` | `/api/v1/dead-letter-jobs/{id}/retry` | Replay with a fresh retry budget |
| `GET` | `/api/v1/workers` | Fleet inventory with derived liveness |
| `GET` | `/api/v1/health` | Liveness — checks nothing external |
| `GET` | `/api/v1/ready` | Readiness — checks the database |
| `GET` | `/metrics` | Prometheus |

`health` deliberately checks nothing external. If liveness checked the database,
a database outage would make the orchestrator kill every API replica and turn a
recoverable dependency failure into a total outage with no capacity left to
recover into. `ready` does check it, so a cut-off instance leaves the load
balancer *without* being restarted.

### Errors

One envelope for every failure, always carrying the request id:

```json
{
  "error": {
    "code": "validation_failed",
    "message": "the request failed validation",
    "fields": [
      {"field": "type",        "message": "must start with a lowercase letter and contain only lowercase letters, digits, underscores, dots or hyphens"},
      {"field": "payload",     "message": "must be a JSON object"},
      {"field": "priority",    "message": "must be between 1 and 100, got 999"},
      {"field": "max_retries", "message": "must be between 0 and 100, got -2"}
    ],
    "request_id": "2e7cada7-58ff-4d94-ab54-1fe1ab3316d6"
  }
}
```

Every problem is reported at once, so fixing three fields takes one round trip
rather than three. Unknown JSON fields are **rejected** rather than ignored: a
client that sends `"priorty": 10` is told, instead of silently receiving the
default priority.

| Situation | Status | Code |
|---|---|---|
| Created / idempotent hit | 201 / 200 | — |
| Malformed JSON, unknown field, empty body | 400 | `bad_request` |
| Content-Type not `application/json` | 415 | `unsupported_media_type` |
| Body over `HTTP_MAX_BODY_BYTES` | 413 | `payload_too_large` |
| Not found | 404 | `not_found` |
| Wrong method on a real endpoint | 405 | `method_not_allowed` (+ `Allow`) |
| Replaying a job that is not dead-lettered | 409 | `conflict` |
| Field validation failed | 422 | `validation_failed` |
| Request exceeded its deadline | 503 | `timeout` (+ `Retry-After`) |

---

## System design

### Job lifecycle

```
   POST /api/v1/jobs
        │  available_at = COALESCE(scheduled_at, now())
        ▼
   ┌─────────┐
   │ pending │◀──────────────────────────┐
   └────┬────┘                           │ retry (attempts <= max_retries):
        │ claim: FOR UPDATE SKIP LOCKED  │ available_at = now() + backoff(attempt)
        │        attempts    += 1        │ status = pending
        │        lease_epoch += 1        │
        │        lease_until  = now()+TTL│
        ▼                                │
   ┌─────────┐   handler error           │
   │ running │───────────┬───────────────┘
   └────┬────┘           │ attempts > max_retries, or a permanent error
        │ success        ▼
        ▼          ┌─────────────┐
   ┌───────────┐   │ dead_letter │──── POST /dead-letter-jobs/{id}/retry ──▶ pending
   │ completed │   └─────────────┘
   └───────────┘

   crash path: running AND lease_until < now()  ──reaper──▶  pending
```

| State | Meaning |
|---|---|
| `pending` | Claimable once `available_at` has passed. Covers both "never attempted" and "waiting out a backoff" — tell them apart with `attempts > 0`. |
| `running` | Leased by a worker until `lease_until`. |
| `completed` | Terminal. |
| `dead_letter` | Retries exhausted or a permanent error. Replayable by an operator. |
| `cancelled` | Terminal. |

There is deliberately **no `failed` resting state**. A `failed` state that some
background process later flips back to `pending` needs a second reaper loop and
a second partial index to do work the failing worker could do inside the
transaction it is already holding. Retry is therefore `running → pending` in one
statement. The payoff: the claim query checks exactly one status, and queue
depth is `count(*) WHERE status = 'pending'`.

The dead-letter queue is likewise a **status, not a separate table**. Moving
rows would mean `DELETE … RETURNING` + `INSERT`, two tables to search for "where
is job X", and duplicate indexes — while the partial index already excludes
dead-lettered rows from the hot path.

### Why `FOR UPDATE SKIP LOCKED`

Twenty workers poll the same query simultaneously. The alternatives:

| Approach | What happens |
|---|---|
| `SELECT` then `UPDATE` | Every worker reads the same top rows and every worker updates them. **The same job runs twenty times.** |
| `SELECT … FOR UPDATE` | Worker 1 locks the rows; workers 2–20 **block** on them, wake up, find them no longer pending, and start over. Correct, but the fleet is serialised — throughput is that of one worker however many you run. |
| `SERIALIZABLE` | Correct, but concurrent claims conflict and PostgreSQL resolves them by aborting transactions. Workers spend their time retrying serialisation failures. |
| **`SKIP LOCKED`** | "Pretend a locked row isn't there." Worker 1 takes rows 1–10, worker 2 immediately takes 11–20. No blocking, no duplicates, throughput scales with workers. |

The claim is a single statement — a CTE that locks and an `UPDATE` that marks —
so the transaction commits in one round trip. **It does not stay open while the
job runs.** Holding a transaction for a 30-second job would pin a connection and
block autovacuum.

> **The row lock protects the *claim*. The lease protects the *execution*.**

`attempts` increments at **claim** time, not on failure. A "poison" job that
crashes its worker process never reaches any failure path; if attempts only grew
on a recorded failure, such a job would be reclaimed forever, taking down every
worker that touched it.

### Leases, fencing, and the zombie worker

The failure a naive lease does not cover:

```
t=0    worker-A claims job J, lease_until = t+30s
t=5    worker-A freezes (GC pause, VM suspend, network partition)
t=31   the reaper sees the expired lease and returns J to pending
t=32   worker-B claims J and starts running it
t=33   worker-A resumes, unaware, and writes status='completed' WHERE id=J
```

Worker-B is now executing a job marked `completed`, and A has clobbered B's
claim. **Fix: fencing tokens.** Every claim increments `lease_epoch`, and every
worker write carries the epoch it was issued:

```sql
UPDATE jobs SET status = 'completed', …
 WHERE id = $1 AND worker_id = $2 AND lease_epoch = $3 AND status = 'running'
```

Worker-A's update matches **zero rows**; it learns it lost and abandons its
result. This is verified by `TestCompleteRejectsAStaleEpoch`.

This does **not** stop the side effect happening twice — A already sent the
email. That is why the guarantee is **at-least-once**, and why handlers must be
idempotent. Exactly-once *delivery* is not achievable across a network;
exactly-once *effect* is achievable only by making the effect idempotent.

### Concurrency model

Inside one worker process, `free` is a counting semaphore — a buffered channel
holding one token per execution slot:

```go
free := make(chan struct{}, concurrency)
```

The loop cannot claim work without first holding a token, so jobs in flight can
never exceed `Concurrency` — the bound is **structural**, not something a future
edit can accidentally break. It also claims exactly as many jobs as there are
free slots; claiming a fixed batch would mark jobs `running` while they queued
inside the process, burning lease time before execution even began.

**Terminology.** A *worker process* is one OS process and the unit of horizontal
scaling. A *slot* is one concurrent execution lane inside it. Total concurrency
is `processes × WORKER_CONCURRENCY`. "20 workers" is ambiguous and this project
never says it.

---

## Database design

### The claim index

```sql
CREATE INDEX jobs_claim_idx
    ON jobs (priority DESC, available_at ASC, id ASC)
    WHERE status = 'pending';
```

- **Partial.** A healthy queue holds millions of `completed` rows and a handful
  of pending ones. A full index would be almost entirely dead weight competing
  for cache; this one stays small and hot, and rows *leave* it on completion.
- **Column order mirrors the `ORDER BY` exactly**, so PostgreSQL walks the index
  in order and never sorts. Without that, `SKIP LOCKED` hands out jobs in
  arbitrary order and the priority feature is decorative.
- **`id` last** makes the ordering total, which makes concurrency tests
  deterministic.

**Measured** on 200,000 completed + 600 pending rows:

| | Execution time | Buffers read | Rows scanned |
|---|---|---|---|
| With `jobs_claim_idx` | **0.225 ms** | 22 | 10 |
| Index scans disabled | 31.116 ms | 3,948 | 200,600 (200,100 discarded) |

**138× faster, 180× fewer buffers.** The partial index is **64 kB** against the
primary key's **8,520 kB** on the same table — 133× smaller — because it indexes
only the rows that are actually claimable.

```powershell
Get-Content scripts/seed_jobs.sql     | docker compose exec -T postgres psql -U taskforge -d taskforge
Get-Content scripts/explain_claim.sql | docker compose exec -T postgres psql -U taskforge -d taskforge
```

### The other indexes

```sql
-- Lease recovery. Running jobs are a tiny fraction, so the reaper's
-- "WHERE status='running' AND lease_until < now()" is a narrow range scan.
CREATE INDEX jobs_lease_expiry_idx ON jobs (lease_until) WHERE status = 'running';

-- Idempotency. THIS, not an application-level SELECT, is what prevents
-- duplicates under concurrency.
CREATE UNIQUE INDEX jobs_idempotency_key_idx
    ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;
```

Deliberately **not** indexed: `type`, `worker_id`, and no GIN on `payload`. Each
costs write amplification on the hottest table in the system and is justified
only by a query that exists.

### `scheduled_at` vs `available_at`

Two columns that look redundant and are not:

- **`scheduled_at`** — *user intent*. Set once at creation, **never mutated**.
- **`available_at`** — *system eligibility*. Seeded from `scheduled_at`, then
  rewritten by every retry backoff.

**The claim query reads only `available_at`.** Collapsing them would mean a
retry backoff silently overwrites what the client asked for.

### Other invariants

`timestamptz` everywhere, never `timestamp` — `timestamptz` stores an absolute
instant, while plain `timestamp` stores a zoneless wall-clock reading and will
corrupt lease arithmetic the moment two containers disagree about their zone.
Compose forces `TZ=UTC` and `PGTZ=UTC`, and the pool sets `timezone=UTC` on
every session.

Eight `CHECK` constraints back up the application rules, including:

```sql
CONSTRAINT jobs_running_has_lease CHECK (
    status <> 'running' OR (lease_until IS NOT NULL AND worker_id IS NOT NULL)
)
```

so "a worker forgot to set the lease" is a failed write rather than a silently
stuck job.

---

## Failure recovery

Kill a worker mid-job and watch another reclaim it. **This is a real run:**

```powershell
# lease 6s, reaper every 3s, so recovery is observable in seconds
$env:WORKER_LEASE_DURATION="6s"; $env:WORKER_REAPER_INTERVAL="3s"
# submit a 60-second job, then SIGKILL the worker holding it
Stop-Process -Id $victim.Id -Force
```

| Moment | status | worker_id | attempts | lease_epoch | lease valid |
|---|---|---|---|---|---|
| Job claimed | `running` | victim | 1 | 1 | ✅ |
| After `SIGKILL` | `running` | victim | 1 | 1 | ❌ orphaned |
| After the reaper | `running` | **rescuer** | 2 | **2** | ✅ |

```
level=WARN msg="reclaimed jobs from expired leases" component=reaper requeued=1 dead_lettered=0
```

`last_error` reads *"lease expired: worker victim stopped reporting; job
reclaimed"*, and the epoch bump to 2 means the dead worker could never have
written to that row even if it came back.

**Every worker runs a reaper.** Electing a single reaper node would need leader
election and would make that node a single point of failure for the very
mechanism that exists to survive failure. The reclaim query uses
`FOR UPDATE SKIP LOCKED` too, so concurrent reapers never conflict.

### Retries and backoff — a real sequence

```
16:20:35.795  flaky  attempt=1  retry_in=601.881548ms   job failed, scheduled for retry
16:20:36.813  flaky  attempt=2  retry_in=1.469315999s   job failed, scheduled for retry
16:20:38.827  flaky  attempt=3  retry_in=-              job dead-lettered
```

Base 1s → 2s → exhausted, each value jittered into `[half, full]`.

**Why jitter is not an optimisation.** A downstream API goes down for 30 seconds
with 5,000 jobs in flight. All 5,000 fail at once. With pure exponential backoff
every one retries at exactly `base`, then exactly `2×base`, forever in lockstep
— 5,000 simultaneous requests. The API comes back up, is immediately flattened
by the synchronised herd, and falls over again. **Without jitter, backoff makes
a recovering dependency worse.**

This project uses *equal jitter* (`half + rand(0, half)`) rather than *full
jitter* (`rand(0, delay)`): full jitter spreads marginally better but can
produce a near-zero wait, which is the wrong behaviour when the dependency is
still down.

Permanent failures (`job.Permanent(err)`) skip retries entirely and dead-letter
on the first attempt — an unknown job type, a malformed payload, a 4xx from an
upstream API. Retrying those burns the budget and hammers somebody else's API to
no purpose.

---

## Observability

### Metrics

| Metric | Type | Labels |
|---|---|---|
| `taskforge_jobs_created_total` | counter | `job_type` |
| `taskforge_jobs_processed_total` | counter | `job_type`, `outcome` |
| `taskforge_jobs_retried_total` | counter | `job_type` |
| `taskforge_jobs_dead_lettered_total` | counter | `job_type`, `reason` |
| `taskforge_jobs_reclaimed_total` | counter | — |
| `taskforge_job_processing_duration_seconds` | histogram | `job_type`, `outcome` |
| `taskforge_job_wait_duration_seconds` | histogram | `job_type` |
| `taskforge_queue_depth` | gauge | `status` |
| `taskforge_queue_oldest_pending_seconds` | gauge | — |
| `taskforge_active_workers`, `taskforge_fleet_capacity` | gauge | — |
| `taskforge_http_requests_total` | counter | `method`, `route`, `status` |

**Cardinality is the thing that kills monitoring systems.** Prometheus creates
one time series per unique label combination, and each costs memory essentially
forever. Every label here comes from a small closed set. Notably `route` is the
**route pattern** — `GET /api/v1/jobs/{id}` is one series, whereas the raw path
would be one series per job id ever requested.

Two bugs found and fixed while building this, both worth knowing:

1. **`r.Pattern` is empty in middleware.** `ServeMux` sets it on the *clone* of
   the request it passes inward, so a middleware wrapped around the mux sees ""
   and labels every request `unmatched`. The router now passes a resolver that
   asks the mux directly.
2. **Unpopulated gauges are not harmless zeroes.** Worker processes were
   exporting `taskforge_active_workers 0`, which made the alert
   `taskforge_active_workers == 0` fire permanently. Fleet gauges are now
   registered only in the process that populates them.

Both are the kind of thing that produces plausible-looking dashboards that are
entirely wrong.

### The one number to watch

`taskforge_queue_oldest_pending_seconds`. Depth alone is ambiguous — 10,000 jobs
draining fast is fine; 50 jobs waiting an hour is an outage. Age answers "is
work actually moving".

Alerting rules are in
[deployments/prometheus/rules.yml](deployments/prometheus/rules.yml) and target
symptoms rather than internal states.

### Logging

`slog`, JSON in containers. Every job log line carries `job_id`, `job_type`,
`worker_id`, `attempt`, `duration_ms`; every request line carries `request_id`.
Level is chosen by outcome — 5xx is `ERROR`, 4xx is `WARN`, because logging
every 404 at ERROR is how alerting gets ignored.

**Payloads are never logged**, and the database password is unprintable by
construction: `config.Database` implements `slog.LogValuer` and simply does not
emit the field, so even `log.Info("cfg", "config", cfg)` cannot leak it.

---

## Testing

```bash
make test               # unit tests, no database needed
make test-race          # race detector (needs a C compiler)
make test-race-docker   # same, inside golang:1.26.5 — nothing to install
```

Integration tests skip unless a DSN is present, so `go test ./...` stays green
without a database:

```powershell
$env:TASKFORGE_TEST_DSN="postgres://taskforge:taskforge@localhost:5433/taskforge?sslmode=disable"
go test ./... -count=1
```

The tests that matter most:

| Test | What it proves |
|---|---|
| `TestClaimNeverHandsTheSameJobToTwoWorkers` | 200 jobs, 16 concurrent workers, **zero duplicate claims** |
| `TestCreateConcurrentIdempotency` | 16 goroutines racing on one key produce **exactly one row** |
| `TestCompleteRejectsAStaleEpoch` | A zombie worker's write is refused and changes nothing |
| `TestFailRetriesUntilBudgetExhausted` | The full retry → dead-letter lifecycle |
| `TestWorkerRespectsConcurrencyLimit` | Peak in-flight never exceeds the semaphore |
| `TestWorkerFinishesInFlightJobsAfterCancellation` | Graceful shutdown really finishes the job |
| `TestBackoffDoesNotOverflow` | `base × 2^attempt` overflows int64 and goes **negative**, which would spin the queue |
| `TestLogValueNeverLeaksPassword` | The password cannot reach the logs |
| `TestJSONErrorsPreserves405` | A catch-all `/` route silently destroys 405 handling |

The queue's correctness lives in SQL — `SKIP LOCKED`, partial unique indexes,
`CHECK` constraints — so it is tested against **real PostgreSQL**. Mocking the
driver would prove only that the mock behaves like the mock.

---

## Benchmarks

Measured on this machine. **Reproduce, do not quote:**

```powershell
$env:TASKFORGE_TEST_DSN="postgres://taskforge:taskforge@localhost:5433/taskforge?sslmode=disable"
go test ./internal/job/postgres/ -bench=Benchmark -benchmem -run='XXX' -benchtime=2s
cd internal/job; go test -bench=Benchmark -benchmem -run='XXX' -benchtime=2s .
```

```
goos: windows   goarch: amd64   cpu: 12th Gen Intel(R) Core(TM) i5-12450H
```

### Database path

| Benchmark | ns/op | ms/op | Derived throughput |
|---|---:|---:|---:|
| `Create` | 2,652,244 | 2.65 | 377 jobs/s |
| `Claim/batch=1` | 2,642,584 | 2.64 | 379 jobs/s |
| `Claim/batch=5` | 3,107,636 | 3.11 | 1,609 jobs/s |
| `Claim/batch=20` | 3,465,080 | 3.47 | 5,772 jobs/s |
| `Claim/batch=50` | 3,979,561 | 3.98 | **12,564 jobs/s** |
| `ClaimParallel` (10/batch, 12 threads) | 630,578 | 0.63 | **15,858 jobs/s** |
| `ClaimWithAging/batch=20` | 133,441,838 | 133.44 | 150 jobs/s |

### Pure Go, no I/O

| Benchmark | ns/op | allocs/op |
|---|---:|---:|
| `StateTransition` | 29.15 | 0 |
| `BackoffDelay` | 60.66 | 0 |
| `RegistryGet` | 60.90 | 0 |
| `RegistryGetParallel` (12 threads) | 114.7 | 0 |
| `CreateParamsValidate` | 1,183 | 2 |
| `ServiceCreate` (in-memory repo) | 6,791 | 8 |

### What these numbers actually say

**1. The database round trip is 99.7% of the latency.** `ServiceCreate` against
an in-memory repository is **6.8 µs**; the same operation against PostgreSQL is
**2,652 µs**. Optimising the Go code would be pointless — the only thing that
matters is the number of round trips.

**2. Batching is nearly free.** Claiming 50 jobs costs 3.98 ms against 2.64 ms
for one — **50× the work for 1.5× the time**, because parse, plan, lock
acquisition and the network hop are paid once. This is why the worker claims as
many jobs as it has free slots rather than one at a time.

**3. `SKIP LOCKED` scales with concurrency.** 12 goroutines claiming
concurrently reach 15,858 jobs/s versus 3,125 jobs/s for one goroutine doing the
same batch size — a **5× speedup from parallelism alone**. Under
`FOR UPDATE` without `SKIP LOCKED`, adding workers would make this *worse*.

**4. Priority aging costs 38×.** `ClaimWithAging` is 133.44 ms against 3.47 ms
for the same batch size. The aged ordering ranks by a computed expression that
`jobs_claim_idx` cannot serve, so PostgreSQL reads every claimable row and sorts
it. **This measurement is why aging is off by default** — it is a real feature
with a real price, and the price should be a deliberate choice.

**Caveat, stated plainly:** these run against a Docker Desktop container on
Windows, whose network stack adds meaningful per-round-trip overhead. A 2.65 ms
single-row `INSERT` is far slower than PostgreSQL on a Unix socket would be. The
absolute numbers describe *this* setup; the **ratios** are what generalise.

---

## Load testing

[tests/load/submit.js](tests/load/submit.js) drives the submission path with k6:

```bash
k6 run tests/load/submit.js
k6 run -e RATE=500 -e DURATION=2m tests/load/submit.js
```

It uses a `constant-arrival-rate` executor, not fixed VUs. The difference
matters: with fixed virtual users a slowing server produces *less* load, because
each user waits for its response — which hides exactly the overload behaviour
you are trying to measure.

A load test that only reports HTTP latency will cheerfully tell you everything
is fine while the queue grows without bound behind it. Watch
`taskforge_queue_oldest_pending_seconds` in Grafana while it runs; if that
climbs, the API kept up but the workers did not, and the fix is
`docker compose up -d --scale worker=N`.

Throughput figures belong to the hardware they were measured on, so none are
quoted here — run it against your own deployment and read the numbers off your
own dashboard.

---

## Configuration

Every variable, its default and its meaning are documented in
[.env.example](.env.example). Nothing outside `internal/config` reads the
environment.

Highlights:

| Variable | Default | Notes |
|---|---|---|
| `WORKER_CONCURRENCY` | `5` | Slots per **process**. Must be ≤ `DB_MAX_CONNS - 1`. |
| `WORKER_LEASE_DURATION` | `60s` | Renewed every ⅓ while a job runs, so it does *not* cap job duration. Minimum 5s. |
| `WORKER_JOB_TIMEOUT` | `30s` | May exceed the lease, precisely because of renewal. |
| `WORKER_REAPER_INTERVAL` | `30s` | Worst-case recovery ≈ `LEASE_DURATION + REAPER_INTERVAL`. |
| `WORKER_BACKOFF_BASE/MAX/FACTOR` | `1s` / `5m` / `2` | `delay = base × factor^(attempt-1)`, capped, then jittered. |
| `WORKER_AGING_ENABLED` | `false` | Starvation control. Costs 38× on the claim query — see benchmarks. |
| `WEBHOOK_ALLOW_PRIVATE_TARGETS` | `true` locally | **Set false in production.** See below. |

Invalid configuration fails at startup with **every** problem listed at once,
and cross-field rules are enforced too:

```
api: fatal: config: LOG_LEVEL: must be one of [debug info warn error], got "verbose"
HTTP_READ_TIMEOUT: must be a duration such as 500ms, 5s or 1m30s, got "5 seconds"
```

### Security notes

- **SSRF.** The webhook handler fetches a URL taken from user-supplied job
  payload, from inside your network. A job with
  `"url": "http://169.254.169.254/latest/meta-data/iam/security-credentials/"`
  would turn the queue into a cloud-credential exfiltration tool. The handler
  installs a `net.Dialer.Control` hook that rejects loopback, private,
  link-local and multicast addresses **after DNS resolution** — checking the
  hostname instead would be bypassable by a public name resolving to 127.0.0.1.
- Request bodies are capped; all timeouts are set (Go's zero-value
  `http.Server` has **none**); all SQL is parameterised; error responses never
  contain internal detail; containers run as `nonroot` on distroless.

---

## Make targets

```
make setup            create .env from .env.example
make run / run-worker run a process locally
make build            compile both binaries into ./bin
make test             run all tests
make test-race        race detector (needs a C compiler)
make test-race-docker race detector inside golang:1.26.5 — nothing to install
make fmt / vet / lint / tidy
make docker-db        PostgreSQL only
make docker-up        build and start the full stack
make docker-scale     full stack with 3 worker replicas
make docker-down      stop, keep data
make docker-nuke      stop and delete the data volume
make psql             psql shell inside the container
make migrate-up / migrate-down / migrate-status / migrate-create
```

**On the race detector and Windows.** `-race` is implemented in C, so it needs
cgo and therefore a C compiler, which a stock Windows install lacks. Either run
`make test-race-docker`, or `winget install BrechtSanders.WinLibs.POSIX.UCRT`
and use `make test-race` directly.

---

## Project layout

```
cmd/api                     API entrypoint: wiring only
cmd/worker                  Worker entrypoint: claim loop + reaper + heartbeat + metrics
internal/config             Environment → validated Config; the only reader of os.Getenv
internal/database           pgxpool setup, Pinger interface, health check
internal/job                Domain, state machine, validation, backoff, service, interfaces
internal/job/postgres       The pgx implementation — the only package importing pgx
internal/worker             Goroutine pool, lease renewal, reaper, heartbeat
internal/handlers           Concrete job handlers: sleep, send_email, webhook, flaky
internal/httpx              JSON codec, error envelope, server + graceful shutdown
internal/api                Router, handlers, middleware
internal/observability      logging (slog), metrics (Prometheus), queuemetrics collector
migrations/                 golang-migrate .up.sql / .down.sql pairs
scripts/                    setupenv, seed_jobs.sql, explain_claim.sql
deployments/                Prometheus config + rules, Grafana provisioning + dashboard
tests/load/                 k6 load test
docs/                       design.md, openapi.yaml
```

The domain lives in **one** package (`internal/job`) rather than being split
into `domain/`, `service/` and `repository/` folders. Go's unit of encapsulation
is the package; splitting a cohesive domain across four of them forces you to
export everything and invites import cycles. The `postgres` sub-package is
separate because it is a real dependency boundary.

Three small interfaces, segregated by consumer, all implemented by one type:

```go
job.Repository     // API side:    Create, GetByID, List, CountByStatus, RetryDeadLetter
job.Queue          // Worker side: Claim, Complete, Fail, RenewLease, ReclaimExpired
job.WorkerRegistry // Heartbeats:  Register, Heartbeat, ListWorkers, Prune
```

The API never claims jobs and the worker never lists them, so neither depends on
what it does not use.

> Go's ban on import cycles caught a genuine design error during this build:
> `internal/job` needs the metrics collectors, and a queue-stats collector
> needed the job types — `job → metrics → job`. The compiler rejecting it was
> the signal that the metrics package had grown a responsibility that did not
> belong to it. It now lives in `internal/observability/queuemetrics`.

---

## Design decisions and tradeoffs

**Why PostgreSQL and not Redis.** Two reasons that matter more than throughput:
a single transaction covers both the job state change and the business rows the
job produced — with Redis those are two systems and you must build reconciliation;
and Redis's default persistence can lose the last fsync window, which for a
queue means silently losing accepted work. Redis is faster in raw ops/sec. The
position taken here is *PostgreSQL until measurements say otherwise* — and the
benchmarks above are those measurements.

**When I would introduce Redis or Kafka.** When `Claim` latency stops being
dominated by the round trip and starts being dominated by lock contention on the
index — visible as claim latency rising while queue depth is stable. Or when
fan-out to multiple independent consumers is needed, which is a log, not a
queue, and Postgres is the wrong shape for it.

**Where the bottleneck goes next.** At ~15,800 claims/s measured on one box, the
first limit is PostgreSQL connections: total connections =
`MaxConns × (api + worker replicas)`, against a default `max_connections` of
100. The fix is PgBouncer in transaction mode before it is sharding. After that,
the `jobs` table's write amplification — at which point partitioning by
`created_at` with a retention policy on completed rows earns its keep.

**Accepted limitations.**

| Decision | Cost |
|---|---|
| Polling, not `LISTEN/NOTIFY` | Up to `WORKER_POLL_INTERVAL` of idle latency, and a steady baseline of empty queries. |
| Offset pagination | Deep pages are slow, so offset is capped at 10,000 rather than served badly. Keyset pagination is the fix if a real use case appears. |
| Every worker runs a reaper | One extra indexed query per worker per interval, in exchange for no leader election and no single point of failure. |
| No authentication | Deliberately out of scope for v1. It belongs at the gateway, and adding a half-considered scheme would be worse than none. |
| Aging off by default | Strict priority can starve low-priority work. Enabling aging costs 38× on the claim query. Both are documented; neither is hidden. |

---

## Licence

MIT.
