// k6 load test: sustained job submission against the API.
//
//   k6 run tests/load/submit.js
//   k6 run -e RATE=500 -e DURATION=2m tests/load/submit.js
//   k6 run -e BASE_URL=http://localhost:8080 tests/load/submit.js
//
// Install k6:  winget install k6.k6   |   brew install k6
//
// This measures the SUBMISSION path only. Whether workers keep up is a separate
// question, answered by taskforge_queue_oldest_pending_seconds in Grafana while
// this runs. A load test that only reports HTTP latency will happily tell you
// everything is fine while the queue grows without bound behind it.

import http from "k6/http";
import { check } from "k6";
import { Counter, Trend } from "k6/metrics";
import { randomIntBetween } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const RATE = parseInt(__ENV.RATE || "100", 10);
const DURATION = __ENV.DURATION || "60s";

const created = new Counter("jobs_created");
const deduplicated = new Counter("jobs_deduplicated");
const submitLatency = new Trend("job_submit_latency", true);

export const options = {
  scenarios: {
    // constant-arrival-rate, not constant-VUs. The difference matters: with a
    // fixed number of virtual users, a slowing server produces LESS load,
    // because each user waits for its response before sending the next request.
    // That hides exactly the overload behaviour you are trying to measure.
    // Arrival rate holds the offered load constant regardless of how the server
    // copes, which is what real traffic does.
    steady: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: DURATION,
      preAllocatedVUs: Math.max(20, Math.ceil(RATE / 5)),
      // Allow k6 to add VUs if responses slow down, otherwise it cannot sustain
      // the target rate and silently under-loads.
      maxVUs: Math.max(100, RATE * 2),
    },
  },
  thresholds: {
    // Fail the run rather than printing a red number nobody notices.
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500", "p(99)<1000"],
  },
};

const JOB_TYPES = [
  { type: "sleep", payload: () => ({ seconds: 0.05 }) },
  { type: "send_email", payload: () => ({ to: `user${randomIntBetween(1, 100000)}@example.com`, subject: "Load test" }) },
  { type: "flaky", payload: () => ({ fail_rate: 0.1 }) },
];

export default function () {
  const spec = JOB_TYPES[randomIntBetween(0, JOB_TYPES.length - 1)];

  const body = JSON.stringify({
    type: spec.type,
    payload: spec.payload(),
    // Mixed priorities so the run also exercises the ordering path.
    priority: randomIntBetween(1, 100),
    max_retries: 3,
  });

  const res = http.post(`${BASE_URL}/api/v1/jobs`, body, {
    headers: { "Content-Type": "application/json" },
    tags: { name: "POST /api/v1/jobs" },
  });

  submitLatency.add(res.timings.duration);

  const ok = check(res, {
    "status is 201 or 200": (r) => r.status === 201 || r.status === 200,
    "body has an id": (r) => {
      try {
        return typeof r.json("id") === "string";
      } catch {
        return false;
      }
    },
  });

  if (ok) {
    if (res.status === 201) created.add(1);
    else deduplicated.add(1);
  }
}

export function handleSummary(data) {
  const m = data.metrics;
  const get = (name, stat) => (m[name] && m[name].values ? m[name].values[stat] : 0);

  const lines = [
    "",
    "  TaskForge submission load test",
    "  ------------------------------",
    `  target rate      ${RATE}/s for ${DURATION}`,
    `  requests         ${get("http_reqs", "count")}`,
    `  achieved rate    ${get("http_reqs", "rate").toFixed(1)}/s`,
    `  jobs created     ${get("jobs_created", "count")}`,
    `  deduplicated     ${get("jobs_deduplicated", "count")}`,
    `  error rate       ${(get("http_req_failed", "rate") * 100).toFixed(2)}%`,
    "",
    `  latency  p50     ${get("http_req_duration", "med").toFixed(1)} ms`,
    `           p95     ${get("http_req_duration", "p(95)").toFixed(1)} ms`,
    `           p99     ${get("http_req_duration", "p(99)").toFixed(1)} ms`,
    `           max     ${get("http_req_duration", "max").toFixed(1)} ms`,
    "",
    "  Now check Grafana: if taskforge_queue_oldest_pending_seconds is climbing,",
    "  the API kept up but the workers did not. Add worker replicas with",
    "  `docker compose up -d --scale worker=N` and run this again.",
    "",
  ];

  return {
    stdout: lines.join("\n"),
    "tests/load/summary.json": JSON.stringify(data, null, 2),
  };
}
