/*
 * TaskForge — browser-side simulation of the claim / lease / fencing /
 * reaper mechanics described in the real Go backend, PLUS an optional live
 * mode that talks to the actual deployed API over HTTPS. Simulated mode
 * needs no server and can never fail; live mode is opt-in, times out
 * gracefully, and always leaves the page in a working state either way.
 */
(() => {
  'use strict';

  // The deployed API's origin. No trailing slash.
  const API_BASE_URL = 'https://taskforge-api.onrender.com';

  // Render's free tier spins a service down after ~15 minutes idle; the next
  // request wakes it, which can take the better part of a minute. This
  // timeout has to be generous enough to survive that cold start rather than
  // falsely reporting the backend as unreachable.
  const LIVE_CONNECT_TIMEOUT_MS = 55000;
  const LIVE_POLL_MS = 1500;
  // The API rejects any limit over 200 (internal/job/params.go's
  // MaxListLimit) with a 400, so this has to match exactly, not just be
  // "generous".
  const LIVE_COUNT_LIMIT = 200;

  const LEASE_MS = 5000;      // simulated lease duration
  const TICK_MS = 100;        // render/physics tick
  const MIN_JOB_MS = 2200;
  const MAX_JOB_MS = 4200;
  const MAX_LOG_LINES = 60;

  const WORKER_COUNT = 3;

  /** @typedef {{id:string, type:'sleep'|'flaky', attempts:number, maxRetries:number}} Job */

  const state = {
    mode: 'simulated', // 'simulated' | 'live'
    pending: /** @type {Job[]} */ ([]),
    workers: /** @type {any[]} */ ([]),
    completed: 0,
    retried: 0,
    dead: 0,
    reclaimed: 0,
    log: /** @type {string[]} */ ([]),
  };

  let liveTimer = null;

  function makeId() {
    return Array.from({ length: 8 }, () => '0123456789abcdef'[Math.floor(Math.random() * 16)]).join('');
  }

  function initWorkers() {
    state.workers = Array.from({ length: WORKER_COUNT }, (_, i) => ({
      name: `worker-${i + 1}`,
      status: 'idle',            // idle | busy | dead
      job: null,
      startedAt: 0,
      duration: 0,
      leaseUntil: 0,
      leaseEpoch: 0,
    }));
  }

  function log(level, msg) {
    const time = new Date().toISOString().split('T')[1].replace('Z', '');
    const cls = level === 'WARN' ? 'lvl-warn' : level === 'ERROR' ? 'lvl-error' : 'lvl-info';
    state.log.unshift(
      `<span class="${cls}">${level.padEnd(5)}</span> <span style="color:var(--text-faint)">${time}</span>  ${msg}`
    );
    if (state.log.length > MAX_LOG_LINES) state.log.length = MAX_LOG_LINES;
  }

  // ---------------------------------------------------------------- actions

  function submitJob(type = 'sleep') {
    const job = { id: makeId(), type, attempts: 0, maxRetries: 2 };
    state.pending.push(job);
    log('INFO', `job created&nbsp; type=${type} job_id=${job.id} status=pending`);
    render();
  }

  function submitBatch(n) {
    for (let i = 0; i < n; i++) {
      state.pending.push({ id: makeId(), type: 'sleep', attempts: 0, maxRetries: 2 });
    }
    log('INFO', `job created&nbsp; type=sleep count=${n} status=pending`);
    render();
  }

  function killWorker(idx) {
    const w = state.workers[idx];
    if (w.status === 'dead') return;
    if (w.status === 'busy') {
      log('ERROR', `worker crashed&nbsp; worker=${w.name} signal=SIGKILL job_id=${w.job.id} lease_epoch=${w.leaseEpoch} (lease still valid — job appears stuck)`);
    } else {
      log('ERROR', `worker crashed&nbsp; worker=${w.name} signal=SIGKILL (was idle)`);
    }
    w.status = 'dead';
    render();
  }

  function restartWorker(idx) {
    const w = state.workers[idx];
    w.status = 'idle';
    w.job = null;
    log('INFO', `worker started&nbsp; worker=${w.name} concurrency=1`);
    render();
  }

  function resetAll() {
    state.pending = [];
    state.completed = 0;
    state.retried = 0;
    state.dead = 0;
    state.reclaimed = 0;
    state.log = [];
    initWorkers();
    log('INFO', 'simulation reset');
    render();
  }

  // ------------------------------------------------------------------ tick

  function tick() {
    if (state.mode !== 'simulated') return;
    const now = performance.now();

    // 1. Idle workers claim pending work, oldest-first — mirrors
    //    ORDER BY priority DESC, available_at ASC, id ASC under SKIP LOCKED.
    for (const w of state.workers) {
      if (w.status !== 'idle' || state.pending.length === 0) continue;
      const job = state.pending.shift();
      w.status = 'busy';
      w.job = job;
      w.startedAt = now;
      w.duration = job.type === 'flaky'
        ? 1500 + Math.random() * 800
        : MIN_JOB_MS + Math.random() * (MAX_JOB_MS - MIN_JOB_MS);
      w.leaseUntil = now + LEASE_MS;
      w.leaseEpoch += 1;
      log('INFO', `job claimed&nbsp; worker=${w.name} job_id=${job.id} type=${job.type} attempt=${job.attempts + 1} lease_epoch=${w.leaseEpoch}`);
    }

    // 2. Busy, alive workers make progress; resolve on completion.
    for (const w of state.workers) {
      if (w.status !== 'busy') continue;
      const elapsed = now - w.startedAt;
      if (elapsed < w.duration) continue;

      const job = w.job;
      job.attempts += 1;

      if (job.type === 'flaky' && Math.random() < 0.55) {
        if (job.attempts > job.maxRetries) {
          state.dead += 1;
          log('ERROR', `job dead-lettered&nbsp; worker=${w.name} job_id=${job.id} attempts_used=${job.attempts} reason=retries_exhausted`);
        } else {
          const backoff = Math.round(1000 * Math.pow(2, job.attempts - 1));
          state.retried += 1;
          log('WARN', `job failed, scheduled for retry&nbsp; worker=${w.name} job_id=${job.id} attempt=${job.attempts} retry_in=${backoff}ms`);
          state.pending.push(job);
        }
      } else {
        state.completed += 1;
        log('INFO', `job completed&nbsp; worker=${w.name} job_id=${job.id} duration_ms=${Math.round(w.duration)}`);
      }

      w.status = 'idle';
      w.job = null;
    }

    // 3. Dead workers holding a job: once the lease expires, the reaper
    //    (which every live worker runs) reclaims it back to pending.
    for (const w of state.workers) {
      if (w.status === 'dead' && w.job && now >= w.leaseUntil) {
        const job = w.job;
        w.job = null;
        state.reclaimed += 1;
        state.pending.push(job);
        log('WARN', `reclaimed jobs from expired leases&nbsp; component=reaper requeued=1 job_id=${job.id} previous_worker=${w.name}`);
      }
    }

    render();
  }

  // ---------------------------------------------------------------- render

  const el = {
    pendingList: document.getElementById('pending-list'),
    pendingCount: document.getElementById('pending-count'),
    workersGrid: document.getElementById('workers-grid'),
    completed: document.getElementById('completed-count'),
    retried: document.getElementById('retried-count'),
    dead: document.getElementById('dead-count'),
    reclaimed: document.getElementById('reclaimed-count'),
    logBody: document.getElementById('log-body'),
  };

  function render() {
    // Pending list
    el.pendingCount.textContent = String(state.pending.length);
    if (state.pending.length === 0) {
      el.pendingList.innerHTML = '<div class="empty-note">No jobs pending</div>';
    } else {
      el.pendingList.innerHTML = state.pending
        .slice(0, 30)
        .map(j => `<div class="job-chip${j.type === 'flaky' ? ' flaky-chip' : ''}"><span>${j.type}</span><span>${j.id}</span></div>`)
        .join('');
    }

    // Workers
    if (state.workers.length === 0 && state.mode === 'live') {
      el.workersGrid.innerHTML = '<div class="empty-note">No workers reporting yet — the worker service may still be waking up.</div>';
    } else {
      el.workersGrid.innerHTML = state.workers.map((w, i) => {
        if (w.live) {
          // Real data from GET /api/v1/workers: alive/dead is derived from
          // heartbeat freshness server-side, and active_jobs/concurrency is
          // the only per-worker load signal that endpoint exposes — there is
          // no per-job detail to animate, so this card reports capacity
          // instead of a simulated progress bar.
          const statusLabel = w.status === 'dead' ? 'dead' : w.status === 'busy' ? 'running' : 'idle';
          const pct = w.liveConcurrency ? Math.min(100, (100 * w.liveActiveJobs) / w.liveConcurrency) : 0;
          return `
            <div class="worker-card state-${w.status}">
              <div class="worker-head">
                <span class="worker-name">${w.name}</span>
                <span class="worker-status st-${w.status}">${statusLabel}</span>
              </div>
              <div class="worker-job-line">${w.liveActiveJobs}/${w.liveConcurrency} slots busy</div>
              <div class="worker-progress"><div class="worker-progress-fill" style="width:${pct}%"></div></div>
              <div class="worker-actions"><span class="icon-btn" style="cursor:default;opacity:.55">real worker</span></div>
            </div>`;
        }

        const now = performance.now();
        let pct = 0;
        let jobLine = '&nbsp;';
        if (w.status === 'busy') {
          pct = Math.min(100, ((now - w.startedAt) / w.duration) * 100);
          jobLine = `${w.job.type} · ${w.job.id}`;
        } else if (w.status === 'dead' && w.job) {
          pct = 100;
          jobLine = `${w.job.type} · ${w.job.id} (orphaned, lease expiring…)`;
        }
        const statusLabel = w.status === 'busy' ? 'running' : w.status === 'dead' ? 'dead' : 'idle';
        return `
          <div class="worker-card state-${w.status}">
            <div class="worker-head">
              <span class="worker-name">${w.name}</span>
              <span class="worker-status st-${w.status}">${statusLabel}</span>
            </div>
            <div class="worker-job-line">${jobLine}</div>
            <div class="worker-progress"><div class="worker-progress-fill" style="width:${pct}%"></div></div>
            <div class="worker-actions">
              ${w.status === 'dead'
                ? `<button class="icon-btn restart" data-restart="${i}">restart</button>`
                : `<button class="icon-btn" data-kill="${i}">kill -9</button>`}
            </div>
          </div>`;
      }).join('');
    }

    // Outcomes
    el.completed.textContent = String(state.completed);
    el.retried.textContent = String(state.retried);
    el.dead.textContent = String(state.dead);
    el.reclaimed.textContent = String(state.reclaimed);

    // Log
    el.logBody.innerHTML = state.log.length
      ? state.log.map(l => `<div class="log-line">${l}</div>`).join('')
      : '<div class="log-empty">No output yet — submit a job to begin.</div>';
  }

  // ------------------------------------------------------------- live mode

  const modeStatusEl = document.getElementById('mode-status');
  const modeSimBtn = document.getElementById('mode-simulated');
  const modeLiveBtn = document.getElementById('mode-live');
  const demoHintEl = document.getElementById('demo-hint');
  const resetBtn = document.getElementById('btn-reset');

  function setModeStatus(kind, text) {
    modeStatusEl.innerHTML = `<span class="mode-dot mode-dot-${kind}"></span> ${text}`;
  }

  function setActiveModeButton(mode) {
    modeSimBtn.classList.toggle('active', mode === 'simulated');
    modeSimBtn.setAttribute('aria-selected', String(mode === 'simulated'));
    modeLiveBtn.classList.toggle('active', mode === 'live');
    modeLiveBtn.setAttribute('aria-selected', String(mode === 'live'));
  }

  function relabelOutcomes(mode) {
    document.getElementById('label-retried').textContent = mode === 'live' ? 'Running' : 'Retried';
    document.getElementById('label-reclaimed').textContent = mode === 'live' ? 'Alive workers' : 'Reclaimed';
  }

  async function apiGet(path) {
    const res = await fetch(API_BASE_URL + path);
    if (!res.ok) throw new Error(`GET ${path} -> HTTP ${res.status}`);
    return res.json();
  }

  async function connectLive() {
    modeSimBtn.disabled = true;
    modeLiveBtn.disabled = true;
    setModeStatus('connecting', 'Connecting to the live backend — a free-tier host that has been idle can take up to a minute to wake up…');

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), LIVE_CONNECT_TIMEOUT_MS);

    try {
      const res = await fetch(API_BASE_URL + '/api/v1/ready', { signal: controller.signal });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      clearTimeout(timer);
      enterLiveMode();
    } catch (err) {
      clearTimeout(timer);
      setActiveModeButton('simulated');
      setModeStatus('error', 'Live backend unreachable right now — staying on the simulated demo. (Free-tier hosts sleep after inactivity; this page keeps working either way.)');
      log('ERROR', `live backend unreachable: ${err.message}`);
    } finally {
      modeSimBtn.disabled = false;
      modeLiveBtn.disabled = false;
    }
  }

  function enterLiveMode() {
    state.mode = 'live';
    setActiveModeButton('live');
    setModeStatus('live', 'Connected — this is real data from the deployed API and PostgreSQL database.');
    demoHintEl.textContent = 'This is the real backend: Submit really creates rows in PostgreSQL and real worker processes claim them. The crash/reclaim demo stays in Simulated mode, since a browser tab can\'t SIGKILL a real cloud container.';
    relabelOutcomes('live');
    resetBtn.textContent = 'Refresh';
    log('INFO', 'switched to live backend');
    livePoll();
    liveTimer = setInterval(livePoll, LIVE_POLL_MS);
  }

  function exitLiveMode() {
    state.mode = 'simulated';
    if (liveTimer) {
      clearInterval(liveTimer);
      liveTimer = null;
    }
    setActiveModeButton('simulated');
    setModeStatus('sim', 'Running fully in your browser — no server required.');
    demoHintEl.textContent = "Click a worker's kill icon to simulate a crash mid-job, then watch the reaper reclaim it after the lease expires.";
    relabelOutcomes('simulated');
    resetBtn.textContent = 'Reset';
    resetAll();
  }

  async function livePoll() {
    try {
      const [pendingJson, runningJson, completedJson, deadJson, workersJson] = await Promise.all([
        apiGet(`/api/v1/jobs?status=pending&limit=${LIVE_COUNT_LIMIT}`),
        apiGet(`/api/v1/jobs?status=running&limit=${LIVE_COUNT_LIMIT}`),
        apiGet(`/api/v1/jobs?status=completed&limit=${LIVE_COUNT_LIMIT}`),
        apiGet(`/api/v1/jobs?status=dead_letter&limit=${LIVE_COUNT_LIMIT}`),
        apiGet('/api/v1/workers'),
      ]);

      // The mode may have changed while these requests were in flight (the
      // person clicked back to Simulated); applying a stale live snapshot
      // on top of a freshly reset simulation would be a visible glitch.
      if (state.mode !== 'live') return;

      state.pending = pendingJson.jobs.map(j => ({ id: j.id.slice(0, 8), type: j.type }));
      state.completed = completedJson.count;
      state.dead = deadJson.count;
      state.retried = runningJson.count; // relabelled "Running" in live mode
      state.reclaimed = workersJson.summary.alive; // relabelled "Alive workers"
      state.workers = workersJson.workers.map(w => ({
        name: `${w.hostname}-${w.pid}`,
        status: !w.alive ? 'dead' : w.active_jobs > 0 ? 'busy' : 'idle',
        live: true,
        liveActiveJobs: w.active_jobs,
        liveConcurrency: w.concurrency,
      }));

      render();
    } catch (err) {
      log('ERROR', `live poll failed: ${err.message}`);
    }
  }

  async function liveSubmitOne(type) {
    const body = type === 'flaky'
      ? { type: 'flaky', payload: {}, max_retries: 2 }
      : { type: 'sleep', payload: { seconds: 1 + Math.random() * 2 } };

    const res = await fetch(`${API_BASE_URL}/api/v1/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`POST /api/v1/jobs -> HTTP ${res.status}`);
    }
    return res.json();
  }

  async function liveSubmit(type, count) {
    try {
      const created = await Promise.all(Array.from({ length: count }, () => liveSubmitOne(type)));
      log('INFO', `job created (live)&nbsp; type=${type} count=${count} job_id=${created[0].id.slice(0, 8)}${count > 1 ? '…' : ''}`);
      livePoll();
    } catch (err) {
      log('ERROR', `live submit failed: ${err.message}`);
    }
  }

  // --------------------------------------------------------------- wiring

  document.getElementById('btn-submit-1').addEventListener('click', () => {
    if (state.mode === 'live') liveSubmit('sleep', 1); else submitJob('sleep');
  });
  document.getElementById('btn-submit-10').addEventListener('click', () => {
    if (state.mode === 'live') liveSubmit('sleep', 10); else submitBatch(10);
  });
  document.getElementById('btn-flaky').addEventListener('click', () => {
    if (state.mode === 'live') liveSubmit('flaky', 1); else submitJob('flaky');
  });
  resetBtn.addEventListener('click', () => {
    if (state.mode === 'live') livePoll(); else resetAll();
  });

  modeSimBtn.addEventListener('click', () => {
    if (state.mode !== 'simulated') exitLiveMode();
  });
  modeLiveBtn.addEventListener('click', () => {
    if (state.mode !== 'live') connectLive();
  });

  el.workersGrid.addEventListener('click', (e) => {
    if (state.mode !== 'simulated') return;
    const target = e.target;
    if (!(target instanceof HTMLElement)) return;
    if (target.dataset.kill !== undefined) killWorker(Number(target.dataset.kill));
    if (target.dataset.restart !== undefined) restartWorker(Number(target.dataset.restart));
  });

  initWorkers();
  log('INFO', 'simulation ready — 3 workers online, 0 jobs pending');
  render();
  setInterval(tick, TICK_MS);
})();
