/*
 * TaskForge — browser-side simulation of the claim / lease / fencing /
 * reaper mechanics described in the real Go backend. Nothing here talks to
 * a server; it exists so the core reliability story can be demonstrated
 * from any machine with just a browser tab.
 */
(() => {
  'use strict';

  const LEASE_MS = 5000;      // simulated lease duration
  const TICK_MS = 100;        // render/physics tick
  const MIN_JOB_MS = 2200;
  const MAX_JOB_MS = 4200;
  const MAX_LOG_LINES = 60;

  const WORKER_COUNT = 3;

  /** @typedef {{id:string, type:'sleep'|'flaky', attempts:number, maxRetries:number}} Job */

  const state = {
    pending: /** @type {Job[]} */ ([]),
    workers: /** @type {any[]} */ ([]),
    completed: 0,
    retried: 0,
    dead: 0,
    reclaimed: 0,
    log: /** @type {string[]} */ ([]),
  };

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
    el.workersGrid.innerHTML = state.workers.map((w, i) => {
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

  // --------------------------------------------------------------- wiring

  document.getElementById('btn-submit-1').addEventListener('click', () => submitJob('sleep'));
  document.getElementById('btn-submit-10').addEventListener('click', () => submitBatch(10));
  document.getElementById('btn-flaky').addEventListener('click', () => submitJob('flaky'));
  document.getElementById('btn-reset').addEventListener('click', resetAll);

  el.workersGrid.addEventListener('click', (e) => {
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
