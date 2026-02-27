// Main thread orchestrator — sets up the SQLite database, spawns a producer
// worker (PG → SQLite) and N consumer workers (SQLite → stdout), then polls
// for progress until all work is drained. Communication is via postMessage.

import { Worker } from 'node:worker_threads';
import { unlinkSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { openDb, initSchema } from './db.js';
import { SQLITE_PATH, NUM_CONSUMERS, PG_PAGE_SIZE, ROW_LIMIT, MAX_DURATION, PROGRESS_INTERVAL_MS, HTTPBIN_URL } from './config.js';
import { createLogger } from './logger.js';

const log = createLogger('main');

const __dirname = dirname(fileURLToPath(import.meta.url));

// Clean slate — delete any leftover .db / -wal / -shm from a previous run
for (const suffix of ['', '-wal', '-shm']) {
  const file = SQLITE_PATH + suffix;
  if (existsSync(file)) unlinkSync(file);
}

// Create schema then immediately close — each worker opens its own connection
const db = openDb();
initSchema(db);
db.close();

const pipelineStart = performance.now();
log.info({ consumers: NUM_CONSUMERS, batchSize: PG_PAGE_SIZE, limit: ROW_LIMIT || 'none', maxDuration: MAX_DURATION || 'none' }, 'starting pipeline');

// Spawn producer
const producer = new Worker(join(__dirname, 'producer.js'), {
  workerData: { pageSize: PG_PAGE_SIZE, limit: ROW_LIMIT },
});
let producerDone = false;
let totalInserted = 0;

// Spawn consumers
const consumers = [];
const consumersDone = new Set();

// --- Shutdown coordination ---
let shuttingDown = false;
let deadlineTimer;
let drainTimeout;

function printSummary() {
  const elapsed = performance.now() - pipelineStart;

  const summaryDb = openDb({ readonly: true });
  const statusRows = summaryDb
    .prepare('SELECT status, COUNT(*) as count FROM work_queue GROUP BY status')
    .all();
  const counts = Object.fromEntries(statusRows.map((r) => [r.status, r.count]));
  const total = Object.values(counts).reduce((a, b) => a + b, 0);

  const rows = summaryDb
    .prepare(
      `SELECT result_1_duration_ms, result_2_duration_ms, result_3_duration_ms
       FROM work_queue WHERE status = 'done'`
    )
    .all();
  summaryDb.close();

  // --- stats helpers ---
  function pct(sorted, p) {
    if (sorted.length === 0) return 0;
    return sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)];
  }

  function calcStats(values) {
    const s = values.filter((v) => v != null).sort((a, b) => a - b);
    if (s.length === 0) return null;
    return {
      avg: s.reduce((a, b) => a + b, 0) / s.length,
      min: s[0],
      med: pct(s, 0.5),
      max: s[s.length - 1],
      p90: pct(s, 0.9),
      p95: pct(s, 0.95),
    };
  }

  // --- formatting helpers ---
  const DOT_WIDTH = 36;

  function dot(name) {
    return `  ${name} ${'·'.repeat(Math.max(1, DOT_WIDTH - name.length))}`;
  }

  function ms(v) {
    if (v == null) return '-';
    if (v >= 60_000) return `${(v / 60_000).toFixed(1)}m`;
    if (v >= 1_000) return `${(v / 1_000).toFixed(2)}s`;
    return `${Math.round(v)}ms`;
  }

  function statsLine(name, stats) {
    if (!stats) return `${dot(name)} -`;
    return (
      `${dot(name)} ` +
      `avg=${ms(stats.avg).padEnd(9)} ` +
      `min=${ms(stats.min).padEnd(9)} ` +
      `med=${ms(stats.med).padEnd(9)} ` +
      `max=${ms(stats.max).padEnd(9)} ` +
      `p(90)=${ms(stats.p90).padEnd(9)} ` +
      `p(95)=${ms(stats.p95)}`
    );
  }

  // --- build output ---
  const stats1 = calcStats(rows.map((r) => r.result_1_duration_ms));
  const stats2 = calcStats(rows.map((r) => r.result_2_duration_ms));
  const stats3 = calcStats(rows.map((r) => r.result_3_duration_ms));
  const statsAll = calcStats(
    rows.flatMap((r) => [r.result_1_duration_ms, r.result_2_duration_ms, r.result_3_duration_ms])
  );

  const lines = [
    '',
    '  PIPELINE SUMMARY',
    '',
    `${dot('rows_done')} ${counts.done ?? 0}`,
    `${dot('rows_failed')} ${counts.failed ?? 0}`,
    `${dot('rows_total')} ${total}`,
    '',
  ];

  if (stats1) {
    lines.push(
      statsLine('http_call_1_duration', stats1),
      statsLine('http_call_2_duration', stats2),
      statsLine('http_call_3_duration', stats3),
      statsLine('http_call_duration', statsAll),
      '',
    );
  }

  lines.push(
    `${dot('consumers')} ${NUM_CONSUMERS}`,
    `${dot('pipeline_duration')} ${ms(elapsed)}`,
    '',
  );

  process.stdout.write(lines.join('\n') + '\n');
}

async function shutdown(code) {
  if (shuttingDown) return;
  shuttingDown = true;

  clearInterval(progressInterval);
  clearTimeout(deadlineTimer);
  clearTimeout(drainTimeout);

  // Terminate all consumer workers
  await Promise.allSettled(consumers.map((w) => w.terminate()));

  // Reset any rows stuck in 'processing' back to 'pending'
  try {
    const cleanupDb = openDb();
    const resetResult = cleanupDb.prepare(
      `UPDATE work_queue SET status = 'pending', processed_at = NULL WHERE status = 'processing'`
    ).run();
    if (resetResult.changes > 0) {
      log.warn({ count: resetResult.changes }, 'reset orphaned processing rows to pending');
    }
    cleanupDb.close();
  } catch (err) {
    log.error({ err }, 'failed to reset orphaned rows');
  }

  try {
    printSummary();
  } catch {
    // DB may not exist if shutdown happens very early
  }

  monitorDb.close();
  process.exit(code);
}

for (let i = 0; i < NUM_CONSUMERS; i++) {
  const worker = new Worker(join(__dirname, 'consumer.js'), { workerData: { id: i + 1, httpbinUrl: HTTPBIN_URL } });
  const threadId = worker.threadId; // capture before exit resets it to -1
  consumers.push(worker);

  worker.on('message', (msg) => {
    if (msg.type === 'consumer_done') {
      consumersDone.add(threadId);
      if (consumersDone.size === consumers.length) {
        shutdown(0);
      }
    }
  });

  worker.on('error', (err) => {
    log.error({ err, threadId }, 'consumer error');
  });

  worker.on('exit', (code) => {
    if (code !== 0 && !shuttingDown && !consumersDone.has(threadId)) {
      log.error({ threadId, exitCode: code }, 'consumer exited with non-zero code');
      consumersDone.add(threadId);
      if (consumersDone.size === consumers.length) {
        shutdown(1);
      }
    }
  });
}

// Producer message handling
producer.on('message', (msg) => {
  if (msg.type === 'batch_inserted') {
    totalInserted += msg.count;
  }
  if (msg.type === 'producer_done') {
    producerDone = true;
    totalInserted = msg.totalInserted;
    // Relay to consumers so they know no more work is coming and can
    // begin their drain-detection logic (3 consecutive empty polls)
    for (const c of consumers) {
      c.postMessage({ type: 'producer_done' });
    }
  }
});

producer.on('error', (err) => {
  log.fatal({ err }, 'producer error');
  shutdown(1);
});

// Progress polling
const monitorDb = openDb({ readonly: true });
const statusQuery = monitorDb.prepare(`
  SELECT status, COUNT(*) as count FROM work_queue GROUP BY status
`);

const progressInterval = setInterval(() => {
  if (shuttingDown) return;
  const rows = statusQuery.all();
  const stats = Object.fromEntries(rows.map((r) => [r.status, r.count]));
  const total = Object.values(stats).reduce((a, b) => a + b, 0);
  const done = stats.done ?? 0;
  const pending = stats.pending ?? 0;
  const processing = stats.processing ?? 0;
  const failed = stats.failed ?? 0;
  log.info({ total, done, pending, inFlight: processing, failed }, 'progress');
}, PROGRESS_INTERVAL_MS);

// Deadline timer — graceful drain after --max-duration seconds
if (MAX_DURATION > 0) {
  deadlineTimer = setTimeout(() => {
    log.warn({ maxDuration: MAX_DURATION }, 'max duration reached, draining consumers');
    for (const c of consumers) {
      c.postMessage({ type: 'drain' });
    }
    // Safety net: force shutdown if consumers don't finish within 30s
    drainTimeout = setTimeout(() => {
      log.warn('drain timeout exceeded, forcing shutdown');
      shutdown(0);
    }, 30_000);
  }, MAX_DURATION * 1000);
}
