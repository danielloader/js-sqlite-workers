// Main thread orchestrator — sets up the SQLite database, spawns a producer
// worker (PG → SQLite) and N consumer workers (SQLite → stdout), then polls
// for progress until all work is drained. Communication is via postMessage.

import { Worker } from 'node:worker_threads';
import { unlinkSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { openDb, initSchema } from './db.js';
import { SQLITE_PATH, NUM_CONSUMERS, PG_PAGE_SIZE, ROW_LIMIT, PROGRESS_INTERVAL_MS, HTTPBIN_URL } from './config.js';
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

log.info({ consumers: NUM_CONSUMERS, batchSize: PG_PAGE_SIZE, limit: ROW_LIMIT || 'none' }, 'starting pipeline');

// Spawn producer
const producer = new Worker(join(__dirname, 'producer.js'), {
  workerData: { pageSize: PG_PAGE_SIZE, limit: ROW_LIMIT },
});
let producerDone = false;
let totalInserted = 0;

// Spawn consumers
const consumers = [];
const consumersDone = new Set();

for (let i = 0; i < NUM_CONSUMERS; i++) {
  const worker = new Worker(join(__dirname, 'consumer.js'), { workerData: { id: i + 1, httpbinUrl: HTTPBIN_URL } });
  consumers.push(worker);

  worker.on('message', (msg) => {
    if (msg.type === 'consumer_done') {
      consumersDone.add(worker.threadId);
      if (consumersDone.size === consumers.length) {
        onAllDone();
      }
    }
  });

  worker.on('error', (err) => {
    log.error({ err, threadId: worker.threadId }, 'consumer error');
  });

  worker.on('exit', (code) => {
    if (code !== 0 && !consumersDone.has(worker.threadId)) {
      log.error({ threadId: worker.threadId, exitCode: code }, 'consumer exited with non-zero code');
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
  process.exit(1);
});

// Progress polling
const monitorDb = openDb({ readonly: true });
const statusQuery = monitorDb.prepare(`
  SELECT status, COUNT(*) as count FROM work_queue GROUP BY status
`);

const progressInterval = setInterval(() => {
  const rows = statusQuery.all();
  const stats = Object.fromEntries(rows.map((r) => [r.status, r.count]));
  const total = Object.values(stats).reduce((a, b) => a + b, 0);
  const done = stats.done ?? 0;
  const pending = stats.pending ?? 0;
  const processing = stats.processing ?? 0;
  const failed = stats.failed ?? 0;
  log.info({ total, done, pending, inFlight: processing, failed }, 'progress');
}, PROGRESS_INTERVAL_MS);

function onAllDone() {
  clearInterval(progressInterval);
  monitorDb.close();

  const finalDb = openDb({ readonly: true });
  const finalStats = finalDb
    .prepare('SELECT status, COUNT(*) as count FROM work_queue GROUP BY status')
    .all();
  finalDb.close();

  const report = Object.fromEntries(finalStats.map((r) => [r.status, r.count]));
  log.info(report, 'final report');

  process.exit(0);
}
