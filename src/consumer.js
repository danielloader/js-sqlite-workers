// Consumer worker thread — polls the SQLite work queue for pending items using
// an atomic UPDATE...RETURNING inside a BEGIN IMMEDIATE transaction. This
// guarantees no two consumers can claim the same row, even under concurrency.
// Each claimed row triggers 3 parallel HTTP requests to httpbingo's /delay
// endpoint, and the JSON responses are saved back into the SQLite row.

import { parentPort, workerData } from 'node:worker_threads';
import { openDb } from './db.js';
import { createLogger } from './logger.js';

const log = createLogger(`consumer-${workerData.id}`);
const httpbinUrl = workerData.httpbinUrl;
const db = openDb();

// Atomic dequeue: the subquery selects a pending row and the UPDATE claims it
// in a single statement, returning the full row. Wrapped in BEGIN IMMEDIATE
// so the write lock is acquired upfront (no upgrade from read → write).
const dequeue = db.prepare(`
  UPDATE work_queue SET status = 'processing', processed_at = datetime('now')
  WHERE id = (SELECT id FROM work_queue WHERE status = 'pending' LIMIT 1)
  RETURNING *
`);

const markDone = db.prepare(`
  UPDATE work_queue SET status = 'done',
    result_1_body = ?, result_1_status = ?, result_1_duration_ms = ?,
    result_2_body = ?, result_2_status = ?, result_2_duration_ms = ?,
    result_3_body = ?, result_3_status = ?, result_3_duration_ms = ?
  WHERE id = ?
`);
const markFailed = db.prepare(`UPDATE work_queue SET status = 'failed' WHERE id = ?`);

let producerDone = false;
let emptyPolls = 0;

parentPort.on('message', (msg) => {
  if (msg.type === 'producer_done') {
    producerDone = true;
  }
});

const MOCK_CPU_LOAD = process.env.MOCK_CPU_LOAD === 'true';

function cpuBurn() {
  if (!MOCK_CPU_LOAD) return;
  let a = 0n, b = 1n;
  for (let i = 0; i < 50_000; i++) [a, b] = [b, a + b];
}

function randomDelay() {
  return (Math.random() * 0.15 + 0.1).toFixed(2);
}

async function poll() {
  let row;
  try {
    row = db.transaction(() => dequeue.get())();
  } catch (err) {
    // busy_timeout should handle most contention, but if it's exceeded
    // we back off and retry on the next poll cycle
    if (err.code === 'SQLITE_BUSY') {
      log.warn('SQLITE_BUSY, retrying');
      setTimeout(poll, 200);
      return;
    }
    throw err;
  }

  if (!row) {
    emptyPolls++;
    // Only exit once the producer is done AND we've confirmed the queue is
    // empty across 3 consecutive polls (guards against a race where the
    // producer_done message arrives between batches)
    if (producerDone && emptyPolls >= 3) {
      db.close();
      parentPort.postMessage({ type: 'consumer_done' });
      return;
    }
    setTimeout(poll, 200);
    return;
  }

  emptyPolls = 0;

  try {
    const delays = [randomDelay(), randomDelay(), randomDelay()];
    const results = await Promise.all(
      delays.map(async (d) => {
        const start = performance.now();
        const res = await fetch(`${httpbinUrl}/delay/${d}`);
        const duration = performance.now() - start;
        const body = await res.text();
        return { body, status: res.status, duration };
      })
    );

    markDone.run(
      results[0].body, results[0].status, results[0].duration,
      results[1].body, results[1].status, results[1].duration,
      results[2].body, results[2].status, results[2].duration,
      row.id
    );

    cpuBurn();

    const data = JSON.parse(row.payload);
    log.debug({ sourceId: row.source_id, personName: data.name, dept: data.department }, 'processed row');
  } catch (err) {
    log.error({ rowId: row.id, err: err.message }, 'failed to process row');
    markFailed.run(row.id);
  }

  parentPort.postMessage({ type: 'item_processed', sourceId: row.source_id });
  // Work available — schedule next poll immediately (no delay)
  setImmediate(poll);
}

poll();
