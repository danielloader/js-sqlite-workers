// Producer worker thread — reads rows from PostgreSQL in pages and batch-inserts
// them into the SQLite work queue. Each page is wrapped in a single BEGIN
// IMMEDIATE transaction so the insert is atomic and minimises WAL contention.

import { parentPort, workerData } from 'node:worker_threads';
import pg from 'pg';
import { openDb } from './db.js';
import { PG_CONFIG } from './config.js';
import { createLogger } from './logger.js';

const log = createLogger('producer');

const { pageSize: PG_PAGE_SIZE, limit: ROW_LIMIT } = workerData;

const db = openDb();
const client = new pg.Client(PG_CONFIG);

const insertRow = db.prepare(`
  INSERT INTO work_queue (source_id, payload) VALUES (?, ?)
`);

// Wrapping in .transaction() gives us an implicit BEGIN IMMEDIATE / COMMIT
// so the entire page is inserted atomically in one write lock acquisition.
const insertBatch = db.transaction((rows) => {
  for (const row of rows) {
    insertRow.run(row.id, JSON.stringify(row));
  }
});

async function produce() {
  log.info('connecting to PostgreSQL');
  await client.connect();
  log.info({ batchSize: PG_PAGE_SIZE }, 'connected, fetching rows');

  let offset = 0;
  let totalInserted = 0;

  while (true) {
    const { rows } = await client.query(
      'SELECT * FROM people ORDER BY id LIMIT $1 OFFSET $2',
      [PG_PAGE_SIZE, offset]
    );

    if (rows.length === 0) break;

    // Trim the last page if a row limit would be exceeded
    const remaining = ROW_LIMIT > 0 ? ROW_LIMIT - totalInserted : rows.length;
    const batch = remaining < rows.length ? rows.slice(0, remaining) : rows;

    insertBatch(batch);
    totalInserted += batch.length;
    log.info({ batchCount: rows.length, totalInserted }, 'batch inserted');

    parentPort.postMessage({ type: 'batch_inserted', count: batch.length });

    if (ROW_LIMIT > 0 && totalInserted >= ROW_LIMIT) break;
    offset += PG_PAGE_SIZE;
  }

  log.info({ totalInserted }, 'done inserting rows');
  parentPort.postMessage({ type: 'producer_done', totalInserted });

  await client.end();
  db.close();
}

produce().catch((err) => {
  log.fatal({ err }, 'fatal error');
  process.exit(1);
});
