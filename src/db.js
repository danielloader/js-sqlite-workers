// SQLite connection factory — each worker thread must open its own connection.
// better-sqlite3 supports this pattern; WAL mode allows concurrent readers
// alongside a single writer, and busy_timeout handles write contention.

import Database from 'better-sqlite3';
import { SQLITE_PATH } from './config.js';

export function openDb({ readonly = false } = {}) {
  const db = new Database(SQLITE_PATH, { readonly });
  db.pragma('journal_mode = WAL');    // allow concurrent reads during writes
  db.pragma('busy_timeout = 5000');   // retry on SQLITE_BUSY for up to 5s
  db.pragma('synchronous = NORMAL');  // safe with WAL, avoids fsync per tx
  db.pragma('cache_size = -64000');   // 64MB page cache
  return db;
}

export function initSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS work_queue (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_id INTEGER NOT NULL,
      payload TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      created_at TEXT DEFAULT (datetime('now')),
      processed_at TEXT,
      result_1_body TEXT,
      result_1_status INTEGER,
      result_1_duration_ms REAL,
      result_2_body TEXT,
      result_2_status INTEGER,
      result_2_duration_ms REAL,
      result_3_body TEXT,
      result_3_status INTEGER,
      result_3_duration_ms REAL
    );
    CREATE INDEX IF NOT EXISTS idx_work_queue_status ON work_queue(status);
  `);
}
