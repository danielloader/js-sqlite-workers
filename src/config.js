// Centralised configuration — CLI flags for tuning, env vars for PG connection.
// Usage: node src/index.js --consumers 4 --batch-size 100 --limit 0

import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const __dirname = dirname(fileURLToPath(import.meta.url));

const { values: flags } = parseArgs({
  options: {
    consumers: { type: 'string', short: 'c', default: '4' },
    'batch-size': { type: 'string', short: 'b', default: '100' },
    limit: { type: 'string', short: 'l', default: '0' },
  },
  strict: false,
});

export const PG_CONFIG = {
  host: process.env.PG_HOST ?? 'localhost',
  port: parseInt(process.env.PG_PORT ?? '5432', 10),
  user: process.env.PG_USER ?? 'postgres',
  password: process.env.PG_PASSWORD ?? 'postgres',
  database: process.env.PG_DATABASE ?? 'workqueue',
};

export const SQLITE_PATH = join(__dirname, '..', 'work_queue.db');

export const NUM_CONSUMERS = parseInt(flags.consumers, 10);
export const PG_PAGE_SIZE = parseInt(flags['batch-size'], 10);
export const ROW_LIMIT = parseInt(flags.limit, 10);  // 0 = no limit
export const HTTPBIN_URL = process.env.HTTPBIN_URL ?? 'http://localhost:8080';
export const PROGRESS_INTERVAL_MS = 2000;
export const LOG_LEVEL = process.env.LOG_LEVEL ?? 'info';
