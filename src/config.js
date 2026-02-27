// Centralised configuration — CLI flags for tuning, env vars for PG connection.
// Usage: node src/index.js --consumers 4 --batch-size 100 --limit 0 --max-duration 60

import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const __dirname = dirname(fileURLToPath(import.meta.url));

const { values: flags } = parseArgs({
  options: {
    consumers: { type: 'string', short: 'c', default: '4' },
    'batch-size': { type: 'string', short: 'b', default: '100' },
    limit: { type: 'string', short: 'l', default: '0' },
    'max-duration': { type: 'string', short: 't', default: '0' },
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
export const MAX_DURATION = parseInt(flags['max-duration'], 10);  // seconds, 0 = no limit

if (!(NUM_CONSUMERS >= 1)) throw new Error(`--consumers must be >= 1, got: ${flags.consumers}`);
if (!(PG_PAGE_SIZE >= 1)) throw new Error(`--batch-size must be >= 1, got: ${flags['batch-size']}`);
if (!(ROW_LIMIT >= 0)) throw new Error(`--limit must be >= 0, got: ${flags.limit}`);
if (!(MAX_DURATION >= 0)) throw new Error(`--max-duration must be >= 0, got: ${flags['max-duration']}`);

export const HTTPBIN_URL = process.env.HTTPBIN_URL ?? 'http://localhost:8080';
export const PROGRESS_INTERVAL_MS = 2000;
export const LOG_LEVEL = process.env.LOG_LEVEL ?? 'info';
