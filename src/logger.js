import pino from 'pino';
import { LOG_LEVEL } from './config.js';

const baseLogger = pino({
  level: LOG_LEVEL,
  transport: {
    target: 'pino-pretty',
    options: {
      singleLine: true,
      ignore: 'pid,hostname',
    },
  },
});

export function createLogger(name) {
  return baseLogger.child({ name });
}
