import { redactSecrets } from '../core/errors.js';

export type LogLevel = 'silent' | 'error' | 'warn' | 'info' | 'debug';

/** Minimal logger contract. Any object with these four methods can be supplied. */
export interface Logger {
  debug(message: string, meta?: Record<string, unknown>): void;
  info(message: string, meta?: Record<string, unknown>): void;
  warn(message: string, meta?: Record<string, unknown>): void;
  error(message: string, meta?: Record<string, unknown>): void;
}

const LEVEL_RANK: Record<LogLevel, number> = { silent: 0, error: 1, warn: 2, info: 3, debug: 4 };

const SENSITIVE_KEY = /pass|secret|token|auth|cookie|credential|url|dsn|payload|^data$/i;

/**
 * Removes values that must never reach logs. Job payloads (`data`) and
 * connection URLs are dropped; token-like keys are masked.
 */
export function redactMeta(meta: Record<string, unknown> | undefined): Record<string, unknown> | undefined {
  if (!meta) return undefined;
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(meta)) {
    if (SENSITIVE_KEY.test(key)) {
      out[key] = '[redacted]';
    } else if (value instanceof Error) {
      out[key] = { name: value.name, code: (value as { code?: unknown }).code, message: redactSecrets(value.message) };
    } else if (typeof value === 'string') {
      out[key] = redactSecrets(value);
    } else {
      out[key] = value;
    }
  }
  return out;
}

/**
 * Creates the logger used internally. Messages below `level` are dropped. The
 * default sink writes one JSON object per line to stdout/stderr.
 */
export function createLogger(level: LogLevel, custom: Logger | null): Logger {
  const enabled = (lvl: Exclude<LogLevel, 'silent'>) => LEVEL_RANK[level] >= LEVEL_RANK[lvl];
  const sink = (lvl: Exclude<LogLevel, 'silent'>, message: string, meta?: Record<string, unknown>) => {
    if (!enabled(lvl)) return;
    const safe = redactMeta(meta);
    if (custom) {
      custom[lvl](redactSecrets(message), safe);
      return;
    }
    const line = JSON.stringify({ time: new Date().toISOString(), level: lvl, msg: redactSecrets(message), ...safe });
    if (lvl === 'error' || lvl === 'warn') {
      process.stderr.write(line + '\n');
    } else {
      process.stdout.write(line + '\n');
    }
  };
  return {
    debug: (m, meta) => sink('debug', m, meta),
    info: (m, meta) => sink('info', m, meta),
    warn: (m, meta) => sink('warn', m, meta),
    error: (m, meta) => sink('error', m, meta),
  };
}

export const silentLogger: Logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
};
