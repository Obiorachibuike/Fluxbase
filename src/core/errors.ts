/**
 * Stable, machine-readable error codes. Match on `error.code`, not on messages.
 */
export type FluxbaseErrorCode =
  | 'CONFIG_INVALID'
  | 'NOT_STARTED'
  | 'CLOSED'
  | 'STORAGE_ERROR'
  | 'REDIS_CONNECTION'
  | 'JOB_NOT_FOUND'
  | 'JOB_EXISTS'
  | 'INVALID_STATE'
  | 'SERIALIZATION'
  | 'VALIDATION'
  | 'UNSUPPORTED'
  | 'LEASE_LOST'
  | 'TIMEOUT'
  | 'UNRECOVERABLE';

/** Base class for every error thrown by Fluxbase. */
export class FluxbaseError extends Error {
  readonly code: FluxbaseErrorCode;

  constructor(code: FluxbaseErrorCode, message: string, options?: { cause?: unknown }) {
    super(redactSecrets(message), options?.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = new.target.name;
    this.code = code;
  }
}

export class ConfigError extends FluxbaseError {
  constructor(message: string) {
    super('CONFIG_INVALID', message);
  }
}

export class ValidationError extends FluxbaseError {
  constructor(message: string) {
    super('VALIDATION', message);
  }
}

export class NotStartedError extends FluxbaseError {
  constructor() {
    super('NOT_STARTED', 'Fluxbase is not started. Call `await fluxbase.start()` before using queues, storage, or workers.');
  }
}

export class ClosedError extends FluxbaseError {
  constructor() {
    super('CLOSED', 'Fluxbase has been closed and can no longer be used. Create a new instance instead.');
  }
}

export class StorageError extends FluxbaseError {
  constructor(message: string, cause?: unknown) {
    super('STORAGE_ERROR', message, { cause });
  }
}

export class RedisConnectionError extends FluxbaseError {
  constructor(message: string, cause?: unknown) {
    super('REDIS_CONNECTION', message, { cause });
  }
}

export class JobNotFoundError extends FluxbaseError {
  constructor(queue: string, id: string) {
    super('JOB_NOT_FOUND', `Job "${id}" was not found in queue "${queue}".`);
  }
}

export class DuplicateJobError extends FluxbaseError {
  constructor(queue: string, id: string) {
    super('JOB_EXISTS', `A job with id "${id}" already exists in queue "${queue}". Use a different jobId, or remove the existing job first.`);
  }
}

export class InvalidStateError extends FluxbaseError {
  constructor(message: string) {
    super('INVALID_STATE', message);
  }
}

export class SerializationError extends FluxbaseError {
  constructor(message: string, cause?: unknown) {
    super('SERIALIZATION', message, { cause });
  }
}

export class UnsupportedCapabilityError extends FluxbaseError {
  constructor(message: string) {
    super('UNSUPPORTED', message);
  }
}

export class LeaseLostError extends FluxbaseError {
  constructor(id: string) {
    super('LEASE_LOST', `The lease for job "${id}" was lost (it expired or was recovered by another worker). The result of this attempt was discarded.`);
  }
}

export class JobTimeoutError extends FluxbaseError {
  constructor(id: string, timeoutMs: number) {
    super('TIMEOUT', `Job "${id}" did not finish within ${timeoutMs} ms.`);
  }
}

/**
 * Throw from a processor to fail a job immediately without further retries.
 */
export class UnrecoverableError extends FluxbaseError {
  constructor(message = 'Unrecoverable job failure') {
    super('UNRECOVERABLE', message);
  }
}

const URL_CREDENTIALS = /([a-z][a-z0-9+.-]*:\/\/)[^/\s@]+@/gi;
const SECRET_ASSIGNMENTS =
  /\b(password|passwd|pwd|secret|token|api[_-]?key|authorization|auth)(["']?\s*[=:]\s*["']?)[^\s"'&,;]+/gi;

/**
 * Masks credentials in free text: `user:password@` in URLs and values assigned to
 * password-like names (`password=...`, `token: ...`). Applied to every log line
 * and to every failure reason stored on a job.
 */
export function redactSecrets(text: string): string {
  return text.replace(URL_CREDENTIALS, '$1***@').replace(SECRET_ASSIGNMENTS, '$1$2***');
}

/** Returns a copy of a URL string with credentials removed, safe for logs. */
export function redactUrl(url: string): string {
  return redactSecrets(url);
}
