import { randomUUID } from 'node:crypto';
import { ValidationError } from '../core/errors.js';

/** Generates a random, unique identifier (UUID v4). */
export function newId(): string {
  return randomUUID();
}

const JOB_ID_PATTERN = /^[A-Za-z0-9_.:-]{1,200}$/;
const QUEUE_NAME_PATTERN = /^[A-Za-z0-9_.-]{1,100}$/;
const JOB_NAME_PATTERN = /^[A-Za-z0-9_.:@/ -]{1,200}$/;

/** Hard limits that protect storage and memory from unbounded input. */
export const limits = {
  maxAttempts: 100,
  maxDelayMs: 30 * 24 * 60 * 60 * 1000,
  maxPriority: 1000,
  maxTimeoutMs: 24 * 60 * 60 * 1000,
  maxValueBytes: 1024 * 1024,
  maxMetadataBytes: 16 * 1024,
  maxKeyLength: 512,
  maxTopicLength: 200,
} as const;

export function isValidJobId(id: unknown): id is string {
  return typeof id === 'string' && JOB_ID_PATTERN.test(id);
}

export function isValidQueueName(name: unknown): name is string {
  return typeof name === 'string' && QUEUE_NAME_PATTERN.test(name);
}

export function isValidJobName(name: unknown): name is string {
  return typeof name === 'string' && JOB_NAME_PATTERN.test(name);
}

export function assertQueueName(name: unknown): asserts name is string {
  if (!isValidQueueName(name)) {
    throw new ValidationError(
      'Queue names must be 1-100 characters from A-Z, a-z, 0-9, "_", ".", "-" (colons are not allowed).',
    );
  }
}

export function assertJobId(id: unknown): asserts id is string {
  if (!isValidJobId(id)) {
    throw new ValidationError(
      'Job ids must be 1-200 characters from A-Z, a-z, 0-9, "_", ".", ":", "-".',
    );
  }
}

export function assertJobName(name: unknown): asserts name is string {
  if (!isValidJobName(name)) {
    throw new ValidationError('Job names must be 1-200 characters from A-Z, a-z, 0-9, space, and "_ . : @ / -".');
  }
}
