import { SerializationError, ValidationError } from '../core/errors.js';

/**
 * Serializes a JSON-compatible value to text. Rejects values that JSON cannot
 * represent faithfully (functions, symbols, bigints, cycles, undefined) and
 * values larger than `maxBytes`.
 */
export function serialize(value: unknown, maxBytes: number, what = 'value'): string {
  if (value === undefined) {
    throw new SerializationError(`${what} must not be undefined. Use null for an empty value.`);
  }
  let text: string | undefined;
  try {
    text = JSON.stringify(value);
  } catch (cause) {
    throw new SerializationError(`${what} is not JSON-serializable: ${(cause as Error).message}`, cause);
  }
  if (text === undefined) {
    throw new SerializationError(`${what} is not JSON-serializable (functions and symbols cannot be stored).`);
  }
  const bytes = Buffer.byteLength(text, 'utf8');
  if (bytes > maxBytes) {
    throw new ValidationError(`${what} is ${bytes} bytes, which exceeds the ${maxBytes} byte limit.`);
  }
  return text;
}

/**
 * Parses text produced by {@link serialize}. Only JSON is accepted; no code is
 * ever evaluated. Malformed content raises a SerializationError rather than
 * being returned as data.
 */
export function deserialize<T = unknown>(text: string, what = 'value'): T {
  try {
    return JSON.parse(text) as T;
  } catch (cause) {
    throw new SerializationError(`Stored ${what} is not valid JSON.`, cause);
  }
}
