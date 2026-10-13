/** Map of topic names to payload types, for typed events. */
export type EventMap = Record<string, unknown>;

/** An event delivered to subscribers. */
export interface FluxbaseEvent<T = unknown> {
  /** Unique id of this event. */
  readonly id: string;
  readonly topic: string;
  readonly data: T;
  /** Publish time, epoch ms. */
  readonly timestamp: number;
  /** Groups related events, for example one request. Generated when not supplied. */
  readonly correlationId: string;
  /** Instance id of the process that published the event. */
  readonly source: string;
}

export type EventHandler<T = unknown> = (event: FluxbaseEvent<T>) => void | Promise<void>;

/**
 * Delivery scope.
 * - `local`: only subscribers in this process. Always available. Not shared across processes.
 * - `broadcast`: also delivered to other processes through the storage backend.
 *   Redis Pub/Sub only, in distributed mode. This is fire-and-forget: a process
 *   that is offline or not subscribed at the time of publishing never receives it.
 */
export type EventScope = 'local' | 'broadcast';

export interface PublishOptions {
  correlationId?: string;
  /** Defaults to `local`. */
  scope?: EventScope;
}

export const TOPIC_MAX_LENGTH = 200;
export const TOPIC_PATTERN = /^[A-Za-z0-9_.:-]+$/;
