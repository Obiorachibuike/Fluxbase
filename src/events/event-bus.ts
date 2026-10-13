import { UnsupportedCapabilityError, ValidationError, redactSecrets } from '../core/errors.js';
import type { Logger } from '../observability/logger.js';
import type { StorageAdapter } from '../storage/adapter.js';
import { newId } from '../utils/ids.js';
import { deserialize, serialize } from '../utils/serialization.js';
import {
  type EventHandler,
  type EventMap,
  type EventScope,
  type FluxbaseEvent,
  type PublishOptions,
  TOPIC_MAX_LENGTH,
  TOPIC_PATTERN,
} from './event.js';

interface WireEvent {
  id: string;
  topic: string;
  data: string;
  timestamp: number;
  correlationId: string;
  source: string;
}

function assertTopic(topic: unknown): asserts topic is string {
  if (typeof topic !== 'string' || topic.length === 0 || topic.length > TOPIC_MAX_LENGTH || !TOPIC_PATTERN.test(topic)) {
    throw new ValidationError(
      `Event topics must be 1-${TOPIC_MAX_LENGTH} characters from A-Z, a-z, 0-9, "_", ".", ":", "-".`,
    );
  }
}

/**
 * In-process publish/subscribe with optional cross-process broadcast.
 *
 * Delivery rules:
 * - Handlers for a topic run concurrently. `publish()` resolves after all local
 *   handlers settle.
 * - A handler that throws or rejects does not affect the publisher or the other
 *   handlers. The error is logged and the event is not retried.
 * - Local events are not persisted. They are lost if the process exits before delivery.
 * - Broadcast events use Redis Pub/Sub. They are NOT durable. Use queues for work that must be delivered.
 */
export class EventBus<TEvents extends EventMap = EventMap> {
  private readonly handlers = new Map<string, Set<EventHandler<unknown>>>();
  private readonly remote = new Map<string, () => void>();
  private closed = false;

  constructor(
    private readonly storage: StorageAdapter,
    private readonly instanceId: string,
    private readonly logger: Logger,
    private readonly maxValueBytes: number,
    private readonly now: () => number = Date.now,
  ) {}

  /**
   * Subscribes to `topic`. Returns an unsubscribe function. Subscribing to the same handler twice has no extra effect.
   */
  subscribe<K extends keyof TEvents & string>(topic: K, handler: EventHandler<TEvents[K]>): () => void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  subscribe(topic: string, handler: EventHandler<any>): () => void {
    assertTopic(topic);
    if (typeof handler !== 'function') throw new ValidationError('handler must be a function.');
    let set = this.handlers.get(topic);
    if (!set) {
      set = new Set();
      this.handlers.set(topic, set);
      void this.attachRemote(topic);
    }
    set.add(handler as EventHandler<unknown>);
    return () => {
      const current = this.handlers.get(topic);
      if (!current || !current.delete(handler as EventHandler<unknown>)) return;
      if (current.size === 0) {
        this.handlers.delete(topic);
        this.detachRemote(topic);
      }
    };
  }

  /**
   * Publishes an event to local subscribers and, with `scope: "broadcast"`, to other processes.
   * Resolves with the delivered event once local handlers have settled.
   */
  async publish<K extends keyof TEvents & string>(
    topic: K,
    data: TEvents[K],
    options?: PublishOptions,
  ): Promise<FluxbaseEvent<TEvents[K]>>;
  async publish(topic: string, data: unknown, options: PublishOptions = {}): Promise<FluxbaseEvent<unknown>> {
    assertTopic(topic);
    if (this.closed) throw new ValidationError('Cannot publish on a closed event bus.');
    const scope: EventScope = options.scope ?? 'local';
    if (scope !== 'local' && scope !== 'broadcast') {
      throw new ValidationError('scope must be "local" or "broadcast".');
    }
    if (scope === 'broadcast' && !this.storage.capabilities.broadcastEvents) {
      throw new UnsupportedCapabilityError(
        'Broadcast events require distributed mode with Redis. Embedded and memory modes deliver events within the current process only. Use scope "local" or switch to distributed mode.',
      );
    }
    if (options.correlationId !== undefined && (options.correlationId.length === 0 || options.correlationId.length > 200)) {
      throw new ValidationError('correlationId must be 1-200 characters.');
    }
    const text = serialize(data, this.maxValueBytes, 'Event data');
    const event: FluxbaseEvent<unknown> = {
      id: newId(),
      topic,
      data: deserialize(text, 'event data'),
      timestamp: this.now(),
      correlationId: options.correlationId ?? newId(),
      source: this.instanceId,
    };

    const delivery = this.deliverLocal(event);
    if (scope === 'broadcast') {
      const wire: WireEvent = {
        id: event.id,
        topic,
        data: text,
        timestamp: event.timestamp,
        correlationId: event.correlationId,
        source: this.instanceId,
      };
      await this.storage.publishBroadcast!(topic, JSON.stringify(wire));
    }
    await delivery;
    return event as FluxbaseEvent<unknown>;
  }

  /** Removes all local handlers and cross-process subscriptions. Called by Fluxbase.close(). */
  async close(): Promise<void> {
    this.closed = true;
    this.handlers.clear();
    for (const unsubscribe of this.remote.values()) unsubscribe();
    this.remote.clear();
  }

  /** Number of local handlers subscribed to `topic`. */
  listenerCount(topic: string): number {
    return this.handlers.get(topic)?.size ?? 0;
  }

  private async deliverLocal(event: FluxbaseEvent<unknown>): Promise<void> {
    const set = this.handlers.get(event.topic);
    if (!set || set.size === 0) return;
    const handlers = [...set];
    await Promise.all(
      handlers.map(async (handler) => {
        try {
          await handler(event);
        } catch (error) {
          this.logger.error('Event handler failed', {
            topic: event.topic,
            eventId: event.id,
            error: error as Error,
          });
        }
      }),
    );
  }

  private async attachRemote(topic: string): Promise<void> {
    if (!this.storage.capabilities.broadcastEvents || this.remote.has(topic) || this.closed) return;
    try {
      const unsubscribe = await this.storage.subscribeBroadcast!(topic, (message) => this.onRemote(message));
      if (this.closed || !this.handlers.has(topic)) {
        unsubscribe();
      } else {
        this.remote.set(topic, unsubscribe);
      }
    } catch (error) {
      this.logger.warn('Failed to subscribe to broadcast events', {
        topic,
        error: redactSecrets((error as Error).message),
      });
    }
  }

  private detachRemote(topic: string): void {
    const unsubscribe = this.remote.get(topic);
    if (unsubscribe) {
      this.remote.delete(topic);
      unsubscribe();
    }
  }

  private onRemote(message: string): void {
    let wire: WireEvent;
    try {
      wire = JSON.parse(message) as WireEvent;
    } catch {
      this.logger.warn('Ignored malformed broadcast event');
      return;
    }
    // The publishing process already delivered this event locally.
    if (wire.source === this.instanceId) return;
    let data: unknown;
    try {
      data = deserialize(wire.data, 'event data');
    } catch {
      this.logger.warn('Ignored broadcast event with malformed data', { topic: wire.topic });
      return;
    }
    void this.deliverLocal({
      id: wire.id,
      topic: wire.topic,
      data,
      timestamp: wire.timestamp,
      correlationId: wire.correlationId,
      source: wire.source,
    });
  }
}

