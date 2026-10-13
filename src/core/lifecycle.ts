import { ClosedError, NotStartedError } from './errors.js';

export type LifecycleState = 'created' | 'starting' | 'running' | 'closing' | 'closed';

/** Tracks the client state and guards operations that require a running instance. */
export class Lifecycle {
  private current: LifecycleState = 'created';

  get state(): LifecycleState {
    return this.current;
  }

  transition(to: LifecycleState): void {
    this.current = to;
  }

  assertRunning(): void {
    if (this.current === 'running') return;
    if (this.current === 'closing' || this.current === 'closed') throw new ClosedError();
    throw new NotStartedError();
  }
}
