// Typed local events. The event map makes unknown topics a compile-time error.
import { Fluxbase } from 'fluxbase';

type AppEvents = {
  'user.registered': { userId: string; email: string };
  'order.paid': { orderId: string; amountCents: number };
};

async function main(): Promise<void> {
  const fb = new Fluxbase<AppEvents>({ storage: { type: 'sqlite', path: './data/events.db' } });
  await fb.start();

  // subscribe() returns an unsubscribe function.
  const unsubscribe = fb.events.subscribe('user.registered', (event) => {
    console.log('welcome', event.data.email, 'from', event.source);
  });

  fb.events.subscribe('order.paid', (event) => {
    console.log('order', event.data.orderId, event.data.amountCents);
  });

  // A handler that throws is isolated: the publisher and other handlers are not affected.
  fb.events.subscribe('order.paid', () => {
    throw new Error('analytics is down');
  });

  await fb.events.publish('user.registered', { userId: 'u-1', email: 'ada@example.com' });
  await fb.events.publish('order.paid', { orderId: 'o-9', amountCents: 4_200 });

  unsubscribe();
  await fb.close();
}

void main();
