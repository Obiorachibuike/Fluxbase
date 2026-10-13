// Embedded mode (the default). No Redis needed. Data lives in ./data/fluxbase.db.
import { Fluxbase } from 'fluxbase';

interface WelcomeEmail {
  to: string;
}

async function main(): Promise<void> {
  const fb = new Fluxbase({ storage: { type: 'sqlite', path: './data/quickstart.db' } });
  await fb.start();

  const emails = fb.queue<WelcomeEmail, string>('emails');
  const worker = emails.process(async (job) => {
    console.log(`sending welcome email to ${job.data.to}`);
    return `sent:${job.data.to}`;
  });

  await emails.add('welcome', { to: 'ada@example.com' });

  // Give the worker a moment, then shut down cleanly.
  await new Promise((resolve) => setTimeout(resolve, 200));
  await worker.stop();
  await fb.close();
}

void main();
