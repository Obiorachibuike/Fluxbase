/**
 * Child-process fixture for reliability tests. It starts a Fluxbase client with the
 * options passed as JSON in argv[2], then processes the "crash" queue. Each claimed
 * job prints `STARTED <id>` and then hangs, so the parent can kill it mid-job.
 */
import { Fluxbase } from '../../src/index.js';

const options = JSON.parse(process.argv[2] ?? '{}') as ConstructorParameters<typeof Fluxbase>[0];
const fb = new Fluxbase(options);
await fb.start();
fb.queue('crash').process(async (job) => {
  process.stdout.write(`STARTED ${job.id}\n`);
  await new Promise(() => undefined); // never finishes
});
