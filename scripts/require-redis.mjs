#!/usr/bin/env node
// Fails fast when the Redis test run has no Redis URL, so a CI job cannot pass by
// silently skipping every distributed test. The URL value is never printed.
if (!process.env.REDIS_URL) {
  console.error('REDIS_URL is not set. The distributed test suite needs a real Redis, for example redis://127.0.0.1:6379.');
  process.exit(1);
}
console.log('REDIS_URL is set (value not shown). Running the full suite including real-Redis tests.');
