# Changelog

## 0.1.0 (unreleased)

First release candidate. Not published to npm.

- Embedded mode (SQLite, default), distributed mode (Redis, Lua atomic transitions), and volatile memory mode (explicit opt-in).
- Job queue: priority, delays, retries with fixed or exponential backoff, `UnrecoverableError`, timeouts, pause/resume, stats, retention, graceful shutdown, lease-based crash recovery.
- Cron schedules with IANA timezones, coalesced missed runs, multi-instance safety.
- Key-value store and cache with TTL in seconds.
- Local event bus with typed topics; Redis Pub/Sub broadcast (non-durable) in distributed mode.
- Dual ESM and CommonJS build with TypeScript declarations.
