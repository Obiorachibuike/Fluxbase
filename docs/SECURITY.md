# Security

## Serialization

Job data, metadata, KV values, cache values, and event payloads are **JSON only**. Fluxbase never calls `eval`, `new Function`, or any deserializer that can construct arbitrary objects. Stored text is parsed with `JSON.parse`. Values that JSON cannot represent (`undefined`, functions, symbols, bigints, cycles) are rejected with `SerializationError`.

Size limits are enforced before writes: job data and KV/cache values default to 1 MiB (`kv.maxValueBytes`, max 64 MiB), metadata to 16 KiB.

## Secrets and logs

- Credentials in URLs (`scheme://user:password@host`) are replaced with `***` in every error message and log line.
- Free-text values assigned to password-like names (`password=`, `token:`, `secret=`, `api_key=`, `authorization=`) are masked in error text, including failure reasons stored on jobs.
- Log metadata keys that look like payloads or secrets (`data`, `payload`, `url`, `dsn`, names containing `pass`, `secret`, `token`, `auth`, `cookie`, `credential`) are replaced with `[redacted]`.
- Failure reasons are stored as `Name: message`, without stack traces.

Redaction is best-effort pattern matching. Do not put secrets in job data or event payloads in the first place.

## Input validation

- Queue names: `[A-Za-z0-9_.-]{1,100}`. Job ids: `[A-Za-z0-9_.:-]{1,200}`. Schedule names: `[A-Za-z0-9_.-]{1,100}`. Event topics: `[A-Za-z0-9_.:-]{1,200}`. KV keys reject control characters and are at most 512 characters.
- Numbers (attempts, delays, priorities, TTLs, timeouts) are range-checked.
- SQL is always parameterised. Redis commands use Lua scripts with `KEYS`/`ARGV` declared explicitly.

## Storage

- SQLite files are created with mode `0700` directories. Keep the database out of web-served directories.
- Redis access control is yours to configure: use a password or ACL user with only the commands and keys prefix Fluxbase needs.
- No secrets are committed. `.gitignore` excludes `.env*` and local data.

## Dependencies

Runtime dependencies are `better-sqlite3`, `ioredis`, and `cron-parser`. Run `npm audit` in your project and keep Node.js on a supported release (22 or later).

## Reporting

Report vulnerabilities privately through GitHub security advisories on `Obiorachibuike/Fluxbase` rather than in public issues.
