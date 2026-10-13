/** Schema version stored in `PRAGMA user_version`. Increase when the schema changes. */
export const SCHEMA_VERSION = 1;

export const SCHEMA_V1 = `
CREATE TABLE IF NOT EXISTS jobs (
  queue            TEXT    NOT NULL,
  id               TEXT    NOT NULL,
  name             TEXT    NOT NULL,
  data             TEXT    NOT NULL,
  state            TEXT    NOT NULL CHECK (state IN ('waiting', 'delayed', 'active', 'completed', 'failed')),
  priority         INTEGER NOT NULL,
  attempts         INTEGER NOT NULL,
  attempts_made    INTEGER NOT NULL DEFAULT 0,
  backoff          TEXT,
  timeout_ms       INTEGER,
  created_at       INTEGER NOT NULL,
  run_at           INTEGER NOT NULL,
  processed_at     INTEGER,
  finished_at      INTEGER,
  failed_reason    TEXT,
  return_value     TEXT,
  metadata         TEXT    NOT NULL DEFAULT '{}',
  lock_token       TEXT,
  locked_until     INTEGER,
  worker_id        TEXT,
  remove_on_complete INTEGER NOT NULL DEFAULT 0,
  remove_on_fail     INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (queue, id)
);

-- The implicit rowid provides FIFO ordering within a priority level.
CREATE INDEX IF NOT EXISTS jobs_ready    ON jobs (queue, state, priority DESC);
CREATE INDEX IF NOT EXISTS jobs_delayed  ON jobs (queue, run_at) WHERE state = 'delayed';
CREATE INDEX IF NOT EXISTS jobs_leases   ON jobs (queue, locked_until) WHERE state = 'active';
CREATE INDEX IF NOT EXISTS jobs_finished ON jobs (queue, state, finished_at);

CREATE TABLE IF NOT EXISTS queue_control (
  queue  TEXT PRIMARY KEY,
  paused INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS schedules (
  queue        TEXT    NOT NULL,
  name         TEXT    NOT NULL,
  cron         TEXT    NOT NULL,
  timezone     TEXT    NOT NULL,
  data         TEXT    NOT NULL,
  job_options  TEXT    NOT NULL,
  next_run_at  INTEGER NOT NULL,
  last_run_at  INTEGER,
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL,
  PRIMARY KEY (queue, name)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS schedules_due ON schedules (next_run_at);

CREATE TABLE IF NOT EXISTS kv (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  expires_at INTEGER
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS kv_expiry ON kv (expires_at) WHERE expires_at IS NOT NULL;
`;
