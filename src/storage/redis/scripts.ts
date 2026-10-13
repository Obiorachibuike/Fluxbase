/**
 * Lua scripts executed atomically by Redis. Redis runs a script without
 * interleaving other commands, so each script is a single transition: claim,
 * complete, fail, renew, and so on. Key names are passed in KEYS and job-hash
 * keys are built from the ARGV prefix. Numbers are formatted with %.0f
 * because Redis Lua numbers lose precision above 2^53 with the default format.
 */

const HELPERS = `
local function fmt(n) return string.format('%.0f', n) end

local function trim(zkey, jobPrefix, keep)
  local excess = redis.call('ZCARD', zkey) - keep
  if excess > 0 then
    local ids = redis.call('ZRANGE', zkey, 0, excess - 1)
    for _, id in ipairs(ids) do
      redis.call('DEL', jobPrefix .. id)
    end
    redis.call('ZREMRANGEBYRANK', zkey, 0, excess - 1)
  end
end
`;

/**
 * KEYS: waiting, delayed, active, paused, failed
 * ARGV: jobPrefix, now, leaseMs, limit, workerId, tokenBase
 * Returns: array of flat HGETALL arrays for the claimed jobs.
 */
export const CLAIM_SCRIPT = `${HELPERS}
local waiting, delayed, active, paused, failed = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local jp = ARGV[1]
local now = tonumber(ARGV[2])
local leaseMs = tonumber(ARGV[3])
local limit = tonumber(ARGV[4])
local workerId = ARGV[5]
local tokenBase = ARGV[6]

-- 1. Promote delayed jobs that are due.
local due = redis.call('ZRANGEBYSCORE', delayed, '-inf', now, 'LIMIT', 0, 500)
for _, id in ipairs(due) do
  local jk = jp .. id
  redis.call('ZREM', delayed, id)
  redis.call('HSET', jk, 'state', 'waiting')
  redis.call('ZADD', waiting, redis.call('HGET', jk, 'wscore'), id)
end

-- 2. Recover jobs whose lease expired.
local expired = redis.call('ZRANGEBYSCORE', active, '-inf', now, 'LIMIT', 0, 100)
for _, id in ipairs(expired) do
  local jk = jp .. id
  local made = tonumber(redis.call('HGET', jk, 'attempts_made'))
  local maxAttempts = tonumber(redis.call('HGET', jk, 'attempts'))
  redis.call('ZREM', active, id)
  redis.call('HSET', jk, 'lock_token', '', 'locked_until', '', 'worker_id', '')
  if made >= maxAttempts then
    redis.call('HSET', jk, 'state', 'failed', 'finished_at', fmt(now),
      'failed_reason', 'LeaseExpired: attempt ' .. fmt(made) .. ' of ' .. fmt(maxAttempts) .. ' lost its lease (worker crashed or stalled)')
    redis.call('ZADD', failed, fmt(now), id)
  else
    redis.call('HSET', jk, 'state', 'waiting')
    redis.call('ZADD', waiting, redis.call('HGET', jk, 'wscore'), id)
  end
end

-- 3. Paused queues accept jobs but hand none out.
if redis.call('EXISTS', paused) == 1 then
  return {}
end

-- 4. Claim the highest-priority waiting jobs.
if limit < 1 then return {} end
local ids = redis.call('ZRANGE', waiting, 0, limit - 1)
local out = {}
local leaseUntil = now + leaseMs
for i, id in ipairs(ids) do
  local jk = jp .. id
  redis.call('ZREM', waiting, id)
  redis.call('HSET', jk, 'state', 'active', 'lock_token', tokenBase .. ':' .. i,
    'locked_until', fmt(leaseUntil), 'worker_id', workerId, 'processed_at', fmt(now))
  redis.call('HINCRBY', jk, 'attempts_made', 1)
  redis.call('ZADD', active, fmt(leaseUntil), id)
  table.insert(out, redis.call('HGETALL', jk))
end
return out
`;

/**
 * KEYS: active
 * ARGV: jobPrefix, now, leaseMs, id1, token1, id2, token2, ...
 * Returns: ids whose lease was renewed.
 */
export const RENEW_SCRIPT = `
local active = KEYS[1]
local jp = ARGV[1]
local now = tonumber(ARGV[2])
local leaseMs = tonumber(ARGV[3])
local newScore = now + leaseMs
local out = {}
for i = 4, #ARGV, 2 do
  local id = ARGV[i]
  local token = ARGV[i + 1]
  local jk = jp .. id
  if redis.call('HGET', jk, 'state') == 'active' and redis.call('HGET', jk, 'lock_token') == token then
    redis.call('HSET', jk, 'locked_until', string.format('%.0f', newScore))
    redis.call('ZADD', active, string.format('%.0f', newScore), id)
    table.insert(out, id)
  end
end
return out
`;

/**
 * KEYS: active, completed
 * ARGV: jobPrefix, id, token, now, returnValue('' for none), keepCompleted
 * Returns: 1 when completed, 0 when the lease was lost.
 */
export const COMPLETE_SCRIPT = `${HELPERS}
local jp = ARGV[1]
local id = ARGV[2]
local jk = jp .. id
if redis.call('HGET', jk, 'state') ~= 'active' or redis.call('HGET', jk, 'lock_token') ~= ARGV[3] then
  return 0
end
redis.call('ZREM', KEYS[1], id)
if redis.call('HGET', jk, 'remove_on_complete') == '1' then
  redis.call('DEL', jk)
  return 1
end
redis.call('HSET', jk, 'state', 'completed', 'finished_at', ARGV[4], 'return_value', ARGV[5],
  'lock_token', '', 'locked_until', '', 'worker_id', '')
redis.call('ZADD', KEYS[2], ARGV[4], id)
trim(KEYS[2], jp, tonumber(ARGV[6]))
return 1
`;

/**
 * KEYS: active, waiting, delayed, failed
 * ARGV: jobPrefix, id, token, now, retryAt('' for final failure), reason, keepFailed
 * Returns: 'retry' | 'failed' | 'lost'
 */
export const FAIL_SCRIPT = `${HELPERS}
local jp = ARGV[1]
local id = ARGV[2]
local token = ARGV[3]
local now = tonumber(ARGV[4])
local retryAt = ARGV[5]
local reason = ARGV[6]
local jk = jp .. id
if redis.call('HGET', jk, 'state') ~= 'active' or redis.call('HGET', jk, 'lock_token') ~= token then
  return 'lost'
end
redis.call('ZREM', KEYS[1], id)
if retryAt ~= '' then
  local at = tonumber(retryAt)
  if at <= now then
    redis.call('HSET', jk, 'state', 'waiting')
    redis.call('ZADD', KEYS[2], redis.call('HGET', jk, 'wscore'), id)
  else
    redis.call('HSET', jk, 'state', 'delayed')
    redis.call('ZADD', KEYS[3], retryAt, id)
  end
  redis.call('HSET', jk, 'run_at', retryAt, 'failed_reason', reason,
    'lock_token', '', 'locked_until', '', 'worker_id', '')
  return 'retry'
end
if redis.call('HGET', jk, 'remove_on_fail') == '1' then
  redis.call('DEL', jk)
  return 'failed'
end
redis.call('HSET', jk, 'state', 'failed', 'finished_at', ARGV[4], 'failed_reason', reason,
  'lock_token', '', 'locked_until', '', 'worker_id', '')
redis.call('ZADD', KEYS[4], ARGV[4], id)
trim(KEYS[4], jp, tonumber(ARGV[7]))
return 'failed'
`;

/**
 * KEYS: active, waiting
 * ARGV: jobPrefix, id, token
 * Returns: 1 when released, 0 otherwise.
 */
export const RELEASE_SCRIPT = `
local jp = ARGV[1]
local id = ARGV[2]
local jk = jp .. id
if redis.call('HGET', jk, 'state') ~= 'active' or redis.call('HGET', jk, 'lock_token') ~= ARGV[3] then
  return 0
end
local made = tonumber(redis.call('HGET', jk, 'attempts_made'))
redis.call('ZREM', KEYS[1], id)
redis.call('HSET', jk, 'state', 'waiting', 'attempts_made', string.format('%.0f', math.max(made - 1, 0)),
  'lock_token', '', 'locked_until', '', 'worker_id', '')
redis.call('ZADD', KEYS[2], redis.call('HGET', jk, 'wscore'), id)
return 1
`;

/**
 * KEYS: failed, waiting
 * ARGV: jobPrefix, id, now
 * Returns: 'retried' | 'not_found' | 'not_failed'
 */
export const RETRY_SCRIPT = `
local jp = ARGV[1]
local id = ARGV[2]
local jk = jp .. id
local state = redis.call('HGET', jk, 'state')
if not state then return 'not_found' end
if state ~= 'failed' then return 'not_failed' end
redis.call('ZREM', KEYS[1], id)
redis.call('HSET', jk, 'state', 'waiting', 'attempts_made', '0', 'failed_reason', '',
  'finished_at', '', 'run_at', ARGV[3])
redis.call('ZADD', KEYS[2], redis.call('HGET', jk, 'wscore'), id)
return 'retried'
`;

/**
 * KEYS: waiting, delayed, completed, failed, active
 * ARGV: jobPrefix, id
 * Returns: 'removed' | 'not_found' | 'active'
 */
export const REMOVE_SCRIPT = `
local jp = ARGV[1]
local id = ARGV[2]
local jk = jp .. id
local state = redis.call('HGET', jk, 'state')
if not state then return 'not_found' end
if state == 'active' then return 'active' end
for i = 1, 4 do
  redis.call('ZREM', KEYS[i], id)
end
redis.call('DEL', jk)
return 'removed'
`;

/**
 * Inserts a job (idempotent). Used by insertJob and fireSchedule.
 * KEYS: waiting, delayed, seq
 * ARGV: jobPrefix, id, fieldsJson
 * Returns: 1 inserted, 0 when the id already exists.
 */
export const INSERT_FUNCTION = `
local function insertJob(jp, id, fieldsJson, waitingKey, delayedKey, seqKey)
  local jk = jp .. id
  if redis.call('EXISTS', jk) == 1 then return 0 end
  local seq = redis.call('INCR', seqKey)
  local f = cjson.decode(fieldsJson)
  local priority = tonumber(f['priority'])
  local wscore = string.format('%.0f', (1000 - priority) * 1000000000000 + seq)
  f['seq'] = string.format('%.0f', seq)
  f['wscore'] = wscore
  local flat = {}
  for k, v in pairs(f) do
    table.insert(flat, k)
    table.insert(flat, tostring(v))
  end
  redis.call('HSET', jk, unpack(flat))
  if f['state'] == 'waiting' then
    redis.call('ZADD', waitingKey, wscore, id)
  else
    redis.call('ZADD', delayedKey, f['run_at'], id)
  end
  return 1
end
`;

export const INSERT_SCRIPT = `${INSERT_FUNCTION}
return insertJob(ARGV[1], ARGV[2], ARGV[3], KEYS[1], KEYS[2], KEYS[3])
`;

/**
 * Compare-and-set a schedule and insert its job.
 * KEYS: scheduleHash, schedulesZset, waiting, delayed, seq
 * ARGV: jobPrefix, expectedNextRunAt, nextRunAt, now, jobId, jobFieldsJson, scheduleMember
 * Returns: 1 when fired, 0 when another scheduler already advanced it.
 */
export const FIRE_SCHEDULE_SCRIPT = `${INSERT_FUNCTION}
local cur = redis.call('HGET', KEYS[1], 'next_run_at')
if cur ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[1], 'next_run_at', ARGV[3], 'last_run_at', ARGV[4], 'updated_at', ARGV[4])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[7])
insertJob(ARGV[1], ARGV[5], ARGV[6], KEYS[3], KEYS[4], KEYS[5])
return 1
`;

/**
 * Upserts a schedule definition.
 * KEYS: scheduleHash, schedulesZset, schedulesIndexSet
 * ARGV: member, nameField, fieldsJson (flat array of field/value pairs as JSON)
 */
export const PUT_SCHEDULE_SCRIPT = `
local pairsList = cjson.decode(ARGV[1])
redis.call('HSET', KEYS[1], unpack(pairsList))
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
redis.call('SADD', KEYS[3], ARGV[4])
return 1
`;

/**
 * KEYS: scheduleHash, schedulesZset, schedulesIndexSet
 * ARGV: member, name
 */
export const REMOVE_SCHEDULE_SCRIPT = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('SREM', KEYS[3], ARGV[2])
return 1
`;
