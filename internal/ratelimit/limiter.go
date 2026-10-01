// Package ratelimit limits how many requests and tokens a tenant can use per minute.
//
// It uses the "token bucket" idea: each tenant has two buckets in Redis
// (one for requests, one for tokens). A bucket is full at the start,
// each request takes something out, and the bucket slowly fills up again.
// The check and the update run inside ONE Lua script, so Redis does it
// atomically. Two gateway instances can never both take the last token.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limits are the per-minute limits of one tenant.
type Limits struct {
	RPM int // requests per minute
	TPM int // tokens per minute
}

// Decision is the answer of Allow.
type Decision struct {
	Allowed           bool
	Reason            string        // "rpm" or "tpm" when denied
	RetryAfter        time.Duration // how long until the request could pass
	RemainingRequests int64
	RemainingTokens   int64
	Degraded          bool // true: Redis was down and we let the request pass (fail-open)
}

// ErrUnavailable means Redis could not be used and we are in fail-closed mode.
var ErrUnavailable = errors.New("rate limiter unavailable")

// Limiter talks to Redis.
type Limiter struct {
	rdb      *redis.Client
	failOpen bool          // what to do when Redis is down
	timeout  time.Duration // max time for one Redis call
	now      func() time.Time
}

// New creates a Limiter. failOpen=true lets requests pass when Redis is down,
// failOpen=false rejects them.
func New(rdb *redis.Client, failOpen bool, timeout time.Duration) *Limiter {
	return &Limiter{rdb: rdb, failOpen: failOpen, timeout: timeout, now: time.Now}
}

// windowMs: a bucket refills completely in one minute.
const windowMs = 60000

// These small Lua functions are shared by both scripts.
//
// refill  = read the bucket and add the tokens that came back since the last time.
// save    = write the bucket back and set an expiry, so idle tenants cost no memory.
//
// Numbers: capacity = the per-minute limit, refill speed = capacity / 60000 per millisecond.
// The time comes from the Go side (ARGV), so we do not depend on Redis TIME.
const luaHelpers = `
local function refill(key, cap, now)
  local d = redis.call('HMGET', key, 'tokens', 'ts')
  local tokens = tonumber(d[1])
  local ts = tonumber(d[2])
  if tokens == nil or ts == nil then
    return cap, now                      -- new bucket starts full
  end
  local elapsed = now - ts
  if elapsed < 0 then elapsed = 0 end    -- clocks of two servers can differ a little
  tokens = math.min(cap, tokens + elapsed * cap / 60000)
  return tokens, math.max(ts, now)
end

local function save(key, tokens, ts, cap)
  redis.call('HSET', key, 'tokens', string.format('%.4f', tokens), 'ts', string.format('%d', ts))
  local ttl = 120000
  if tokens < 0 then                     -- a debt needs longer to be paid back
    ttl = ttl + math.ceil(-tokens * 60000 / cap)
  end
  redis.call('PEXPIRE', key, ttl)
end
`

// allowScript checks BOTH buckets. It takes from both, or from none.
// KEYS[1] = requests bucket, KEYS[2] = tokens bucket
// ARGV    = now_ms, rpm_limit, tpm_limit, token_cost
// Returns = {allowed, reason (0 ok, 1 rpm, 2 tpm), retry_ms, remaining_requests, remaining_tokens}
var allowScript = redis.NewScript(luaHelpers + `
local now  = tonumber(ARGV[1])
local rpm  = tonumber(ARGV[2])
local tpm  = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

local rt, rts = refill(KEYS[1], rpm, now)
local tt, tts = refill(KEYS[2], tpm, now)

local reason, retry = 0, 0
if rt < 1 then
  reason = 1
  retry = math.ceil((1 - rt) * 60000 / rpm)
end
if tt < cost then
  local r = math.ceil((cost - tt) * 60000 / tpm)
  if reason == 0 then reason = 2 end
  if r > retry then retry = r end
end

if reason == 0 then      -- both have enough: take from both
  rt = rt - 1
  tt = tt - cost
end
save(KEYS[1], rt, rts, rpm)
save(KEYS[2], tt, tts, tpm)

local allowed = 0
if reason == 0 then allowed = 1 end
return {allowed, reason, retry, math.floor(rt), math.floor(tt)}
`)

// adjustScript corrects the tokens bucket after we know the real usage.
// KEYS[1] = tokens bucket
// ARGV    = now_ms, tpm_limit, delta   (delta > 0 takes more, delta < 0 gives back)
var adjustScript = redis.NewScript(luaHelpers + `
local now   = tonumber(ARGV[1])
local tpm   = tonumber(ARGV[2])
local delta = tonumber(ARGV[3])

if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0               -- the bucket already expired (it was full again), nothing to fix
end
local tt, ts = refill(KEYS[1], tpm, now)
tt = math.min(tpm, tt - delta)   -- may go below zero: the tenant "owes" tokens
save(KEYS[1], tt, ts, tpm)
return 1
`)

func requestsKey(tenantID string) string { return "rl:{" + tenantID + "}:rpm" }
func tokensKey(tenantID string) string   { return "rl:{" + tenantID + "}:tpm" }

// Allow asks: may this tenant make one more request that will use about `cost` tokens?
// If Redis fails: fail-open returns Allowed with Degraded=true, fail-closed returns ErrUnavailable.
func (l *Limiter) Allow(ctx context.Context, tenantID string, lim Limits, cost int64) (Decision, error) {
	if lim.RPM <= 0 || lim.TPM <= 0 {
		return Decision{Reason: "rpm"}, nil // a limit of 0 means "nothing allowed"
	}
	if cost < 0 {
		cost = 0
	}

	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()

	res, err := allowScript.Run(ctx, l.rdb,
		[]string{requestsKey(tenantID), tokensKey(tenantID)},
		l.now().UnixMilli(), lim.RPM, lim.TPM, cost,
	).Int64Slice()
	if err != nil || len(res) != 5 {
		if err == nil {
			err = fmt.Errorf("unexpected script result: %v", res)
		}
		if l.failOpen {
			return Decision{Allowed: true, Degraded: true}, nil
		}
		return Decision{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}

	d := Decision{
		Allowed:           res[0] == 1,
		RetryAfter:        time.Duration(res[2]) * time.Millisecond,
		RemainingRequests: max(res[3], 0),
		RemainingTokens:   max(res[4], 0),
	}
	switch res[1] {
	case 1:
		d.Reason = "rpm"
	case 2:
		d.Reason = "tpm"
	}
	return d, nil
}

// Adjust fixes the tokens bucket once the real usage is known.
// delta = real tokens - estimated tokens. Negative gives tokens back.
// It is best effort: the caller should only log an error, not fail the request.
func (l *Limiter) Adjust(ctx context.Context, tenantID string, lim Limits, delta int64) error {
	if delta == 0 || lim.TPM <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()

	return adjustScript.Run(ctx, l.rdb, []string{tokensKey(tenantID)},
		l.now().UnixMilli(), lim.TPM, delta,
	).Err()
}

// RetryAfterSeconds turns a wait time into whole seconds for the Retry-After header (at least 1).
func RetryAfterSeconds(d time.Duration) int {
	s := int(math.Ceil(d.Seconds()))
	if s < 1 {
		s = 1
	}
	return s
}
