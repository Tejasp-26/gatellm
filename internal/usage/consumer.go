package usage

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ConsumerConfig holds the settings of the consumer.
type ConsumerConfig struct {
	Name       string        // our name inside the group (default: the host name)
	Batch      int64         // how many events we read and write at once
	Block      time.Duration // how long one read waits for new events
	Blocking   bool          // false = never block in Redis, sleep Block between empty reads instead
	ClaimIdle  time.Duration // a message that nobody confirmed for this long belongs to a dead consumer
	ClaimEvery time.Duration // how often we look for such messages
}

// Consumer reads events from the Redis Stream and writes them to Postgres.
//
// Guarantee: "at least once". A message is confirmed (XACK) only AFTER Postgres has it.
// If we crash in between, the message is delivered again, and ON CONFLICT DO NOTHING in
// the writer makes the second write harmless. At-least-once + idempotent write = no event
// is lost and none is counted twice.
type Consumer struct {
	rdb *redis.Client
	w   EventWriter
	cfg ConsumerConfig

	Counters *Counters // optional, for /metrics

	needPending bool // true = first read the messages that were delivered to us but never confirmed
	failures    int  // failures in a row, used for the backoff
}

func NewConsumer(rdb *redis.Client, w EventWriter, cfg ConsumerConfig) *Consumer {
	if cfg.Name == "" {
		cfg.Name, _ = os.Hostname()
		if cfg.Name == "" {
			cfg.Name = "gatellm"
		}
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 50
	}
	if cfg.Block <= 0 {
		cfg.Block = 5 * time.Second
	}
	if cfg.ClaimIdle <= 0 {
		cfg.ClaimIdle = 30 * time.Second
	}
	if cfg.ClaimEvery <= 0 {
		cfg.ClaimEvery = 2 * cfg.ClaimIdle
	}
	return &Consumer{rdb: rdb, w: w, cfg: cfg, needPending: true}
}

// Run works until ctx is cancelled. Then it DRAINS: it writes everything that is still waiting
// (at most drainTimeout long) before it returns. Call it in a goroutine and wait for it at shutdown.
func (c *Consumer) Run(ctx context.Context, drainTimeout time.Duration) {
	c.ensureGroup(ctx)

	var nextClaim time.Time
	for ctx.Err() == nil {
		if !time.Now().Before(nextClaim) {
			c.claimStale(ctx)
			nextClaim = time.Now().Add(c.cfg.ClaimEvery)
		}
		c.step(ctx)
	}

	// Stop requested. The context is cancelled, so the drain gets a fresh one with its own deadline.
	dctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	c.drain(dctx)
}

// step does one round: read a batch and write it.
func (c *Consumer) step(ctx context.Context) {
	id, block := ">", time.Duration(-1) // ">" = only messages that were never delivered; -1 = do not block
	if c.needPending {
		id = "0" // "0" = the messages that were delivered to us but not confirmed yet
	} else if c.cfg.Blocking {
		block = c.cfg.Block
	}

	msgs, err := c.read(ctx, id, block)
	if err != nil {
		if ctx.Err() == nil {
			c.fail(ctx, "reading the usage stream failed", err)
		}
		return
	}
	if len(msgs) == 0 {
		if c.needPending {
			c.needPending = false // nothing left from before, now read the new ones
		} else if !c.cfg.Blocking {
			sleep(ctx, c.cfg.Block)
		}
		return
	}
	if err := c.process(msgs); err != nil {
		c.needPending = true // the unconfirmed messages are read again
		c.fail(ctx, "writing usage events failed, will retry", err)
		return
	}
	c.failures = 0
}

// read gets a batch from the stream. block < 0 means: do not wait.
func (c *Consumer) read(ctx context.Context, id string, block time.Duration) ([]redis.XMessage, error) {
	res, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: GroupName, Consumer: c.cfg.Name,
		Streams: []string{StreamKey, id}, Count: c.cfg.Batch, Block: block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // nothing arrived
	}
	if err != nil {
		return nil, err
	}
	var msgs []redis.XMessage
	for _, s := range res {
		msgs = append(msgs, s.Messages...)
	}
	return msgs, nil
}

// process writes a batch to Postgres and confirms it. A returned error means: some
// messages are NOT confirmed (they stay in the stream and will be tried again).
// It does not use the context of Run, so a batch that is being written is never cut in half.
func (c *Consumer) process(msgs []redis.XMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var events []Event
	var kept []redis.XMessage
	for _, m := range msgs {
		ev, err := parseEvent(m.Values)
		if err != nil {
			// A message that cannot be understood will never work. Do not block the queue with it.
			if err := c.deadLetter(ctx, m, err); err != nil {
				return err
			}
			continue
		}
		events = append(events, ev)
		kept = append(kept, m)
	}
	if len(events) == 0 {
		return nil
	}

	err := c.w.Write(ctx, events)
	if err == nil {
		c.Counters.addWritten(len(events))
		return c.ack(ctx, kept...)
	}
	if len(events) == 1 {
		if IsPermanent(err) {
			return c.deadLetter(ctx, kept[0], err)
		}
		return err
	}

	// The batch failed. One bad event must not stop the good ones, so try them one by one.
	var firstErr error
	for i, ev := range events {
		err := c.w.Write(ctx, []Event{ev})
		switch {
		case err == nil:
			c.Counters.addWritten(1)
			if e := c.ack(ctx, kept[i]); e != nil && firstErr == nil {
				firstErr = e
			}
		case IsPermanent(err):
			if e := c.deadLetter(ctx, kept[i], err); e != nil && firstErr == nil {
				firstErr = e
			}
		default:
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (c *Consumer) ack(ctx context.Context, msgs ...redis.XMessage) error {
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	return c.rdb.XAck(ctx, StreamKey, GroupName, ids...).Err()
}

// deadLetter moves a message that can never be written to the dead-letter stream and confirms it.
// If even that fails we return the error and the message stays pending (nothing is lost).
func (c *Consumer) deadLetter(ctx context.Context, m redis.XMessage, cause error) error {
	values := make(map[string]any, len(m.Values)+2)
	for k, v := range m.Values {
		values[k] = v
	}
	values["dead_reason"] = cause.Error()
	values["original_id"] = m.ID
	err := c.rdb.XAdd(ctx, &redis.XAddArgs{Stream: DeadKey, MaxLen: 10000, Approx: true, Values: values}).Err()
	if err != nil {
		return err
	}
	c.Counters.addDead()
	slog.Warn("usage event moved to the dead-letter stream",
		"request_id", m.Values["request_id"], "reason", cause.Error())
	return c.ack(ctx, m)
}

// claimStale takes over messages of consumers that died (they were read but never confirmed
// and nobody touched them for ClaimIdle). XAUTOCLAIM does this in one command.
func (c *Consumer) claimStale(ctx context.Context) {
	start := "0-0"
	for ctx.Err() == nil {
		msgs, next, err := c.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: StreamKey, Group: GroupName, Consumer: c.cfg.Name,
			MinIdle: c.cfg.ClaimIdle, Start: start, Count: c.cfg.Batch,
		}).Result()
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("could not claim old usage messages", "err", err)
			}
			return
		}
		if len(msgs) > 0 {
			slog.Info("took over usage messages of a dead consumer", "count", len(msgs))
			if err := c.process(msgs); err != nil {
				c.needPending = true
				c.fail(ctx, "writing claimed usage events failed, will retry", err)
				return
			}
		}
		if next == "" || next == "0-0" {
			return
		}
		start = next
	}
}

// drain writes everything that is still waiting, then returns. It gives up when ctx ends;
// whatever is left stays in the stream, and the next start of the gateway writes it.
func (c *Consumer) drain(ctx context.Context) {
	written := 0
	for ctx.Err() == nil {
		msgs, err := c.read(ctx, "0", -1) // first what we got before but never confirmed
		if err == nil && len(msgs) == 0 {
			msgs, err = c.read(ctx, ">", -1) // then what is new
		}
		if err != nil {
			slog.Warn("usage drain stopped: cannot read the stream", "err", err)
			return
		}
		if len(msgs) == 0 {
			slog.Info("usage pipeline drained", "events_written", written)
			return
		}
		if err := c.process(msgs); err != nil {
			slog.Warn("usage drain: write failed, retrying", "err", err)
			sleep(ctx, 200*time.Millisecond)
			continue
		}
		written += len(msgs)
	}
	slog.Warn("usage drain timed out, the rest stays in the stream for the next start")
}

// ensureGroup creates the group. If Redis is not reachable yet, it tries again every second.
func (c *Consumer) ensureGroup(ctx context.Context) {
	for ctx.Err() == nil {
		if err := EnsureGroup(ctx, c.rdb); err == nil {
			return
		} else if ctx.Err() == nil {
			slog.Warn("could not create the usage consumer group, retrying", "err", err)
		}
		sleep(ctx, time.Second)
	}
}

// fail logs a problem and waits a bit (200ms, 400ms, 800ms ... up to 5s) before the next try.
func (c *Consumer) fail(ctx context.Context, msg string, err error) {
	c.failures++
	slog.Warn(msg, "err", err, "failures_in_a_row", c.failures)
	if strings.Contains(err.Error(), "NOGROUP") {
		// Redis lost the stream (for example after a flush). Create the group again.
		EnsureGroup(context.Background(), c.rdb)
	}
	wait := 200 * time.Millisecond << min(c.failures-1, 5)
	sleep(ctx, min(wait, 5*time.Second))
}

// sleep waits d, but returns earlier when ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
