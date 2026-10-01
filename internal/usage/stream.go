package usage

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Names in Redis.
const (
	StreamKey = "usage:events" // new events wait here
	DeadKey   = "usage:dead"   // events that can never be written (kept for a human to look at)
	GroupName = "usage-writers"
)

// Stream is the writing side: the request handlers put events in the Redis Stream.
// A Redis Stream is an append-only log. A consumer group lets several readers share the work,
// and every message stays "pending" until a reader confirms it with XACK. That is what
// makes the pipeline safe: a crash before XACK means the message is delivered again.
type Stream struct {
	rdb    *redis.Client
	maxLen int64
}

// NewStream keeps about maxLen events. If nobody reads them for a long time the OLDEST are
// dropped, so a dead consumer cannot fill the memory of Redis.
func NewStream(rdb *redis.Client, maxLen int64) *Stream {
	return &Stream{rdb: rdb, maxLen: maxLen}
}

// Publish adds one event to the stream (XADD). "~" means Redis may trim in whole blocks, which is faster.
func (s *Stream) Publish(ctx context.Context, e Event) error {
	return s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamKey, MaxLen: s.maxLen, Approx: true, Values: e.fields(),
	}).Err()
}

// EnsureGroup creates the stream and the consumer group if they do not exist yet.
// Starting at "0" means a new group also reads events that were published before it existed.
func EnsureGroup(ctx context.Context, rdb *redis.Client) error {
	err := rdb.XGroupCreateMkStream(ctx, StreamKey, GroupName, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") { // BUSYGROUP = it exists already
		return err
	}
	return nil
}
