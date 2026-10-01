package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// maxEntryBytes: a bigger answer is not stored (it would waste Redis memory).
const maxEntryBytes = 512 * 1024

// Redis is the Cache that keeps answers in Redis.
type Redis struct {
	rdb     *redis.Client
	ttl     time.Duration // how long an answer is kept
	timeout time.Duration // max time for one Redis call: the cache must never slow requests down
}

func NewRedis(rdb *redis.Client, ttl, timeout time.Duration) *Redis {
	return &Redis{rdb: rdb, ttl: ttl, timeout: timeout}
}

func (c *Redis) Get(ctx context.Context, key string) (*Entry, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	data, err := c.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil // simply not there
	}
	if err != nil {
		return nil, false, err
	}

	var entry Entry
	if err := json.Unmarshal(data, &entry); err != nil || entry.Response == nil {
		return nil, false, fmt.Errorf("stored cache entry is damaged: %v", err)
	}
	return &entry, true, nil
}

func (c *Redis) Set(ctx context.Context, key string, entry *Entry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if len(data) > maxEntryBytes {
		return nil // too big, we just do not cache it
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return c.rdb.Set(ctx, key, data, c.ttl).Err()
}
