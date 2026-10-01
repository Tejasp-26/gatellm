package usage

import "sync/atomic"

// Counters count what the usage pipeline does. They live in memory (since the last start)
// and are shown in /metrics. All methods are safe to call on a nil *Counters.
type Counters struct {
	published atomic.Int64 // events put in the Redis Stream
	fallback  atomic.Int64 // events written straight to Postgres because the stream failed
	lost      atomic.Int64 // events that could not be saved anywhere (they are in the error log)
	written   atomic.Int64 // events the consumer wrote to Postgres (duplicates included)
	dead      atomic.Int64 // events moved to the dead-letter stream
}

// add increases one counter. A nil *Counters means "nobody is counting".
func add(c *Counters, field func(*Counters) *atomic.Int64, n int) {
	if c != nil {
		field(c).Add(int64(n))
	}
}

func get(c *Counters, field func(*Counters) *atomic.Int64) int64 {
	if c == nil {
		return 0
	}
	return field(c).Load()
}

func published(c *Counters) *atomic.Int64 { return &c.published }
func fallback(c *Counters) *atomic.Int64  { return &c.fallback }
func lost(c *Counters) *atomic.Int64      { return &c.lost }
func written(c *Counters) *atomic.Int64   { return &c.written }
func dead(c *Counters) *atomic.Int64      { return &c.dead }

func (c *Counters) Published() int64 { return get(c, published) }
func (c *Counters) Fallback() int64  { return get(c, fallback) }
func (c *Counters) Lost() int64      { return get(c, lost) }
func (c *Counters) Written() int64   { return get(c, written) }
func (c *Counters) Dead() int64      { return get(c, dead) }

func (c *Counters) addPublished()    { add(c, published, 1) }
func (c *Counters) addFallback()     { add(c, fallback, 1) }
func (c *Counters) addLost()         { add(c, lost, 1) }
func (c *Counters) addWritten(n int) { add(c, written, n) }
func (c *Counters) addDead()         { add(c, dead, 1) }
