package usage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ---- helpers ----

func sampleEvent(id string) Event {
	return Event{
		RequestID: id, TenantID: "11111111-1111-1111-1111-111111111111",
		Provider: "mock", Model: "m1", PromptTokens: 5, CompletionTokens: 7, CostUSD: 0.00012,
		LatencyMS: 42, CacheStatus: "MISS", Status: "ok", CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}
}

// newRedis connects to the test Redis and clears our two streams before and after the test.
func newRedis(t *testing.T) *redis.Client {
	t.Helper()
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		t.Skip("TEST_REDIS_URL is not set, skipping the Redis tests")
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis is not reachable: %v", err)
	}
	clean := func() { rdb.Del(ctx, StreamKey, DeadKey) }
	clean()
	t.Cleanup(func() { clean(); rdb.Close() })
	return rdb
}

// fakeWriter keeps events in memory, like Postgres with ON CONFLICT DO NOTHING (no duplicates).
type fakeWriter struct {
	mu        sync.Mutex
	rows      map[string]Event
	calls     int
	failFirst int           // the first N calls fail with a temporary error
	alwaysErr error         // every call fails with this error
	badID     string        // a call that contains this request id fails with a permanent error
	delay     time.Duration // every call takes this long
}

func newFakeWriter() *fakeWriter { return &fakeWriter{rows: map[string]Event{}} }

func (f *fakeWriter) Write(ctx context.Context, events []Event) error {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.alwaysErr != nil {
		return f.alwaysErr
	}
	if f.calls <= f.failFirst {
		return errors.New("database is temporarily down")
	}
	for _, e := range events {
		if e.RequestID == f.badID {
			return &permanentError{err: errors.New("tenant does not exist")}
		}
	}
	for _, e := range events {
		f.rows[e.RequestID] = e
	}
	return nil
}

func (f *fakeWriter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// startConsumer runs the consumer in a goroutine. stop() cancels it and waits until Run (and its drain) is finished.
// It is also stopped when the test ends, even if the test fails half way.
func startConsumer(t *testing.T, c *Consumer, drain time.Duration) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx, drain); close(done) }()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

func fastConfig(name string) ConsumerConfig {
	return ConsumerConfig{Name: name, Batch: 50, Block: 20 * time.Millisecond, Blocking: false,
		ClaimIdle: time.Hour, ClaimEvery: time.Hour}
}

func publish(t *testing.T, rdb *redis.Client, n int, prefix string) {
	t.Helper()
	s := NewStream(rdb, 100000)
	for i := 0; i < n; i++ {
		if err := s.Publish(context.Background(), sampleEvent(fmt.Sprintf("%s-%d", prefix, i))); err != nil {
			t.Fatal(err)
		}
	}
}

func pending(t *testing.T, rdb *redis.Client) int64 {
	t.Helper()
	p, err := rdb.XPending(context.Background(), StreamKey, GroupName).Result()
	if err != nil {
		t.Fatal(err)
	}
	return p.Count
}

// ---- events ----

func TestEventSurvivesTheStream(t *testing.T) {
	rdb := newRedis(t)
	want := sampleEvent("r-1")
	if err := NewStream(rdb, 1000).Publish(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	msgs, err := rdb.XRange(context.Background(), StreamKey, "-", "+").Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("expected one message: %v %v", msgs, err)
	}
	got, err := parseEvent(msgs[0].Values)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("event changed on the way:\n got  %+v\n want %+v", got, want)
	}
}

func TestParseEventRejectsBrokenMessages(t *testing.T) {
	good := sampleEvent("r-1").fields()
	// Redis returns every value as a string, so we do the same here.
	asStrings := func(mod func(m map[string]any)) map[string]any {
		m := map[string]any{}
		for k, v := range good {
			m[k] = fmt.Sprint(v)
		}
		mod(m)
		return m
	}
	if _, err := parseEvent(asStrings(func(m map[string]any) {})); err != nil {
		t.Fatalf("the good message must parse: %v", err)
	}
	tests := map[string]func(m map[string]any){
		"no request id":    func(m map[string]any) { delete(m, "request_id") },
		"tenant not uuid":  func(m map[string]any) { m["tenant_id"] = "not-a-uuid" },
		"tokens not a int": func(m map[string]any) { m["prompt_tokens"] = "many" },
		"cost not number":  func(m map[string]any) { m["cost_usd"] = "free" },
		"bad time":         func(m map[string]any) { m["created_at"] = "yesterday" },
	}
	for name, mod := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := parseEvent(asStrings(mod)); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

// ---- the consumer ----

func TestConsumerWritesEveryEventAndConfirmsThem(t *testing.T) {
	rdb := newRedis(t)
	w := newFakeWriter()
	publish(t, rdb, 120, "a") // more than one batch of 50

	stop := startConsumer(t, NewConsumer(rdb, w, fastConfig("c1")), 5*time.Second)
	waitFor(t, "120 events written", func() bool { return w.count() == 120 })
	stop()

	if n := pending(t, rdb); n != 0 {
		t.Errorf("all messages must be confirmed, %d are still pending", n)
	}
}

func TestEventsPublishedWhileRunningArePickedUp(t *testing.T) {
	rdb := newRedis(t)
	w := newFakeWriter()
	stop := startConsumer(t, NewConsumer(rdb, w, fastConfig("c1")), 5*time.Second)
	defer stop()
	time.Sleep(100 * time.Millisecond)

	publish(t, rdb, 5, "live")
	waitFor(t, "5 live events", func() bool { return w.count() == 5 })
}

func TestBlockingModeAlsoWorks(t *testing.T) {
	rdb := newRedis(t)
	w := newFakeWriter()
	cfg := fastConfig("c1")
	cfg.Blocking, cfg.Block = true, 200*time.Millisecond
	stop := startConsumer(t, NewConsumer(rdb, w, cfg), 5*time.Second)
	defer stop()
	time.Sleep(100 * time.Millisecond)

	publish(t, rdb, 3, "blk")
	waitFor(t, "3 events in blocking mode", func() bool { return w.count() == 3 })
}

func TestTemporaryDatabaseFailureLosesNothing(t *testing.T) {
	rdb := newRedis(t)
	w := newFakeWriter()
	w.failFirst = 3 // the database is "down" for the first 3 writes
	publish(t, rdb, 30, "t")

	stop := startConsumer(t, NewConsumer(rdb, w, fastConfig("c1")), 5*time.Second)
	waitFor(t, "30 events written after the outage", func() bool { return w.count() == 30 })
	stop()

	if n := pending(t, rdb); n != 0 {
		t.Errorf("%d messages still pending", n)
	}
	if n := rdb.XLen(context.Background(), DeadKey).Val(); n != 0 {
		t.Errorf("a temporary failure must not create dead letters, found %d", n)
	}
}

func TestPoisonMessageGoesToTheDeadLetterStream(t *testing.T) {
	rdb := newRedis(t)
	w := newFakeWriter()
	ctx := context.Background()
	publish(t, rdb, 3, "ok")
	rdb.XAdd(ctx, &redis.XAddArgs{Stream: StreamKey, Values: map[string]any{"request_id": "bad", "tenant_id": "nope"}})
	publish(t, rdb, 2, "ok2")

	stop := startConsumer(t, NewConsumer(rdb, w, fastConfig("c1")), 5*time.Second)
	waitFor(t, "5 good events", func() bool { return w.count() == 5 })
	stop()

	dead := rdb.XRange(ctx, DeadKey, "-", "+").Val()
	if len(dead) != 1 || dead[0].Values["request_id"] != "bad" || dead[0].Values["dead_reason"] == "" {
		t.Errorf("the bad message must be in the dead-letter stream with a reason: %+v", dead)
	}
	if n := pending(t, rdb); n != 0 {
		t.Errorf("the poison message must be confirmed too, %d pending", n)
	}
}

func TestOneUnwritableEventDoesNotBlockTheOthers(t *testing.T) {
	rdb := newRedis(t)
	w := newFakeWriter()
	w.badID = "x-7" // the database will refuse this one for good
	publish(t, rdb, 20, "x")

	stop := startConsumer(t, NewConsumer(rdb, w, fastConfig("c1")), 5*time.Second)
	waitFor(t, "19 good events", func() bool { return w.count() == 19 })
	stop()

	dead := rdb.XRange(context.Background(), DeadKey, "-", "+").Val()
	if len(dead) != 1 || dead[0].Values["request_id"] != "x-7" {
		t.Errorf("only x-7 should be dead: %+v", dead)
	}
	if n := pending(t, rdb); n != 0 {
		t.Errorf("%d pending", n)
	}
}

// The app is killed after it read events but before it wrote them. After the restart nothing is lost.
func TestRestartWritesWhatWasReadButNeverConfirmed(t *testing.T) {
	rdb := newRedis(t)
	publish(t, rdb, 40, "crash")

	// First run: the database is broken, so the events are read but never confirmed.
	broken := newFakeWriter()
	broken.alwaysErr = errors.New("database is down")
	stop := startConsumer(t, NewConsumer(rdb, broken, fastConfig("same-name")), 300*time.Millisecond)
	waitFor(t, "events were delivered", func() bool {
		p, err := rdb.XPending(context.Background(), StreamKey, GroupName).Result()
		return err == nil && p.Count == 40 // (the group may not exist for a moment)
	})
	stop() // drain times out, like a kill

	// Second run (a restart): same consumer name, the database works again.
	w := newFakeWriter()
	stop = startConsumer(t, NewConsumer(rdb, w, fastConfig("same-name")), 5*time.Second)
	waitFor(t, "all 40 events written after the restart", func() bool { return w.count() == 40 })
	stop()
	if n := pending(t, rdb); n != 0 {
		t.Errorf("%d pending after recovery", n)
	}
}

// A consumer with a different name (another server, a new container) died with unconfirmed messages.
func TestMessagesOfADeadConsumerAreTakenOver(t *testing.T) {
	rdb := newRedis(t)
	ctx := context.Background()
	publish(t, rdb, 10, "dead")
	if err := EnsureGroup(ctx, rdb); err != nil {
		t.Fatal(err)
	}
	// "ghost" reads the messages and then disappears without confirming.
	got, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: GroupName, Consumer: "ghost", Streams: []string{StreamKey, ">"}, Count: 100, Block: -1,
	}).Result()
	if err != nil || len(got[0].Messages) != 10 {
		t.Fatalf("setup failed: %v", err)
	}

	w := newFakeWriter()
	cfg := fastConfig("alive")
	cfg.ClaimIdle, cfg.ClaimEvery = 100*time.Millisecond, 50*time.Millisecond
	stop := startConsumer(t, NewConsumer(rdb, w, cfg), 5*time.Second)
	defer stop()

	waitFor(t, "the ghost's 10 events written", func() bool { return w.count() == 10 })
}

func TestStopDrainsEverythingBeforeReturning(t *testing.T) {
	rdb := newRedis(t)
	w := newFakeWriter()
	w.delay = 5 * time.Millisecond // slow writes, so the stop comes while work is left
	publish(t, rdb, 300, "drain")

	c := NewConsumer(rdb, w, fastConfig("c1"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx, 20*time.Second); close(done) }()
	waitFor(t, "the consumer started working", func() bool { return w.count() > 0 })
	cancel() // shutdown!
	<-done   // Run returns only when the drain is finished

	if got := w.count(); got != 300 {
		t.Errorf("after Run returned %d of 300 events were written", got)
	}
	if n := pending(t, rdb); n != 0 {
		t.Errorf("%d pending", n)
	}
}

func TestDrainGivesUpAtTheDeadlineAndKeepsTheRest(t *testing.T) {
	rdb := newRedis(t)
	w := newFakeWriter()
	w.alwaysErr = errors.New("database is down")
	publish(t, rdb, 10, "keep")

	start := time.Now()
	stop := startConsumer(t, NewConsumer(rdb, w, fastConfig("c1")), 400*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	stop()
	if time.Since(start) > 3*time.Second {
		t.Error("the drain must stop at its deadline")
	}
	// The events are not lost: they are still in the stream.
	if n := rdb.XLen(context.Background(), StreamKey).Val(); n != 10 {
		t.Errorf("the 10 events must still be in the stream, found %d", n)
	}
}

func TestConsumerSurvivesTheStreamBeingDeleted(t *testing.T) {
	rdb := newRedis(t)
	w := newFakeWriter()
	stop := startConsumer(t, NewConsumer(rdb, w, fastConfig("c1")), 5*time.Second)
	defer stop()
	time.Sleep(100 * time.Millisecond)

	rdb.Del(context.Background(), StreamKey) // for example somebody flushed Redis
	time.Sleep(100 * time.Millisecond)
	publish(t, rdb, 3, "after")
	waitFor(t, "events after the stream was deleted", func() bool { return w.count() == 3 })
}

// ---- the recorder ----

func TestRecorderPublishesToTheStream(t *testing.T) {
	rdb := newRedis(t)
	fb := newFakeWriter()
	NewRecorder(NewStream(rdb, 1000), fb, time.Second).Record(context.Background(), sampleEvent("rec-1"))

	if n := rdb.XLen(context.Background(), StreamKey).Val(); n != 1 {
		t.Errorf("one event must be in the stream, found %d", n)
	}
	if fb.count() != 0 {
		t.Error("the fallback must not be used when Redis works")
	}
}

func TestRecorderWritesDirectlyWhenRedisIsDown(t *testing.T) {
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: 0, DialTimeout: 100 * time.Millisecond})
	fb := newFakeWriter()
	NewRecorder(NewStream(dead, 1000), fb, 300*time.Millisecond).Record(context.Background(), sampleEvent("rec-2"))
	if fb.count() != 1 {
		t.Error("with Redis down the event must go straight to the database")
	}
}

func TestRecorderNeverPanicsWhenEverythingIsDown(t *testing.T) {
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: 0, DialTimeout: 100 * time.Millisecond})
	fb := newFakeWriter()
	fb.alwaysErr = errors.New("database is down too")
	NewRecorder(NewStream(dead, 1000), fb, 300*time.Millisecond).Record(context.Background(), sampleEvent("rec-3"))
	if fb.calls != 1 {
		t.Errorf("the fallback should have been tried once, calls=%d", fb.calls)
	}
}

func TestRecorderStillWorksAfterTheClientLeft(t *testing.T) {
	rdb := newRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the client is gone
	NewRecorder(NewStream(rdb, 1000), newFakeWriter(), time.Second).Record(ctx, sampleEvent("rec-4"))
	if n := rdb.XLen(context.Background(), StreamKey).Val(); n != 1 {
		t.Error("a cancelled request context must not stop the event from being recorded")
	}
}

func TestStreamIsTrimmedToTheMaximum(t *testing.T) {
	rdb := newRedis(t)
	s := NewStream(rdb, 100) // approximate trimming works in blocks, so allow some room
	for i := 0; i < 1500; i++ {
		s.Publish(context.Background(), sampleEvent(fmt.Sprintf("trim-%d", i)))
	}
	if n := rdb.XLen(context.Background(), StreamKey).Val(); n > 1200 {
		t.Errorf("the stream must stay small, has %d entries", n)
	}
}
