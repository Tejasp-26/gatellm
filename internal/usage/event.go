package usage

import (
	"fmt"
	"strconv"
	"time"
)

// Event is one line of usage: what one request used and what it cost.
// It never contains the prompt or the answer, only numbers and names.
type Event struct {
	RequestID        string // unique per request, it is the idempotency key
	TenantID         string
	Provider         string
	Model            string
	PromptTokens     int
	CompletionTokens int
	CostUSD          float64
	LatencyMS        int
	CacheStatus      string // MISS, HIT-EXACT, HIT-SEMANTIC, COALESCED, BYPASS or OFF
	Status           string // ok, error, cancelled or stream_error
	CreatedAt        time.Time
}

// fields turns the event into the key/value pairs that we put in the Redis Stream.
func (e Event) fields() map[string]any {
	return map[string]any{
		"request_id":        e.RequestID,
		"tenant_id":         e.TenantID,
		"provider":          e.Provider,
		"model":             e.Model,
		"prompt_tokens":     e.PromptTokens,
		"completion_tokens": e.CompletionTokens,
		"cost_usd":          strconv.FormatFloat(e.CostUSD, 'f', -1, 64),
		"latency_ms":        e.LatencyMS,
		"cache_status":      e.CacheStatus,
		"status":            e.Status,
		"created_at":        e.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
}

// parseEvent is the opposite of fields. Redis gives every value back as a string.
// A message that cannot be parsed is "poison": retrying will never fix it.
func parseEvent(values map[string]any) (Event, error) {
	str := func(key string) string {
		s, _ := values[key].(string)
		return s
	}
	num := func(key string) (int, error) {
		n, err := strconv.Atoi(str(key))
		if err != nil {
			return 0, fmt.Errorf("field %s is not a whole number", key)
		}
		return n, nil
	}

	e := Event{
		RequestID: str("request_id"), TenantID: str("tenant_id"),
		Provider: str("provider"), Model: str("model"),
		CacheStatus: str("cache_status"), Status: str("status"),
	}
	if e.RequestID == "" {
		return Event{}, fmt.Errorf("request_id is missing")
	}
	if !looksLikeUUID(e.TenantID) {
		return Event{}, fmt.Errorf("tenant_id is not a UUID")
	}
	var err error
	if e.PromptTokens, err = num("prompt_tokens"); err != nil {
		return Event{}, err
	}
	if e.CompletionTokens, err = num("completion_tokens"); err != nil {
		return Event{}, err
	}
	if e.LatencyMS, err = num("latency_ms"); err != nil {
		return Event{}, err
	}
	if e.CostUSD, err = strconv.ParseFloat(str("cost_usd"), 64); err != nil {
		return Event{}, fmt.Errorf("field cost_usd is not a number")
	}
	if e.CreatedAt, err = time.Parse(time.RFC3339Nano, str("created_at")); err != nil {
		return Event{}, fmt.Errorf("field created_at is not a time")
	}
	return e, nil
}

// looksLikeUUID checks the shape 8-4-4-4-12 of hex digits.
func looksLikeUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch {
		case i == 8 || i == 13 || i == 18 || i == 23:
			if c != '-' {
				return false
			}
		case !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'):
			return false
		}
	}
	return true
}
