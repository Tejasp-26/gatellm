package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"gatellm/internal/cache"
	"gatellm/internal/provider"
	"gatellm/internal/router"
)

// flightTimeout is the longest we wait for the provider on behalf of a group of identical requests.
const flightTimeout = 2 * time.Minute

// flightResult is what the one real call hands to everybody who waited for it.
type flightResult struct {
	resp       *provider.ChatResponse
	sv         router.Served
	hit        string  // "" = the provider answered, otherwise "HIT-EXACT" or "HIT-SEMANTIC" (found while we were the leader)
	similarity float64 // only for HIT-SEMANTIC
}

// flightError carries who was tried, so every waiting request can write the same error.
type flightError struct {
	sv  router.Served
	err error
}

func (e *flightError) Error() string { return e.err.Error() }
func (e *flightError) Unwrap() error { return e.err }

// chatCached answers a cacheable request (not streaming, temperature 0).
//
//  1. Look in the cache. Found -> answer at once, no provider call, no cost.
//  2. Not found -> ask the provider. If 10 identical requests arrive at the same time,
//     singleflight makes only ONE of them (the "leader") call the provider.
//     The other 9 ("followers") wait and get the same answer.
//  3. The leader stores the answer in the cache.
func (h *Handler) chatCached(w http.ResponseWriter, r *http.Request, be backend, req *provider.ChatRequest, clientModel string, res *reservation) {
	tenantID := ""
	if t := TenantFrom(r.Context()); t != nil {
		tenantID = t.ID
	}
	key := cache.Key(tenantID, clientModel, req)

	// 1. Look in the cache. A Redis error is not a reason to fail the request: treat it as a miss.
	if entry, hit := h.cacheGet(r.Context(), key); hit {
		res.refund() // nothing was used, give the reserved tokens back (the request still counts for rpm)
		h.recordUsage(r.Context(), usageInfo{provider: entry.Provider, model: entry.Model, cacheStatus: "HIT-EXACT", status: "ok"})
		h.writeCached(w, be, entry.Response, router.Served{Provider: entry.Provider, Model: entry.Model}, "HIT-EXACT", false)
		return
	}

	// 2. Miss. Only one identical request at a time goes to the provider.
	leader := false
	v, err, _ := h.flights.Do(key, func() (any, error) {
		leader = true // only the goroutine that really runs this function sets it
		// Run on our own context: if the leader's client leaves, the followers still need the answer.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), flightTimeout)
		defer cancel()

		// Double check: another request may have filled the cache just before we became the leader.
		if entry, hit := h.cacheGet(ctx, key); hit {
			return &flightResult{resp: entry.Response, sv: router.Served{Provider: entry.Provider, Model: entry.Model}, hit: "HIT-EXACT"}, nil
		}

		// Semantic cache: only the leader asks, so 10 identical requests cost one embedding call.
		sem := h.semanticLookup(ctx, r, tenantID, clientModel, req)
		if sem.entry != nil {
			// Remember it for this exact wording too, so the next identical request is a cheap exact hit.
			h.cacheSet(ctx, r, key, sem.entry)
			return &flightResult{
				resp: sem.entry.Response, sv: router.Served{Provider: sem.entry.Provider, Model: sem.entry.Model},
				hit: "HIT-SEMANTIC", similarity: sem.similarity,
			}, nil
		}

		resp, sv, err := be.chat(ctx, req)
		h.noteFallback(sv, err)
		if err != nil {
			res.refund() // nothing was used
			h.recordUsage(r.Context(), usageInfo{provider: sv.Provider, model: sv.Model, cacheStatus: "MISS", status: "error"})
			return nil, &flightError{sv: sv, err: err}
		}

		// Same accounting as a normal request: real tokens and real cost, once.
		answerChars := 0
		if len(resp.Choices) > 0 {
			answerChars = utf8.RuneCountInString(resp.Choices[0].Message.Content)
		}
		res.settle(&resp.Usage, answerChars)
		h.recordSpend(r.Context(), sv.Provider, sv.Model, &resp.Usage, estimatePromptTokens(req), answerChars)
		h.recordUsage(r.Context(), usageInfo{provider: sv.Provider, model: sv.Model, cacheStatus: "MISS",
			status: "ok", charged: true, u: &resp.Usage, promptEstimate: estimatePromptTokens(req), answerChars: answerChars})

		// Store the answer for next time (only real answers).
		if len(resp.Choices) > 0 {
			entry := &cache.Entry{Provider: sv.Provider, Model: sv.Model, Response: resp}
			h.cacheSet(ctx, r, key, entry)
			sem.store(h, ctx, r, entry)
		}
		return &flightResult{resp: resp, sv: sv}, nil
	})

	// 3. Write the answer.
	if err != nil {
		var fe *flightError
		sv := router.Served{}
		if errors.As(err, &fe) {
			sv = fe.sv
		}
		if !leader {
			res.refund() // a follower used nothing (the leader refunded its own)
			h.recordUsage(r.Context(), usageInfo{provider: sv.Provider, model: sv.Model, cacheStatus: "COALESCED", status: "error"})
		}
		setRouteHeader(w, be, sv)
		w.Header().Set("X-Cache", "MISS")
		if !leader {
			w.Header().Set("X-Coalesced", "true")
		}
		slog.Warn("provider call failed",
			"request_id", RequestIDFrom(r.Context()), "tenant_id", tenantID,
			"provider", sv.Provider, "tried", sv.Tried, "coalesced", !leader, "err", err)
		writeUpstreamError(w, sv, err)
		return
	}

	out := v.(*flightResult)
	status := "MISS"
	if out.hit != "" {
		status = out.hit
	}
	if !leader || out.hit != "" {
		res.refund() // the tokens we reserved were not used by us
		// This request paid nothing: it got an answer from the cache or from the leader's call.
		recorded := status
		if out.hit == "" {
			recorded = "COALESCED"
		}
		h.recordUsage(r.Context(), usageInfo{provider: out.sv.Provider, model: out.sv.Model, cacheStatus: recorded, status: "ok"})
	}
	if out.hit == "HIT-SEMANTIC" {
		w.Header().Set("X-Cache-Similarity", strconv.FormatFloat(out.similarity, 'f', 4, 64))
	}
	h.writeCached(w, be, out.resp, out.sv, status, !leader)
}

// cacheGet reads the cache. Any problem is logged and counted as "not found":
// the cache is only a speed-up, it must never break a request.
func (h *Handler) cacheGet(ctx context.Context, key string) (*cache.Entry, bool) {
	entry, hit, err := h.Cache.Get(ctx, key)
	if err != nil {
		slog.Warn("cache read failed, treating it as a miss", "request_id", RequestIDFrom(ctx), "err", err)
		return nil, false
	}
	return entry, hit
}

// cacheSet stores an answer in the exact cache. A problem is only logged.
func (h *Handler) cacheSet(ctx context.Context, r *http.Request, key string, entry *cache.Entry) {
	if err := h.Cache.Set(ctx, key, entry); err != nil {
		slog.Warn("could not store answer in the cache", "request_id", RequestIDFrom(r.Context()), "err", err)
	}
}

// semantic remembers the result of the semantic lookup, so a miss can store the new answer
// afterwards without embedding the prompt a second time.
type semantic struct {
	entry      *cache.Entry // set on a hit
	similarity float64
	tenantID   string
	scope      string
	text       string
	vec        []float32 // nil = the semantic cache is not used for this request
}

// semanticLookup embeds the prompt and looks for a similar stored question.
// Every problem (no embedding, database down) is logged and counted as a miss:
// the semantic cache is a speed-up, it must never break a request.
func (h *Handler) semanticLookup(ctx context.Context, r *http.Request, tenantID, clientModel string, req *provider.ChatRequest) *semantic {
	s := &semantic{tenantID: tenantID}
	if h.Semantic == nil || h.Embedder == nil {
		return s
	}
	text, ok := cache.PromptText(req)
	if !ok {
		return s // too long for the semantic cache
	}
	vec, err := h.Embedder.Embed(ctx, text)
	if err != nil {
		slog.Warn("embedding failed, skipping the semantic cache", "request_id", RequestIDFrom(r.Context()), "err", err)
		return s
	}
	s.scope, s.text, s.vec = cache.Scope(clientModel, req), text, vec

	entry, sim, hit, err := h.Semantic.Find(ctx, tenantID, s.scope, vec)
	if err != nil {
		slog.Warn("semantic cache read failed, treating it as a miss", "request_id", RequestIDFrom(r.Context()), "err", err)
		return s
	}
	// The similarity is logged, the prompt is not.
	slog.Info("semantic lookup", "request_id", RequestIDFrom(r.Context()), "tenant_id", tenantID, "similarity", sim, "hit", hit)
	if hit {
		s.entry, s.similarity = entry, sim
	}
	return s
}

// store saves a fresh answer in the semantic cache (only if the lookup could embed the prompt).
func (s *semantic) store(h *Handler, ctx context.Context, r *http.Request, entry *cache.Entry) {
	if s.vec == nil {
		return
	}
	if err := h.Semantic.Add(ctx, s.tenantID, s.scope, s.text, s.vec, entry); err != nil {
		slog.Warn("could not store answer in the semantic cache", "request_id", RequestIDFrom(r.Context()), "err", err)
	}
}

// writeCached sends the answer with the cache headers.
func (h *Handler) writeCached(w http.ResponseWriter, be backend, resp *provider.ChatResponse, sv router.Served, status string, coalesced bool) {
	setRouteHeader(w, be, sv)
	w.Header().Set("X-Provider", sv.Provider)
	w.Header().Set("X-Cache", status)
	if coalesced {
		w.Header().Set("X-Coalesced", "true")
	}
	writeJSON(w, http.StatusOK, resp)
}
