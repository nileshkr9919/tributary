// Package api serves Tributary's HTTP/JSON API.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/nileshkr9919/tributary/internal/broker"
	"github.com/nileshkr9919/tributary/internal/quota"
	"github.com/nileshkr9919/tributary/internal/settings"
	"github.com/nileshkr9919/tributary/internal/sub"
	"github.com/nileshkr9919/tributary/internal/topic"
)

const maxBody = 64 << 20

type api struct{ b *broker.Broker }

// Handler returns the HTTP handler for a broker.
func Handler(b *broker.Broker) http.Handler {
	a := &api{b: b}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /metrics", a.metrics)
	mux.HandleFunc("POST /v1/topics", a.createTopic)
	mux.HandleFunc("POST /v1/topics/{topic}/messages", a.produce)
	mux.HandleFunc("POST /v1/pools", a.createPool)
	mux.HandleFunc("GET /v1/pools", a.listPools)
	mux.HandleFunc("POST /v1/pools/{pool}/resume", a.resumePool)
	mux.HandleFunc("POST /v1/subscriptions", a.createSub)
	mux.HandleFunc("GET /v1/subscriptions/{sub}", a.getSub)
	mux.HandleFunc("POST /v1/subscriptions/{sub}/receive", a.receive)
	mux.HandleFunc("POST /v1/subscriptions/{sub}/cancel", a.cancel)
	mux.HandleFunc("POST /v1/subscriptions/{sub}/pause", a.pause(true))
	mux.HandleFunc("POST /v1/subscriptions/{sub}/resume", a.pause(false))
	mux.HandleFunc("GET /v1/subscriptions/{sub}/dlq", a.dlq)
	mux.HandleFunc("POST /v1/subscriptions/{sub}/dlq/requeue", a.requeue)
	mux.HandleFunc("POST /v1/ack", a.ack)
	mux.HandleFunc("POST /v1/nack", a.nack)
	mux.HandleFunc("POST /v1/extend", a.extend)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// failErr maps domain errors to HTTP status codes.
func failErr(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, topic.ErrNotFound), errors.Is(err, sub.ErrTopicNotFound), errors.Is(err, broker.ErrSubNotFound),
		errors.Is(err, broker.ErrPoolNotFound), errors.Is(err, quota.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, topic.ErrExists), errors.Is(err, broker.ErrSubExists), errors.Is(err, quota.ErrExists):
		code = http.StatusConflict
	case errors.Is(err, sub.ErrCheckpointTooLarge), errors.Is(err, broker.ErrPayloadTooBig):
		code = http.StatusRequestEntityTooLarge
	case errors.Is(err, settings.ErrInvalid), errors.Is(err, settings.ErrMissingParam), errors.Is(err, settings.ErrUnknownProfile),
		errors.Is(err, topic.ErrShards), errors.Is(err, quota.ErrInvalid), errors.Is(err, sub.ErrNotExtendable),
		errors.Is(err, broker.ErrEmptyName), errors.Is(err, broker.ErrBatchTooLarge), errors.Is(err, topic.ErrBadID),
		errors.Is(err, topic.ErrBadName):
		code = http.StatusBadRequest
	}
	fail(w, code, err.Error())
}

// decode reads a JSON body; an empty body decodes as {}.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(v); err != nil && err.Error() != "EOF" {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			fail(w, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			fail(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		}
		return false
	}
	return true
}

func (a *api) createTopic(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name   string `json:"name"`
		Shards int    `json:"shards"`
	}
	if !decode(w, r, &req) {
		return
	}
	if err := a.b.CreateTopic(req.Name, req.Shards); err != nil {
		failErr(w, err)
		return
	}
	n, _ := a.b.Topics.Shards(req.Name)
	writeJSON(w, http.StatusCreated, map[string]any{"name": req.Name, "shards": n})
}

func (a *api) produce(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Messages []struct {
			Key            string            `json:"key"`
			Headers        map[string]string `json:"headers"`
			Payload        json.RawMessage   `json:"payload"`
			Priority       string            `json:"priority"`
			DeliverAtMs    int64             `json:"deliver_at_ms"`
			IdempotencyKey string            `json:"idempotency_key"`
		} `json:"messages"`
	}
	if !decode(w, r, &req) {
		return
	}
	in := make([]topic.Input, len(req.Messages))
	for i, m := range req.Messages {
		in[i] = topic.Input{Key: m.Key, Headers: m.Headers, Payload: m.Payload, Priority: m.Priority, IdempotencyKey: m.IdempotencyKey}
		if m.DeliverAtMs > 0 {
			in[i].DeliverAt = time.UnixMilli(m.DeliverAtMs)
		}
	}
	ms, err := a.b.Produce(r.PathValue("topic"), in)
	if err != nil {
		failErr(w, err)
		return
	}
	ids := make([]string, len(ms))
	for i, m := range ms {
		ids[i] = m.ID
	}
	writeJSON(w, http.StatusOK, map[string]any{"ids": ids})
}

func (a *api) createPool(w http.ResponseWriter, r *http.Request) {
	var req quota.PoolConfig
	if !decode(w, r, &req) {
		return
	}
	if req.Unit == "" {
		req.Unit = "tokens"
	}
	if req.Burst == 0 && req.Mode != "window" {
		req.Burst = req.PerMinute / 10
	}
	if err := a.b.Pools.CreateWith(req); err != nil {
		failErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, a.b.Pools.Stats(req.Name))
}

func (a *api) listPools(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"pools": a.b.Pools.List()})
}

func (a *api) resumePool(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("pool")
	if !a.b.Pools.Exists(name) {
		fail(w, http.StatusNotFound, "pool not found")
		return
	}
	a.b.Pools.Resume(name)
	writeJSON(w, http.StatusOK, a.b.Pools.Stats(name))
}

func (a *api) createSub(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name      string            `json:"name"`
		Topic     string            `json:"topic"`
		Profile   string            `json:"profile"`
		Params    map[string]string `json:"params"`
		Overrides json.RawMessage   `json:"overrides"`
	}
	if !decode(w, r, &req) {
		return
	}
	s, err := a.b.CreateSubscription(req.Name, req.Topic, req.Profile, req.Params, req.Overrides)
	if err != nil {
		failErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, subView(s))
}

func subView(s *sub.Subscription) map[string]any {
	return map[string]any{
		"name": s.Name(), "topic": s.Topic(), "settings": s.Settings(), "counts": s.Counts(),
		"dead_letter_topic": sub.DeadLetterTopic(s.Topic(), s.Name()),
	}
}

func (a *api) subFor(w http.ResponseWriter, r *http.Request) *sub.Subscription {
	s, err := a.b.Sub(r.PathValue("sub"))
	if err != nil {
		failErr(w, err)
		return nil
	}
	return s
}

func (a *api) getSub(w http.ResponseWriter, r *http.Request) {
	if s := a.subFor(w, r); s != nil {
		writeJSON(w, http.StatusOK, subView(s))
	}
}

func (a *api) receive(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Member string `json:"member"`
		Max    int    `json:"max"`
		WaitMs int64  `json:"wait_ms"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.Max <= 0 {
		req.Max = 1
	}
	if req.Max > 1000 {
		req.Max = 1000
	}
	if req.Member == "" {
		req.Member = r.RemoteAddr
	}
	ds, err := a.b.Receive(r.Context(), r.PathValue("sub"), req.Member, req.Max, time.Duration(req.WaitMs)*time.Millisecond)
	if err != nil {
		failErr(w, err)
		return
	}
	if ds == nil {
		ds = []sub.Delivery{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": ds})
}

func (a *api) ack(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Acks []struct {
			Token string             `json:"token"`
			Usage map[string]float64 `json:"usage"`
		} `json:"acks"`
	}
	if !decode(w, r, &req) {
		return
	}
	results := make([]map[string]any, 0, len(req.Acks))
	for _, x := range req.Acks {
		st := sub.StatusLost
		if s := a.b.SubForToken(x.Token); s != nil {
			var err error
			if st, err = s.Ack(x.Token, x.Usage); err != nil {
				failErr(w, err)
				return
			}
		}
		results = append(results, map[string]any{"token": x.Token, "status": st})
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func (a *api) nack(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token        string             `json:"token"`
		Reason       string             `json:"reason"`
		RetryAfterMs int64              `json:"retry_after_ms"`
		Scope        string             `json:"scope"`
		Checkpoint   json.RawMessage    `json:"checkpoint"`
		Error        string             `json:"error"`
		Usage        map[string]float64 `json:"usage"`
	}
	if !decode(w, r, &req) {
		return
	}
	s := a.b.SubForToken(req.Token)
	if s == nil {
		writeJSON(w, http.StatusOK, map[string]any{"status": sub.StatusLost})
		return
	}
	st, err := s.Nack(req.Token, sub.NackRequest{Reason: req.Reason, RetryAfter: time.Duration(req.RetryAfterMs) * time.Millisecond,
		Scope: req.Scope, Checkpoint: req.Checkpoint, Error: req.Error, Usage: req.Usage})
	if err != nil {
		failErr(w, err)
		return
	}
	a.b.RecordNack(s.Name(), req.Reason)
	writeJSON(w, http.StatusOK, map[string]any{"status": st})
}

func (a *api) extend(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token      string          `json:"token"`
		DurationMs int64           `json:"duration_ms"`
		Checkpoint json.RawMessage `json:"checkpoint"`
	}
	if !decode(w, r, &req) {
		return
	}
	s := a.b.SubForToken(req.Token)
	if s == nil {
		writeJSON(w, http.StatusOK, map[string]any{"status": sub.StatusLost})
		return
	}
	st, err := s.Extend(req.Token, time.Duration(req.DurationMs)*time.Millisecond, req.Checkpoint)
	if err != nil {
		failErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": st})
}

func (a *api) cancel(w http.ResponseWriter, r *http.Request) {
	s := a.subFor(w, r)
	if s == nil {
		return
	}
	var req struct {
		ID  string `json:"id"`
		Key string `json:"key"`
	}
	if !decode(w, r, &req) {
		return
	}
	n := 0
	switch {
	case req.ID != "":
		n = s.Cancel(req.ID)
	case req.Key != "":
		n = s.CancelKey(req.Key)
	default:
		fail(w, http.StatusBadRequest, "id or key is required")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cancelled": n})
}

func (a *api) pause(on bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s := a.subFor(w, r)
		if s == nil {
			return
		}
		var req struct {
			FairnessValue string `json:"fairness_value"`
		}
		if !decode(w, r, &req) {
			return
		}
		var err error
		if on {
			err = s.Pause(req.FairnessValue)
		} else {
			err = s.Resume(req.FairnessValue)
		}
		if err != nil {
			failErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"paused": on, "fairness_value": req.FairnessValue})
	}
}

func (a *api) dlq(w http.ResponseWriter, r *http.Request) {
	if s := a.subFor(w, r); s != nil {
		dl := s.DeadLetters()
		if dl == nil {
			dl = []sub.DeadLetter{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"dead_letters": dl})
	}
}

func (a *api) requeue(w http.ResponseWriter, r *http.Request) {
	s := a.subFor(w, r)
	if s == nil {
		return
	}
	var req struct {
		IDs []string `json:"ids"`
	}
	if !decode(w, r, &req) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"requeued": s.Requeue(req.IDs)})
}
