package sub

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nileshkr9919/tributary/internal/topic"
)

// Status is the outcome of an ack, nack or extend.
type Status string

const (
	StatusOK         Status = "ok"
	StatusSuperseded Status = "superseded"
	StatusCancelled  Status = "cancelled"
	StatusPaused     Status = "paused"
	StatusLost       Status = "lost"
)

// Message states.
const (
	statePending      = "pending"
	stateDelayed      = "delayed"
	stateLeased       = "leased"
	stateHeld         = "held"
	stateAcked        = "acked"
	stateDeadLettered = "dead-lettered"
	stateDropped      = "dropped"
	stateSuperseded   = "superseded"
	stateCancelled    = "cancelled"
)

var (
	ErrCheckpointTooLarge = errors.New("checkpoint exceeds the subscription's checkpoint limit")
	ErrNotExtendable      = errors.New("leases on this subscription are not extendable")
	ErrTopicNotFound      = errors.New("topic not found")
)

func terminal(state string) bool {
	switch state {
	case stateAcked, stateDeadLettered, stateDropped, stateSuperseded, stateCancelled:
		return true
	}
	return false
}

// record is the persisted state of one message for one subscription.
type record struct {
	State      string          `json:"s"`
	Attempt    int             `json:"a,omitempty"`
	LeaseID    uint64          `json:"l,omitempty"`
	Holder     string          `json:"h,omitempty"`
	PrevHolder string          `json:"ph,omitempty"`
	LeaseStart time.Time       `json:"ls,omitempty"`
	Deadline   time.Time       `json:"d,omitempty"`
	NotBefore  time.Time       `json:"nb,omitempty"`
	Eligible   time.Time       `json:"e,omitempty"`
	Expiries   int             `json:"x,omitempty"`
	Checkpoint json.RawMessage `json:"c,omitempty"`
	Charged    float64         `json:"ch,omitempty"`
	Cost       float64         `json:"co,omitempty"`
	Throttled  time.Duration   `json:"th,omitempty"`
	Reason     string          `json:"r,omitempty"`
	Ref        string          `json:"ref,omitempty"` // dead-letter copy holding the payload, for requeued messages
	Notified   string          `json:"n,omitempty"`   // status to report to the old holder: superseded or cancelled
	Gen        int             `json:"g,omitempty"`   // how many times this message was requeued from dead letters
}

// Delivery is one leased message handed to a consumer.
type Delivery struct {
	ID             string            `json:"id"`
	Token          string            `json:"token"`
	Key            string            `json:"key,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	Payload        json.RawMessage   `json:"payload,omitempty"`
	Attempt        int               `json:"attempt"`
	OperationID    string            `json:"operation_id"`
	Priority       string            `json:"priority,omitempty"`
	Checkpoint     json.RawMessage   `json:"checkpoint,omitempty"`
	PreviousHolder string            `json:"previous_holder,omitempty"`
	WaitMs         int64             `json:"wait_ms"`
}

// DeadLetter describes a dead-lettered message.
type DeadLetter struct {
	SourceID string    `json:"id"`
	CopyID   string    `json:"copy_id"`
	Reason   string    `json:"reason"`
	Attempts int       `json:"attempts"`
	At       time.Time `json:"at"`
	State    string    `json:"state"` // dead or requeued
	Gen      int       `json:"gen,omitempty"`
}

// absent reports whether a checkpoint is missing or JSON null; neither replaces a stored one.
func absent(c []byte) bool {
	t := string(c)
	return len(c) == 0 || t == "null"
}

// NackRequest describes a negative acknowledgement.
type NackRequest struct {
	Reason     string             `json:"reason"`
	RetryAfter time.Duration      `json:"-"`
	Scope      string             `json:"scope"`
	Checkpoint json.RawMessage    `json:"checkpoint"`
	Error      string             `json:"error"`
	Usage      map[string]float64 `json:"usage"`
}

// Pauser is implemented by gates that can pause the pool a subscription draws from.
// A zero until means paused until resumed by an operator.
type Pauser interface {
	PausePool(sub string, until time.Time) bool
}

// Gate admits dispatches against quota and settles reported usage.
type Gate interface {
	Reserve(sub string, m topic.Message) (charge float64, ok bool, retryAt time.Time)
	Settle(sub string, charged float64, grantedAt time.Time, usage map[string]float64)
}

// Unlimited is a Gate that admits everything.
type Unlimited struct{}

func (Unlimited) Reserve(string, topic.Message) (float64, bool, time.Time) {
	return 0, true, time.Time{}
}
func (Unlimited) Settle(string, float64, time.Time, map[string]float64) {}

type pos struct {
	shard  int
	offset uint64
}

type tokenParts struct {
	sub     string
	p       pos
	attempt int
	leaseID uint64
}

func encodeToken(sub string, p pos, attempt int, leaseID uint64) string {
	raw := fmt.Sprintf("%s|%d|%d|%d|%d", sub, p.shard, p.offset, attempt, leaseID)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeToken(tok string) (tokenParts, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return tokenParts{}, false
	}
	f := strings.Split(string(raw), "|")
	if len(f) < 5 {
		return tokenParts{}, false
	}
	n := len(f)
	sh, e1 := strconv.Atoi(f[n-4])
	off, e2 := strconv.ParseUint(f[n-3], 10, 64)
	at, e3 := strconv.Atoi(f[n-2])
	lid, e4 := strconv.ParseUint(f[n-1], 10, 64)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
		return tokenParts{}, false
	}
	return tokenParts{sub: strings.Join(f[:n-4], "|"), p: pos{sh, off}, attempt: at, leaseID: lid}, true
}

// TokenSubscription returns the subscription named in a token, or "" if malformed.
func TokenSubscription(tok string) string {
	tp, ok := decodeToken(tok)
	if !ok {
		return ""
	}
	return tp.sub
}
