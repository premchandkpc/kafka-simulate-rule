package domain

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
)

var (
	pathPattern = regexp.MustCompile(`^\$[\.\[]+`)
)

func init() {
	_ = pathPattern
}

func EventNow() time.Time { return time.Now().UTC() }

type EventEnvelope struct {
	ID           string            `json:"id"`
	Type         string            `json:"type"`
	TenantID     string            `json:"tenant_id"`
	PartitionKey string            `json:"partition_key"`
	OccurredAt   time.Time         `json:"occurred_at"`
	Data         json.RawMessage   `json:"data"`
	Headers      map[string]string `json:"headers,omitempty"`
}

func (e *EventEnvelope) VirtualShard(modulus uint32) uint32 {
	key := e.TenantID + ":" + e.PartitionKey
	h := fnv32a(key)
	return h % modulus
}

func (e *EventEnvelope) Validate() error {
	if e.ID == "" {
		return ErrEventIDRequired
	}
	if e.Type == "" {
		return ErrEventTypeRequired
	}
	if e.TenantID == "" {
		return ErrTenantIDRequired
	}
	if e.PartitionKey == "" {
		return ErrPartitionKeyRequired
	}
	if e.OccurredAt.IsZero() {
		return ErrOccurredAtRequired
	}
	return nil
}

type MatchMode string

const (
	MatchModeFirstMatch MatchMode = "first_match"
	MatchModeAllMatches MatchMode = "all_matches"
)

type RuleRevision struct {
	RuleID          string          `json:"rule_id"`
	Revision        int64           `json:"revision"`
	ContentHash     string          `json:"content_hash"`
	MatchMode       MatchMode       `json:"match_mode"`
	Compiled        []CompiledRule  `json:"compiled"`
	Source          json.RawMessage `json:"source"`
	CompilerVersion string          `json:"compiler_version"`
	CreatedAt       time.Time       `json:"created_at"`
}

type CompiledRule struct {
	ID        string      `json:"id"`
	Priority  int         `json:"priority"`
	When      Predicate   `json:"when"`
	Then      []Action    `json:"then"`
	Otherwise []Action    `json:"otherwise,omitempty"`
}

type Predicate struct {
	All   []*Predicate `json:"all,omitempty"`
	Any   []*Predicate `json:"any,omitempty"`
	Not   *Predicate   `json:"not,omitempty"`
	Path  string        `json:"path,omitempty"`
	Op    Operator      `json:"op,omitempty"`
	Value interface{}   `json:"value,omitempty"`
}

type Operator string

const (
	OpEq        Operator = "eq"
	OpNeq       Operator = "neq"
	OpGt        Operator = "gt"
	OpGte       Operator = "gte"
	OpLt        Operator = "lt"
	OpLte       Operator = "lte"
	OpIn        Operator = "in"
	OpNotIn     Operator = "not_in"
	OpExists    Operator = "exists"
	OpNotExists Operator = "not_exists"
)

func ValidOp(op Operator) bool {
	switch op {
	case OpEq, OpNeq, OpGt, OpGte, OpLt, OpLte, OpIn, OpNotIn, OpExists, OpNotExists:
		return true
	default:
		return false
	}
}

type Action struct {
	Emit    *EmitAction    `json:"emit,omitempty"`
	Command *CommandAction `json:"command,omitempty"`
}

type EmitAction struct {
	Topic string          `json:"topic"`
	Data  json.RawMessage `json:"data"`
}

type CommandAction struct {
	Destination string          `json:"destination"`
	Name        string          `json:"name"`
	Data        json.RawMessage `json:"data"`
}

type Effect struct {
	ID          string          `json:"id"`
	ExecutionID string          `json:"execution_id"`
	Destination string          `json:"destination"`
	Name        string          `json:"name"`
	Payload     json.RawMessage `json:"payload"`
	EffectType  EffectType      `json:"effect_type"`
	CreatedAt   time.Time       `json:"created_at"`
}

type EffectType string

const (
	EffectTypeEmit    EffectType = "emit"
	EffectTypeCommand EffectType = "command"
)

type Decision struct {
	MatchedRules []string `json:"matched_rules"`
	Effects      []Effect `json:"effects"`
	Hash         string   `json:"hash"`
}

type Execution struct {
	ID           string          `json:"id"`
	EventID      string          `json:"event_id"`
	TenantID     string          `json:"tenant_id"`
	RuleSet      string          `json:"rule_set"`
	Revision     int64           `json:"revision"`
	DecisionHash string          `json:"decision_hash"`
	Status       ExecutionStatus `json:"status"`
	Error        string          `json:"error,omitempty"`
	TraceID      string          `json:"trace_id,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
}

type ExecutionStatus string

const (
	ExecutionStatusPending     ExecutionStatus = "pending"
	ExecutionStatusCompleted   ExecutionStatus = "completed"
	ExecutionStatusFailed      ExecutionStatus = "failed"
	ExecutionStatusQuarantined ExecutionStatus = "quarantined"
)

type OutboxEffect struct {
	ID          string          `json:"id"`
	ExecutionID string          `json:"execution_id"`
	Destination string          `json:"destination"`
	Name        string          `json:"name"`
	Payload     json.RawMessage `json:"payload"`
	EffectType  EffectType      `json:"effect_type"`
	Status      OutboxStatus    `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	AvailableAt time.Time       `json:"available_at"`
	LastError   string          `json:"last_error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type OutboxStatus string

const (
	OutboxStatusPending     OutboxStatus = "pending"
	OutboxStatusDelivered   OutboxStatus = "delivered"
	OutboxStatusFailed      OutboxStatus = "failed"
	OutboxStatusQuarantined OutboxStatus = "quarantined"
)

type InboxEntry struct {
	TenantID    string       `json:"tenant_id"`
	EventID     string       `json:"event_id"`
	Status      InboxStatus  `json:"status"`
	ExecutionID string       `json:"execution_id,omitempty"`
	FirstSeenAt time.Time    `json:"first_seen_at"`
	CommittedAt *time.Time   `json:"committed_at,omitempty"`
}

type InboxStatus string

const (
	InboxStatusProcessing InboxStatus = "processing"
	InboxStatusCommitted  InboxStatus = "committed"
)

type ShardLease struct {
	VirtualShard uint32    `json:"virtual_shard"`
	Owner        string    `json:"owner"`
	FencingToken int64     `json:"fencing_token"`
	ExpiresAt    time.Time `json:"expires_at"`
	RoutingEpoch int64     `json:"routing_epoch"`
}

type QuarantineEntry struct {
	ID         string    `json:"id"`
	SourceType string    `json:"source_type"`
	SourceID   string    `json:"source_id"`
	ErrorClass string    `json:"error_class"`
	PayloadRef string    `json:"payload_ref"`
	CreatedAt  time.Time `json:"created_at"`
}

type RuleActivation struct {
	TenantScope string    `json:"tenant_scope"`
	RuleSet     string    `json:"rule_set"`
	Revision    int64     `json:"revision"`
	Version     int64     `json:"version"`
	Actor       string    `json:"actor"`
	ActivatedAt time.Time `json:"activated_at"`
}

func fnv32a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func NewID() string { return uuid.New().String() }

func ComputeEffectID(tenantID, eventID, ruleSet string, revision int64, ruleID string, actionIndex int) string {
	raw := fmt.Sprintf("%s|%s|%s|%d|%s|%d", tenantID, eventID, ruleSet, revision, ruleID, actionIndex)
	h := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", h)
}

func ComputeDecisionHash(revisionHash, eventID, factsHash string, matched []string, effects []Effect) string {
	for _, id := range matched {
		revisionHash += id
	}
	for _, ef := range effects {
		revisionHash += ef.ID
	}
	h := sha256.Sum256([]byte(revisionHash + eventID + factsHash))
	return fmt.Sprintf("%x", h)
}

func ComputeSourceHash(source json.RawMessage) string {
	h := sha256.Sum256(source)
	return fmt.Sprintf("%x", h)
}