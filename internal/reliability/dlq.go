package reliability

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/premchand/flowrule/internal/executor"
)

// DLQSnapshot is a serializable record of a failed message.
type DLQSnapshot struct {
	ID            uint64                `json:"id"`
	CorrelationID string                `json:"correlation_id"`
	RuleID        string                `json:"rule_id"`
	PlanVersion   int64                 `json:"plan_version"`
	Body          []byte                `json:"body"`
	ContentType   string                `json:"content_type"`
	Headers       map[string]string     `json:"headers"`
	ReceivedAt    time.Time             `json:"received_at"`
	FailedAt      time.Time             `json:"failed_at"`
	FailedStage   string                `json:"failed_stage"`
	ErrorChain    []executor.StageError `json:"error_chain"`
	RetryCount    int                   `json:"retry_count"`
}

// DLQ persists failed messages via Badger.
type DLQ struct {
	db *badger.DB
}

// NewDLQ creates a new DLQ backed by Badger.
func NewDLQ(dir string) (*DLQ, error) {
	opts := badger.DefaultOptions(dir).
		WithLogger(nil).
		WithLoggingLevel(badger.WARNING)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("dlq: open badger: %w", err)
	}

	return &DLQ{db: db}, nil
}

// Push persists a DLQSnapshot with a 72-hour TTL.
func (q *DLQ) Push(snap *DLQSnapshot) error {
	key := fmt.Sprintf("dlq:%s:%016x:%s",
		snap.RuleID, time.Now().UnixNano(), snap.CorrelationID)
	val, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("dlq.Push marshal: %w", err)
	}
	return q.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(key), val).WithTTL(72 * time.Hour)
		return txn.SetEntry(e)
	})
}

// List returns DLQ entries for a given rule ID.
func (q *DLQ) List(ruleID string, limit int) ([]*DLQSnapshot, error) {
	prefix := []byte("dlq:" + ruleID + ":")
	var results []*DLQSnapshot
	err := q.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid() && len(results) < limit; it.Next() {
			item := it.Item()
			var snap DLQSnapshot
			if err := item.Value(func(v []byte) error {
				return json.Unmarshal(v, &snap)
			}); err != nil {
				continue
			}
			results = append(results, &snap)
		}
		return nil
	})
	return results, err
}

// Replay replays DLQ entries for a given rule ID.
func (q *DLQ) Replay(ctx context.Context, ruleID string, submitFn func(*DLQSnapshot) error) (int, error) {
	snaps, err := q.List(ruleID, 1000)
	if err != nil {
		return 0, fmt.Errorf("dlq.Replay list: %w", err)
	}
	replayed := 0
	for _, snap := range snaps {
		if err := submitFn(snap); err != nil {
			continue
		}
		key := fmt.Sprintf("dlq:%s:%016x:%s", snap.RuleID, snap.FailedAt.UnixNano(), snap.CorrelationID)
		_ = q.db.Update(func(txn *badger.Txn) error {
			return txn.Delete([]byte(key))
		})
		replayed++
	}
	return replayed, nil
}

// Close closes the underlying Badger database.
func (q *DLQ) Close() error {
	return q.db.Close()
}
