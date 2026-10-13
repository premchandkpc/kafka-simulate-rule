package events

import (
	"context"
	"errors"
	"testing"

	"github.com/flowrule/flowrule/internal/domain"
	"github.com/flowrule/flowrule/internal/ports"
)

// countingTransactionManager records whether Process crossed the transaction
// boundary. These tests capture the current public contract before refactoring.
type countingTransactionManager struct {
	calls int
	tx    ports.Transaction
	err   error
}

func (m *countingTransactionManager) Begin(context.Context) (ports.Transaction, error) {
	m.calls++
	return m.tx, m.err
}

func (*countingTransactionManager) WithTransaction(context.Context, func(context.Context) error) error {
	return nil
}

type failingShardLeaseRepository struct {
	ports.ShardLeaseRepository
	err error
}

func (r failingShardLeaseRepository) AssertHeld(context.Context, uint32, string, int64) error {
	return r.err
}

func TestProcessRejectsNonPositiveFencingTokenBeforeBeginningTransaction(t *testing.T) {
	manager := &countingTransactionManager{tx: &eventTxStub{ctx: context.Background()}}
	service := NewService(nil, nil, nil, domain.SystemClock{}, manager, nil)

	_, err := service.Process(context.Background(), testEnvelope(), 0, 0, "worker-a")
	if !errors.Is(err, domain.ErrFencingTokenMismatch) {
		t.Fatalf("Process() error = %v, want ErrFencingTokenMismatch", err)
	}
	if manager.calls != 0 {
		t.Fatalf("Begin() called %d times, want 0 for invalid fencing token", manager.calls)
	}
}

func TestProcessValidatesShardLeaseBeforeMutatingInbox(t *testing.T) {
	ctx := context.Background()
	tx := &eventTxStub{ctx: ctx}
	inbox := &inboxStub{}
	leaseErr := errors.New("lease lost")
	repos := ports.EventTxRepos{
		Inbox:       inbox,
		ShardLeases: failingShardLeaseRepository{err: leaseErr},
	}
	manager := &countingTransactionManager{tx: tx}
	service := NewService(
		nil,
		eventEvaluatorStub{},
		nil,
		domain.SystemClock{},
		manager,
		func(ports.Transaction) (ports.EventTxRepos, error) {
			return repos, nil
		},
	)

	_, err := service.Process(ctx, testEnvelope(), 7, 12, "worker-a")
	if !errors.Is(err, leaseErr) {
		t.Fatalf("Process() error = %v, want wrapped lease error", err)
	}
	if !tx.committed {
		// The transaction must not commit after lease validation fails.
		if inbox.inserted {
			t.Fatal("inbox was mutated before shard lease validation succeeded")
		}
		return
	}
	t.Fatal("transaction committed after shard lease validation failed")
}
