package application

import "github.com/flowrule/flowrule/internal/ports"

// TxRepos holds repository instances scoped to a single transaction.
type TxRepos struct {
	Inbox       ports.InboxRepository
	Activations ports.ActivationRepository
	RuleRepo    ports.RuleRepository
	Executions  ports.ExecutionRepository
	Outbox      ports.OutboxRepository
}

// RepoFactory creates transaction-scoped repositories from a database querier.
// The concrete querier type is an adapter concern (e.g., sql.Tx implements this).
type RepoFactory func(db interface{}) TxRepos
