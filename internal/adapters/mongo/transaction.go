package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/flowrule/flowrule/internal/ports"
)

type TransactionManager struct {
	client *mongo.Client
}

func NewTransactionManager(client *mongo.Client) *TransactionManager {
	return &TransactionManager{client: client}
}

func (m *TransactionManager) Begin(ctx context.Context) (ports.Transaction, error) {
	session, err := m.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("start session: %w", err)
	}

	err = session.StartTransaction(options.Transaction().
		SetReadConcern(options.ReadConcern().SetLevel("majority")).
		SetWriteConcern(options.WriteConcern().SetW("majority")))
	if err != nil {
		session.EndSession(context.Background())
		return nil, fmt.Errorf("start transaction: %w", err)
	}

	return &Transaction{session: session}, nil
}

func (m *TransactionManager) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return mongo.WithSession(ctx, m.client, func(sessCtx mongo.SessionContext) error {
		return fn(sessCtx)
	})
}

type Transaction struct {
	session     *mongo.Session
	fencingToken int64
}

func (t *Transaction) Commit(ctx context.Context) error {
	return (*t.session).CommitTransaction(ctx)
}

func (t *Transaction) Rollback(ctx context.Context) error {
	return (*t.session).AbortTransaction(ctx)
}

func (t *Transaction) Context() context.Context {
	return context.Background()
}

func (t *Transaction) FencingToken() int64 {
	return t.fencingToken
}

func (t *Transaction) SetFencingToken(token int64) {
	t.fencingToken = token
}

type Querier interface {
	Collection(name string) *mongo.Collection
	Session() *mongo.Session
}

type dbQuerier struct {
	db *mongo.Database
}

func (q *dbQuerier) Collection(name string) *mongo.Collection {
	return q.db.Collection(name)
}

func (q *dbQuerier) Session() *mongo.Session {
	return nil
}

type txQuerier struct {
	session *mongo.Session
}

func (q *txQuerier) Collection(name string) *mongo.Collection {
	return q.session.Client().Database("flowrule").Collection(name)
}

func (q *txQuerier) Session() *mongo.Session {
	return q.session
}

func NewQuerier(db *mongo.Database) Querier {
	return &dbQuerier{db: db}
}

func NewTxQuerier(session *mongo.Session) Querier {
	return &txQuerier{session: session}
}

type TxWrapper struct {
	*Transaction
}

func (t *TxWrapper) Collection(name string) *mongo.Collection {
	return t.session.Client().Database("flowrule").Collection(name)
}

func (t *TxWrapper) Session() *mongo.Session {
	return t.session
}

func WrapTx(tx ports.Transaction) Querier {
	if mw, ok := tx.(*Transaction); ok {
		return &TxWrapper{mw}
	}
	return nil
}