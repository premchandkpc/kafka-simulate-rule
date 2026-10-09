package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

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

	txOpts := options.Transaction().
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority()))

	err = session.StartTransaction(txOpts)
	if err != nil {
		session.EndSession(context.Background())
		return nil, fmt.Errorf("start transaction: %w", err)
	}

	return &Transaction{session: session}, nil
}

func (m *TransactionManager) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	session, err := m.client.StartSession()
	if err != nil {
		return fmt.Errorf("start session: %w", err)
	}
	defer session.EndSession(context.Background())

	txOpts := options.Transaction().
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority()))

	err = session.StartTransaction(txOpts)
	if err != nil {
		return fmt.Errorf("start transaction: %w", err)
	}

	e := fn(ctx)
	if e != nil {
		session.AbortTransaction(ctx)
		return e
	}

	return session.CommitTransaction(ctx)
}

type Transaction struct {
	session      mongo.Session
	fencingToken int64
}

func (t *Transaction) Commit(ctx context.Context) error {
	return t.session.CommitTransaction(ctx)
}

func (t *Transaction) Rollback(ctx context.Context) error {
	return t.session.AbortTransaction(ctx)
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

// Session returns the underlying MongoDB session
func (t *Transaction) Session() mongo.Session {
	return t.session
}

// Querier interface for repositories
type Querier interface {
	Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection
	Session() mongo.Session
}

type dbQuerier struct {
	db *mongo.Database
}

func (q *dbQuerier) Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection {
	return q.db.Collection(name, opts...)
}

func (q *dbQuerier) Session() mongo.Session {
	return nil
}

type txQuerier struct {
	session mongo.Session
}

func (q *txQuerier) Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection {
	return q.session.Client().Database("flowrule").Collection(name, opts...)
}

func (q *txQuerier) Session() mongo.Session {
	return q.session
}

func NewQuerier(db *mongo.Database) Querier {
	return &dbQuerier{db: db}
}

func NewTxQuerier(session mongo.Session) Querier {
	return &txQuerier{session: session}
}

type TxWrapper struct {
	*Transaction
}

func (t *TxWrapper) Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection {
	return t.session.Client().Database("flowrule").Collection(name, opts...)
}

func (t *TxWrapper) Session() mongo.Session {
	return t.session
}

func WrapTx(tx ports.Transaction) Querier {
	if mw, ok := tx.(*Transaction); ok {
		return &TxWrapper{mw}
	}
	return nil
}
