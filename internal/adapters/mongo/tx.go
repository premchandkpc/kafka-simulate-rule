package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Tx struct {
	session    mongo.Session
	fencingToken int64
}

func NewTx(ctx context.Context, client *mongo.Client) (*Tx, error) {
	session, err := client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("start session: %w", err)
	}

	err = session.StartTransaction(options.Transaction().SetReadConcern(options.ReadConcern().SetLevel("majority")).SetWriteConcern(options.WriteConcern().SetW("majority")))
	if err != nil {
		session.EndSession(context.Background())
		return nil, fmt.Errorf("start transaction: %w", err)
	}

	return &Tx{session: session}, nil
}

func (t *Tx) Session() mongo.Session {
	return t.session
}

func (t *Tx) Commit(ctx context.Context) error {
	return t.session.CommitTransaction(ctx)
}

func (t *Tx) Rollback(ctx context.Context) error {
	return t.session.AbortTransaction(ctx)
}

func (t *Tx) FencingToken() int64 {
	return t.fencingToken
}

func (t *Tx) SetFencingToken(token int64) {
	t.fencingToken = token
}

func (t *Tx) EndSession(ctx context.Context) {
	t.session.EndSession(ctx)
}

// Querier interface for repositories
type Querier interface {
	Collection(name string) *mongo.Collection
	Session() mongo.Session
}

type dbQuerier struct {
	db *mongo.Database
}

func (q *dbQuerier) Collection(name string) *mongo.Collection {
	return q.db.Collection(name)
}

func (q *dbQuerier) Session() mongo.Session {
	return nil
}

type txQuerier struct {
	session mongo.Session
}

func (q *txQuerier) Collection(name string) *mongo.Collection {
	return q.session.Client().Database("flowrule").Collection(name)
}

func (q *txQuerier) Session() mongo.Session {
	return q.session
}

func NewQuerier(db *mongo.Database) Querier {
	return &dbQuerier{db: db}
}

func NewTxQuerier(tx *Tx) Querier {
	return &txQuerier{session: tx.session}
}