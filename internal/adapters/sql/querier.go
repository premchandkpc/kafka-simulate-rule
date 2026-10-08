package sql

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Querier abstracts the common query interface shared by *pgxpool.Pool and *pgx.Tx.
type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Tx wraps pgx.Tx and implements ports.Tx for the application layer.
type Tx struct {
	tx            pgx.Tx
	fencingToken  int64
}

func NewTx(tx pgx.Tx) *Tx {
	return &Tx{tx: tx}
}

func (t *Tx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return t.tx.Exec(ctx, sql, arguments...)
}

func (t *Tx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.tx.Query(ctx, sql, args...)
}

func (t *Tx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.tx.QueryRow(ctx, sql, args...)
}

func (t *Tx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *Tx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

func (t *Tx) FencingToken() int64 {
	return t.fencingToken
}

func (t *Tx) SetFencingToken(token int64) {
	t.fencingToken = token
}

func (t *Tx) Context() context.Context {
	return context.Background()
}

func (t *Tx) Querier() Querier {
	return t
}

func NewTxFromPool(ctx context.Context, pool *pgxpool.Pool) (*Tx, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return NewTx(tx), nil
}
