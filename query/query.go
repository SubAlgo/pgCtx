package query

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Query interface {
	Exec(ctx context.Context, queryString string, args ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, queryString string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, queryString string, args ...interface{}) pgx.Row
}
