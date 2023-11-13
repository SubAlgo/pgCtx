package pgctx

import (
	"context"
	"fmt"

	"github.com/SubAlgo/pgctx/query"
	"github.com/exaring/otelpgx"
	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	dbKey    struct{}
	queryKey struct{}
)

type DB struct {
	dbCtx context.Context
}

type Config struct {
	ConnString string // example: "host=localhost port=5432 database=postgres user=postgres password=secret"
	UseTracer  bool
}

// New pgctx.DB
func New(ctx context.Context, config Config) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(config.ConnString)
	if err != nil {
		return nil, err
	}

	if config.UseTracer {
		cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}

	var db DB
	db.dbCtx = context.WithValue(ctx, queryKey, pool)
	db.dbCtx = context.WithValue(db.dbCtx, dbKey, pool)

	return &db, nil
}

/*
opts:
- dbscan.WithStructTagKey(tagKey string)
- dbscan.WithAllowUnknownColumns(allowUnknownColumns bool)
- dbscan.WithColumnSeparator(separator string)
- dbscan.WithFieldNameMapper(mapperFn func(string) string)
*/
func NewWithScanApi(ctx context.Context, config Config, opts ...dbscan.APIOption) (*DB, *pgxscan.API, error) {
	db, err := New(ctx, config)
	if err != nil {
		return nil, nil, err
	}
	dbscanAPI, err := pgxscan.NewDBScanAPI(opts...)
	if err != nil {
		return nil, nil, err
	}
	scanAPI, err := pgxscan.NewAPI(dbscanAPI)
	if err != nil {
		return nil, nil, err
	}
	return db, scanAPI, nil
}

func (d DB) getQuery(ctx context.Context) query.Query {
	if tx, ok := ctx.Value(dbKey).(*pgxpool.Tx); ok {
		return tx
	}

	return d.dbCtx.Value(queryKey).(*pgxpool.Pool)
}

func (d DB) getDB(ctx context.Context) *pgxpool.Pool {
	return d.dbCtx.Value(queryKey).(*pgxpool.Pool)
}

func (d DB) Exec(ctx context.Context, queryString string, args ...interface{}) (pgconn.CommandTag, error) {
	return d.getQuery(ctx).Exec(ctx, queryString, args...)
}

func (d DB) Query(ctx context.Context, queryString string, args ...interface{}) (pgx.Rows, error) {
	return d.getQuery(ctx).Query(ctx, queryString, args...)
}

func (d DB) QueryRow(ctx context.Context, queryString string, args ...interface{}) pgx.Row {
	return d.getQuery(ctx).QueryRow(ctx, queryString, args...)
}

func (d DB) Transactional(ctx context.Context, f func(context.Context) error) error {
	db := d.getDB(ctx)

	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("%s: %w", "unable to begin transaction", err)
	}

	d.dbCtx = context.WithValue(d.dbCtx, queryKey, tx)
	defer tx.Rollback(d.dbCtx)
	if err := f(d.dbCtx); err != nil {
		return err
	}
	return tx.Commit(d.dbCtx)
}
