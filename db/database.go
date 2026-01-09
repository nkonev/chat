package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"embed"
	"fmt"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/logger"
	"net/http"
	"strings"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	pgxStd "github.com/jackc/pgx/v4/stdlib"
	proxy "github.com/shogo82148/go-sql-proxy"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.uber.org/fx"
)

func makeLoggingDriver(cfg *config.AppConfig, lgr *logger.LoggerWrapper) driver.Driver {
	return proxy.NewProxyContext(&pgxStd.Driver{}, &proxy.HooksContext{
		PreExec: func(_ context.Context, _ *proxy.Stmt, _ []driver.NamedValue) (interface{}, error) {
			return time.Now(), nil
		},
		PostExec: func(c context.Context, ctx interface{}, stmt *proxy.Stmt, args []driver.NamedValue, _ driver.Result, err error) error {
			if cfg.PostgreSQL.Dump {
				s := fmt.Sprintf("Exec: %s; args = %v (%s)\n", stmt.QueryString, writeNamedValues(args), time.Since(ctx.(time.Time)))
				if cfg.PostgreSQL.PrettyLog && !cfg.Logger.Json {
					fmt.Printf("[SQL] trace_id=%s: %s\n", logger.GetTraceId(c), s)
				} else {
					lgr.InfoContext(c, s)
				}
			}
			return err
		},

		PreQuery: func(c context.Context, stmt *proxy.Stmt, args []driver.NamedValue) (interface{}, error) {
			return time.Now(), nil
		},
		PostQuery: func(c context.Context, ctx interface{}, stmt *proxy.Stmt, args []driver.NamedValue, rows driver.Rows, err error) error {
			if cfg.PostgreSQL.Dump {
				s := fmt.Sprintf("Query: %s; args = %v (%s)\n", stmt.QueryString, writeNamedValues(args), time.Since(ctx.(time.Time)))
				if cfg.PostgreSQL.PrettyLog && !cfg.Logger.Json {
					fmt.Printf("[SQL] trace_id=%s: %s\n", logger.GetTraceId(c), s)
				} else {
					lgr.InfoContext(c, s)
				}
			}
			return err
		},
	})
}

func writeNamedValues(args []driver.NamedValue) string {
	sb := strings.Builder{}
	sb.WriteString("[")
	for i, arg := range args {
		if i != 0 {
			sb.WriteString(fmt.Sprintf(", "))
		}
		if len(arg.Name) > 0 {
			sb.WriteString(arg.Name)
			sb.WriteString(":")
		}
		sb.WriteString(fmt.Sprintf("%#v", arg.Value))
	}
	sb.WriteString("]")

	return sb.String()
}

type DB struct {
	*sql.DB
	lgr *logger.LoggerWrapper
}

type Tx struct {
	*sql.Tx
	lgr *logger.LoggerWrapper
}

// Transactional and non- operations
type CommonOperations interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func (dbR *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return dbR.DB.QueryContext(ctx, query, args...)
}

func (txR *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return txR.Tx.QueryContext(ctx, query, args...)
}

func (dbR *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return dbR.DB.QueryRowContext(ctx, query, args...)
}

func (txR *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return txR.Tx.QueryRowContext(ctx, query, args...)
}

func (dbR *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return dbR.DB.ExecContext(ctx, query, args...)
}

func (txR *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return txR.Tx.ExecContext(ctx, query, args...)
}

// Begin starts and returns a new transaction.
func (db *DB) Begin(ctx context.Context, lgr *logger.LoggerWrapper) (*Tx, error) {
	if tx, err := db.DB.BeginTx(ctx, nil); err != nil {
		return nil, fmt.Errorf("error during interacting with db: %w", err)
	} else {
		return &Tx{tx, lgr}, nil
	}
}

func (tx *Tx) SafeRollback() {
	if err0 := tx.Rollback(); err0 != nil {
		tx.lgr.Error("Error during rollback tx ", "err", err0)
	}
}

// Performs txFunc transactionally with the result.
// Deliberately doesn't support "nesting" to keep the code simple - to have only one place with TransactWithResult().
func TransactWithResult[T any](ctx context.Context, db *DB, txFunc func(*Tx) (T, error)) (ret T, err error) {
	tx, err := db.Begin(ctx, db.lgr)
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			tx.Rollback() // err is non-nil; don't change it
		} else {
			err = tx.Commit() // err is nil; if Commit returns error update err
		}
	}()
	ret, err = txFunc(tx)
	return ret, err
}

// Performs txFunc transactionally without any result.
// Deliberately doesn't support "nesting" to keep the code simple - to have only one place with Transact().
func Transact(ctx context.Context, db *DB, txFunc func(*Tx) error) (err error) {
	tx, err := db.Begin(ctx, db.lgr)
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			tx.Rollback() // err is non-nil; don't change it
		} else {
			err = tx.Commit() // err is nil; if Commit returns error update err
		}
	}()
	err = txFunc(tx)
	return err
}

func ConfigureDatabase(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	tp *sdktrace.TracerProvider,
	lc fx.Lifecycle,
) (*DB, error) {
	dri := makeLoggingDriver(cfg, lgr)

	otDriver := otelsql.WrapDriver(dri, otelsql.WithAttributes(
		semconv.DBSystemPostgreSQL,
	))

	connector, err := otDriver.(driver.DriverContext).OpenConnector(cfg.PostgreSQL.Url)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)

	err = otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(
		semconv.DBSystemPostgreSQL,
	), otelsql.WithTracerProvider(tp))
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(cfg.PostgreSQL.MaxLifetime)
	db.SetMaxIdleConns(cfg.PostgreSQL.MaxIdleConnections)
	db.SetMaxOpenConns(cfg.PostgreSQL.MaxOpenConnections)

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	dbWrapper := &DB{
		DB:  db,
		lgr: lgr,
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			lgr.Info("Stopping database")
			if err := db.Close(); err != nil {
				lgr.Error("Error shutting down database", "err", err)
			}
			return nil
		},
	})

	return dbWrapper, nil
}

//go:embed migrations
var embeddedMigrationFiles embed.FS

func (db *DB) Migrate(mc config.MigrationConfig) error {
	db.lgr.Info("Starting migration")
	staticDir := http.FS(embeddedMigrationFiles)
	src, err := httpfs.New(staticDir, "migrations")
	if err != nil {
		return err
	}

	pgInstance, err := postgres.WithInstance(db.DB, &postgres.Config{
		MigrationsTable:  mc.MigrationTable,
		StatementTimeout: mc.StatementDuration,
	})
	if err != nil {
		return err
	}

	m, err := migrate.NewWithInstance("httpfs", src, "", pgInstance)
	if err != nil {
		return err
	}
	//defer m.Close()
	if err := m.Up(); err != nil && err.Error() != "no change" {
		return err
	}
	db.lgr.Info("Migration successfully completed")
	return nil
}

func (db *DB) Reset(mc config.MigrationConfig, hard bool) error {

	additional := ""
	if hard { // whether to preserve techical info (need to fast-worward, need to skip old db migration and so on)
		additional = `
		drop table if exists technical;
		`
	}

	_, err := db.Exec(fmt.Sprintf(`
	drop EXTENSION if exists pg_trgm;
	drop sequence if exists chat_id_sequence;
	
	drop table if exists chat_common;
	drop table if exists chat_participant;
	drop table if exists message_published;
	drop table if exists message_pinned;
	drop table if exists message_reaction;
	drop table if exists message;
	drop table if exists chat_user_view;
	drop table if exists has_unread_messages;
	
	%s

	drop table if exists blog;

	drop FUNCTION if exists strip_tags;
	drop FUNCTION if exists cyrillic_transliterate;

	drop table if exists %s;
`, additional, mc.MigrationTable))
	db.lgr.Info("Recreating database", "err", err)
	return err
}

func RunMigrations(db *DB, cfg *config.AppConfig) error {
	return db.Migrate(cfg.PostgreSQL.Migration)
}

func RunResetDatabaseSoft(db *DB, cfg *config.AppConfig) error {
	return db.Reset(cfg.PostgreSQL.Migration, false)
}

func RunResetDatabaseHard(db *DB, cfg *config.AppConfig) error {
	return db.Reset(cfg.PostgreSQL.Migration, true)
}
