package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
)

type Type string

const (
	TypeSqlite   Type = "sqlite"
	TypePostgres Type = "postgres"

	defaultPostgresTimeoutSecond = "30"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
	ErrInvalid  = errors.New("invalid")
	ErrReadonly = errors.New("readonly")
)

type DB struct {
	typ Type
	db  *sql.DB

	primaryLockConn *sql.Conn
}

func NewSqlite(dataDir string) (*DB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=rwc", path.Join(dataDir, "db.sqlite")))
	if err != nil {
		return nil, err
	}
	if _, err = db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return &DB{typ: TypeSqlite, db: db}, nil
}

func NewPostgres(dsn string, useIAM bool) (*DB, error) {
	var err error
	if dsn, err = addPostgresConnectTimeout(dsn); err != nil {
		return nil, err
	}
	if !useIAM {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return nil, err
		}
		return &DB{typ: TypePostgres, db: db}, nil
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}
	db := stdlib.OpenDB(*cfg, stdlib.OptionBeforeConnect(func(ctx context.Context, c *pgx.ConnConfig) error {
		region := extractRegionFromHost(c.Host)
		token, err := buildIAMAuthToken(ctx, c.Host, fmt.Sprintf("%d", c.Port), c.User, region)
		if err != nil {
			return err
		}
		c.Password = token
		return nil
	}))
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return &DB{typ: TypePostgres, db: db}, nil
}

func addPostgresConnectTimeout(dsn string) (string, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", err
		}
		q := u.Query()
		if q.Get("connect_timeout") == "" {
			q.Set("connect_timeout", defaultPostgresTimeoutSecond)
		}
		u.RawQuery = q.Encode()
		return u.String(), nil
	}
	if !strings.Contains(dsn, "connect_timeout=") {
		dsn += " connect_timeout=" + defaultPostgresTimeoutSecond
	}
	return dsn, nil
}

func (db *DB) Type() Type {
	return db.typ
}

func (db *DB) DB() *sql.DB {
	return db.db
}

func (db *DB) Exec(query string, args ...any) (sql.Result, error) {
	return db.db.Exec(query, args...)
}

func (db *DB) Query(query string, args ...any) (*sql.Rows, error) {
	return db.db.Query(query, args...)
}

func (db *DB) QueryRow(query string, args ...any) *sql.Row {
	return db.db.QueryRow(query, args...)
}

func (db *DB) Migrator() *Migrator {
	return NewMigrator(db.typ, db)
}

func (db *DB) Migrate(extraTables ...Table) error {
	defaultTables := []Table{
		&Project{},
		&CheckConfigs{},
		&Incident{},
		&IncidentNotification{},
		&AlertNotification{},
		&ApplicationDeployment{},
		&ApplicationSettings{},
		&Dashboards{},
		&Setting{},
		&User{},
		&AlertingRule{},
		&Alert{},
	}
	return db.Migrator().Migrate(append(defaultTables, extraTables...)...)
}

func (db *DB) IsUniqueViolationError(err error) bool {
	switch db.typ {
	case TypePostgres:
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return pgErr.Code == "23505"
		}
		return false
	case TypeSqlite:
		e, ok := err.(sqlite3.Error)
		return ok && e.Code == sqlite3.ErrConstraint
	}
	return false
}

func (db *DB) GetDeploymentUuid() (string, error) {
	settingKey := "deployment_uuid"
	var id string
	err := db.GetSetting(settingKey, &id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return "", err
	}
	uid, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	id = uid.String()
	err = db.SetSetting(settingKey, id)
	if err != nil {
		return "", err
	}
	return id, nil
}

type Table interface {
	Migrate(m *Migrator) error
}

type Migrator struct {
	typ Type
	db  *DB
}

func NewMigrator(t Type, db *DB) *Migrator {
	return &Migrator{typ: t, db: db}
}

func (m *Migrator) Migrate(tables ...Table) error {
	for _, t := range tables {
		err := t.Migrate(m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Migrator) Exec(query string, args ...any) error {
	switch m.typ {
	case TypePostgres:
		query = strings.ReplaceAll(query, "INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
	}
	_, err := m.db.Exec(query, args...)
	return err
}

func (m *Migrator) AddColumnIfNotExists(table, column, dataType string) error {
	switch m.typ {
	case TypeSqlite:
		rows, err := m.db.Query(fmt.Sprintf("SELECT name FROM pragma_table_info('%s');", table))
		if err != nil {
			return nil
		}
		defer func() {
			_ = rows.Close()
		}()
		var name string
		for rows.Next() {
			if err := rows.Scan(&name); err != nil {
				return err
			}
			if name == column {
				return nil
			}
		}
		_, err = m.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, dataType))
		if err != nil {
			return err
		}
	case TypePostgres:
		_, err := m.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", table, column, dataType))
		if err != nil {
			return err
		}
	}
	return nil
}

func marshal[T any](v *T) (*string, error) {
	if v == nil {
		return nil, nil
	}
	d, err := json.Marshal(v)
	s := string(d)
	return &s, err
}

func unmarshal[T any](s string, v **T) error {
	if s == "" {
		return nil
	}
	var vv T
	*v = &vv
	return json.Unmarshal([]byte(s), &vv)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func ValidatePostgresIAM(dsn string) error {
	// use pgx.ParseConfig to extract host
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("invalid DSN: %w", err)
	}
	if cfg.Host == "" {
		return fmt.Errorf("IAM auth: host is required")
	}
	if extractRegionFromHost(cfg.Host) == "" && os.Getenv("AWS_REGION") == "" && os.Getenv("AWS_DEFAULT_REGION") == "" {
		return fmt.Errorf("IAM auth: cannot determine AWS region from host %q and AWS_REGION not set; use standard RDS endpoint like db.x.region.rds.amazonaws.com or set AWS_REGION", cfg.Host)
	}
	return nil
}

func extractRegionFromHost(host string) string {
	parts := strings.Split(host, ".")
	for i, p := range parts {
		if p == "rds" && i > 0 {
			return parts[i-1]
		}
	}
	return ""
}
