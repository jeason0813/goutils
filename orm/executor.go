package orm

import (
	"database/sql"
	"sync"
	"time"
)

type Executor interface {
	Conn(conn *sql.DB)
	ConnWithDriver(conn *sql.DB, driver string)
	Query() Query
	Session(f func(conn Conn) error) error
	Begin() (Transaction, error)
}

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Error(args ...interface{})
}

type executor struct {
	query  *sync.Pool
	conn   *sql.DB
	driver string
	logger Logger

	TZLocation *time.Location // The timezone of the application
	DatabaseTZ *time.Location // The timezone of the database
}

var defaultExecutor = NewExecutor().(*executor)

func DefaultExecutor() Executor {
	return defaultExecutor
}

func NewExecutor() Executor {
	e := &executor{
		query:      &sync.Pool{},
		TZLocation: time.Local,
	}
	e.query.New = func() interface{} {
		return newQuery(e)
	}
	return e
}

func (e *executor) Conn(conn *sql.DB) {
	e.conn = conn
	e.DatabaseTZ = time.Local
}

func (e *executor) ConnWithDriver(conn *sql.DB, driver string) {
	e.conn = conn
	e.driver = driver
	if driver == SQLITE {
		e.DatabaseTZ = time.UTC
	} else {
		e.DatabaseTZ = time.Local
	}

}

func (e *executor) SetLogger(logger Logger) {
	e.logger = logger
}

func (e *executor) Query() Query {
	return e.getQuery()
}

func (e *executor) getQuery() *query {
	query := e.query.Get().(*query)
	query.Session(e.conn)
	return query
}

func (e *executor) Session(f func(conn Conn) error) error {
	tx, err := e.conn.Begin()
	defer func() {
		if err := recover(); err != nil {
			tx.Rollback()
		}
	}()
	if err != nil {
		return err
	}
	err = f(tx)
	if err != nil {
		tx.Rollback()
		return err
	} else {
		tx.Commit()
		return nil
	}
}

func (e *executor) Begin() (Transaction, error) {
	return e.conn.Begin()
}
