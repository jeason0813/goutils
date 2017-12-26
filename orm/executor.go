package orm

import (
	"database/sql"
	"sync"
	"time"
)

type Executor struct {
	query  *sync.Pool
	conn   *sql.DB
	driver string

	TZLocation *time.Location // The timezone of the application
	DatabaseTZ *time.Location // The timezone of the database
}

var defaultExecutor = NewExecutor()

func DefaultExecutor() *Executor {
	return defaultExecutor
}

func NewExecutor() *Executor {
	executor := &Executor{
		query:      &sync.Pool{},
		TZLocation: time.Local,
	}
	executor.query.New = func() interface{} {
		return newQuery(executor)
	}
	return executor
}

func (e *Executor) Conn(conn *sql.DB) {
	e.conn = conn
	e.DatabaseTZ = time.Local
}

func (e *Executor) ConnWithDriver(conn *sql.DB, driver string) {
	e.conn = conn
	e.driver = driver
	if driver == SQLITE {
		e.DatabaseTZ = time.UTC
	} else {
		e.DatabaseTZ = time.Local
	}

}

func (e *Executor) Query() Query {
	return e.getQuery()
}

func (e *Executor) getQuery() *query {
	query := e.query.Get().(*query)
	query.Session(e.conn)
	return query
}

func (e *Executor) Session(f func(conn Conn) error) error {
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

func (e *Executor) Begin() (Transaction, error) {
	return e.conn.Begin()
}
