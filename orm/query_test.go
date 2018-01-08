package orm

import (
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

type result struct {
	Id int `json:"id"`
}

func TestDefaultExecutor(t *testing.T) {
	executor := DefaultExecutor()
	executor.Conn(loadMysql())

	_, err := executor.Query().Table("t").Count()
	if err != nil {
		t.Fatal(err)
	}
	a := &result{}
	err = executor.Query().Table("t").Limit(0, 1).Find(a)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(a)
}

func loadMysql() *sql.DB {
	u, err := url.Parse("mysql://game:gamegame@127.0.0.1:3306/game")
	if err != nil {
		return nil
	}
	_, port, err := net.SplitHostPort(u.Host)
	if port == "" {
		port = "3306"
	}
	db, err := sql.Open(u.Scheme, fmt.Sprintf("%s@tcp(%s)%s?charset=utf8", u.User, u.Host, u.Path))
	return db
}
