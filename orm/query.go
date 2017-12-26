package orm

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type Conn interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type Transaction interface {
	Conn
	Rollback() error
	Commit() error
}

type Query interface {
	Session(conn Conn) Query

	CondQuery
	ExecQuery
	SelectQuery
}

type query struct {
	executor *Executor
	conn     Conn

	model  *Model
	id     string
	schema *Schema
	i      interface{}

	table     string
	fields    []string
	order     []string
	limit     []string
	where     []string
	whereArgs []interface{}

	sqlQuery string
	args     []interface{}

	err error
}

var (
	re, _ = regexp.Compile(`[?](\w+)`)
)

func newQuery(executor *Executor) *query {
	q := &query{
		executor: executor,
	}
	q.init()
	return q
}

func (q *query) init() {
	q.order = nil
	q.limit = nil
	q.fields = nil
	q.where = nil
	q.whereArgs = nil

	q.id = ""
	q.model = nil
	q.schema = nil

	q.sqlQuery = ""
	q.args = []interface{}{}

	q.err = nil
}

func (q *query) error(err error) *query {
	q.err = err
	return q
}

func (q *query) exec(query string, args ...interface{}) (sql.Result, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.conn.Exec(query, args)
}

func (q *query) query(query string, args ...interface{}) (*sql.Rows, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.conn.Query(query, args)
}

func (q *query) release() {
	q.init()
	q.executor.query.Put(q)
}

func (q *query) Session(conn Conn) Query {
	q.conn = conn
	return q
}

func MapToSlice(query string, mp interface{}) (string, []interface{}, error) {
	vv := reflect.ValueOf(mp)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Map {
		return "", []interface{}{}, errors.New("need a map pointer")
	}

	args := make([]interface{}, 0, len(vv.Elem().MapKeys()))
	var err error
	query = re.ReplaceAllStringFunc(query, func(src string) string {
		v := vv.Elem().MapIndex(reflect.ValueOf(src[1:]))
		if !v.IsValid() {
			err = fmt.Errorf("map key %s is missing", src[1:])
		} else {
			args = append(args, v.Interface())
		}
		return "?"
	})

	return query, args, err
}

func StructToSlice(query string, st interface{}) (string, []interface{}, error) {
	vv := reflect.ValueOf(st)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Struct {
		return "", []interface{}{}, errors.New("need a struct pointer")
	}

	args := make([]interface{}, 0)
	var err error
	query = re.ReplaceAllStringFunc(query, func(src string) string {
		fv := vv.Elem().FieldByName(src[1:]).Interface()
		if v, ok := fv.(driver.Valuer); ok {
			var value driver.Value
			value, err = v.Value()
			if err != nil {
				return "?"
			}
			args = append(args, value)
		} else {
			args = append(args, fv)
		}
		return "?"
	})
	if err != nil {
		return "", []interface{}{}, err
	}
	return query, args, nil
}

func formatTime(sqlTypeName string, t time.Time) (v interface{}) {
	switch sqlTypeName {
	case "time":
		s := t.Format("2006-01-02 15:04:05") //time.RFC3339
		v = s[11:19]
	case "date":
		v = t.Format("2006-01-02")
	case "dateTime", "timestamp":
		v = t.Format("2006-01-02 15:04:05")
	case "timestampz":
		v = t.Format(time.RFC3339Nano)
	case "int":
		v = t.Unix()
	default:
		v = t
	}
	return
}

func str2Time(executor *Executor, col *column, data string) (outTime time.Time, outErr error) {
	sdata := strings.TrimSpace(data)
	var x time.Time
	var err error

	var parseLoc = executor.DatabaseTZ
	if col.zone != nil {
		parseLoc = col.zone
	}

	if sdata == zeroTime0 || sdata == zeroTime1 {
	} else if !strings.ContainsAny(sdata, "- :") { // !nashtsai! has only found that mymysql driver is using this for time type column
		// time stamp
		sd, err := strconv.ParseInt(sdata, 10, 64)
		if err == nil {
			x = time.Unix(sd, 0)
			//session.engine.logger.Debugf("time(0) key[%v]: %+v | sdata: [%v]\n", col.FieldName, x, sdata)
		} else {
			//session.engine.logger.Debugf("time(0) err key[%v]: %+v | sdata: [%v]\n", col.FieldName, x, sdata)
		}
	} else if len(sdata) > 19 && strings.Contains(sdata, "-") {
		x, err = time.ParseInLocation(time.RFC3339Nano, sdata, parseLoc)
		if err != nil {
			x, err = time.ParseInLocation("2006-01-02 15:04:05.999999999", sdata, parseLoc)
			//session.engine.logger.Debugf("time(2) key[%v]: %+v | sdata: [%v]\n", col.FieldName, x, sdata)
		}
		if err != nil {
			x, err = time.ParseInLocation("2006-01-02 15:04:05.9999999 Z07:00", sdata, parseLoc)
			//session.engine.logger.Debugf("time(3) key[%v]: %+v | sdata: [%v]\n", col.FieldName, x, sdata)
		}
	} else if len(sdata) == 19 && strings.Contains(sdata, "-") {
		x, err = time.ParseInLocation("2006-01-02 15:04:05", sdata, parseLoc)
		//session.engine.logger.Debugf("time(4) key[%v]: %+v | sdata: [%v]\n", col.FieldName, x, sdata)
	} else if len(sdata) == 10 && sdata[4] == '-' && sdata[7] == '-' {
		x, err = time.ParseInLocation("2006-01-02", sdata, parseLoc)
		//session.engine.logger.Debugf("time(5) key[%v]: %+v | sdata: [%v]\n", col.FieldName, x, sdata)
	} else if col.sql == Time {
		if strings.Contains(sdata, " ") {
			ssd := strings.Split(sdata, " ")
			sdata = ssd[1]
		}

		sdata = strings.TrimSpace(sdata)
		if executor.driver == MYSQL && len(sdata) > 8 {
			sdata = sdata[len(sdata)-8:]
		}

		st := fmt.Sprintf("2006-01-02 %v", sdata)
		x, err = time.ParseInLocation("2006-01-02 15:04:05", st, parseLoc)
		//session.engine.logger.Debugf("time(6) key[%v]: %+v | sdata: [%v]\n", col.FieldName, x, sdata)
	} else {
		outErr = fmt.Errorf("unsupported time format %v", sdata)
		return
	}
	if err != nil {
		outErr = fmt.Errorf("unsupported time format %v: %v", sdata, err)
		return
	}
	outTime = x.In(executor.TZLocation)
	return
}
func byte2Time(executor *Executor, col *column, data []byte) (outTime time.Time, outErr error) {
	return str2Time(executor, col, string(data))
}
