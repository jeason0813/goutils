package orm

import (
	"bytes"
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type CondQuery interface {
	Table(table string) Query
	TableAlias(table string, alias string) Query
	Field(field string) Query
	FieldAlias(field string, alias string) Query
	Where(field string, operate string, value interface{}) Query
	Limit(start int, offset int) Query
	Page(page int, size int) Query
	Order(field string) Query
	Asc(field string) Query
	Desc(field string) Query
	Sql(query string, args interface{}) Query
}

func (q *query) Table(table string) Query {
	q.table = "`" + table + "`"
	return q
}

func (q *query) TableAlias(table string, alias string) Query {
	q.table = "`" + table + "`"
	if alias != "" {
		q.table += " as " + alias
	}
	return q
}

func (q *query) Field(field string) Query {
	return q.FieldAlias("`"+field+"`", "")
}

func (q *query) FieldAlias(field string, alias string) Query {
	if alias != "" {
		field += " as " + alias
	}
	if q.fields == nil {
		q.fields = []string{field}
	} else {
		q.fields = append(q.fields, field)
	}
	return q
}

func (q *query) Where(field string, operate string, value interface{}) Query {
	sql := "`" + field + "`" + operate + " ? "
	if q.where == nil {
		q.where = []string{sql}
	} else {
		q.where = append(q.where, sql)
	}
	if q.whereArgs == nil {
		q.whereArgs = []interface{}{value}
	} else {
		q.whereArgs = append(q.whereArgs, value)
	}
	return q
}

func (q *query) Limit(start int, offset int) Query {
	q.limit = []string{strconv.Itoa(start), strconv.Itoa(offset)}
	return q
}

func (q *query) Page(page int, size int) Query {
	start := 0
	if page >= 1 {
		start = (page - 1) * size
	}
	q.Limit(start, size)
	return q
}

func (q *query) Order(field string) Query {
	field = "`" + field + "`"
	if q.order == nil {
		q.order = []string{field}
	} else {
		q.order = append(q.order, field)
	}
	return q
}

func (q *query) Asc(field string) Query {
	field = "`" + field + "`" + " asc"
	if q.order == nil {
		q.order = []string{field}
	} else {
		q.order = append(q.order, field)
	}
	return q
}

func (q *query) Desc(field string) Query {
	field = "`" + field + "`" + " desc"
	if q.order == nil {
		q.order = []string{field}
	} else {
		q.order = append(q.order, field)
	}
	return q
}

func (q *query) Sql(query string, args interface{}) Query {
	value := reflect.ValueOf(args)
	switch value.Kind() {
	case reflect.Slice:
		return q.sql(query, args.([]interface{})...)
	case reflect.Map:
		query, args, err := MapToSlice(query, args)
		if err != nil {
			return q.error(err)
		}
		return q.sql(query, args)
	case reflect.Struct:
		query, args, err := StructToSlice(query, args)
		if err != nil {
			return q.error(err)
		}
		return q.sql(query, args)
	default:
		return q.error(errors.New("args can be slice, map, struct"))
	}
}

func (q *query) getWhere() (string, []interface{}, error) {
	var args []interface{}

	buf := bytes.Buffer{}
	if q.where != nil {
		buf.WriteString(strings.Join(q.where, " AND "))
		args = q.whereArgs
	} else {
		buf.WriteString("1")
	}
	if q.order != nil {
		buf.WriteString(" ORDER BY ")
		buf.WriteString(strings.Join(q.order, ","))
	}
	if q.limit != nil {
		buf.WriteString(" LIMIT ")
		buf.WriteString(strings.Join(q.limit, ","))
	}
	return buf.String(), args, nil
}

func (q *query) getField() (string, error) {
	if q.fields != nil {
		return strings.Join(q.fields, ","), nil
	} else {
		return "*", nil
	}
}

func (q *query) sql(query string, args ...interface{}) *query {
	q.sqlQuery = query
	q.args = args
	return q
}
