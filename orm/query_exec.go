package orm

import (
	"bytes"
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

type ExecQuery interface {
	Insert(data ...map[string]interface{}) (int64, error)
	Update(data map[string]interface{}) (int64, error)
	//InsertOrUpdate(update interface{}, data ...interface{}) (int64, error)
	Delete() (int64, error)

	Id(field string) Query
}

func (q *query) Insert(data ...map[string]interface{}) (int64, error) {
	defer q.release()
	var fields []string
	var d [][]interface{}
	var err error
	if q.model != nil {
		fields, d, err = q.modelInsert(data...)
		if err != nil {
			return 0, err
		}
	} else {
		fields, d = parseMap(data...)
	}
	if len(d) == 1 {
		result, err := q.inlineInsert(fields, d[0])
		last, err := result.LastInsertId()
		if err != nil {
			return 0, err
		}
		if q.id != "" {
			data[0][q.id] = last
			return last, nil
		} else {
			return result.RowsAffected()
		}
	} else {
		result, err := q.inlineInsertBatch(fields, d...)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected()
	}
}

func (q *query) Update(data map[string]interface{}) (int64, error) {
	defer q.release()
	var fields []string
	var d [][]interface{}
	var err error
	if q.model != nil {
		fields, d, err = q.modelUpdate(data)
		if err != nil {
			return 0, err
		}
	} else {
		fields, d = parseMap(data)
	}
	result, err := q.inlineUpdate(fields, d[0])
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

//func (q *query) InsertOrUpdate(update interface{}, data ...interface{}) (int64, error) {
//	defer q.release()
//	var sql = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE SET %s"
//}

func (q *query) Delete() (int64, error) {
	defer q.release()
	var s = "DELETE FROM %s WHERE %s"
	if q.table == "" {
		return 0, errors.New("table is empty")
	}
	where, args, err := q.getWhere()
	if err != nil {
		return 0, err
	}
	result, err := q.exec(fmt.Sprint(s, q.table, where), args)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (q *query) Id(field string) Query {
	q.id = field
	return q
}

func (q *query) inlineInsert(fields []string, data []interface{}) (sql.Result, error) {
	if q.table == "" {
		return nil, errors.New("table is empty")
	}
	var s = "INSERT INTO %s %s VALUES %s"
	fieldStr, valueStr, err := q.getInsertField(fields, data)
	if err != nil {
		return nil, err
	}

	return q.exec(fmt.Sprint(s, q.table, fieldStr, valueStr), data...)
}

func (q *query) inlineInsertBatch(fields []string, data ...[]interface{}) (sql.Result, error) {
	if q.table == "" {
		return nil, errors.New("table is empty")
	}
	var s = "INSERT INTO %s (%s) VALUES %s"
	fieldStr, valueStrTemplate, err := q.getInsertField(fields, data[0])
	if err != nil {
		return nil, err
	}
	l := len(data)
	args := make([]interface{}, len(data)*len(data[0]))
	valueStr := ""
	for i := 0; i < l; i++ {
		valueStr += valueStrTemplate
		args = append(args, data[i])
		if i+1 < l {
			valueStr += ","
		}
	}
	return q.exec(fmt.Sprint(s, q.table, fieldStr, valueStr), args...)
}

func (q *query) inlineUpdate(fields []string, data []interface{}) (sql.Result, error) {
	if q.table == "" {
		return nil, errors.New("table is empty")
	}
	var s = "UPDATE %s SET %s WHERE %s"

	setSql, err := q.getSet(fields, data)
	if err != nil {
		return nil, err
	}
	where, args, err := q.getWhere()
	if err != nil {
		return nil, err
	}
	return q.exec(fmt.Sprint(s, q.table, setSql, where), append(data, args...)...)
}

func (q *query) getSet(fields []string, data []interface{}) (string, error) {
	if len(fields) != len(data) {
		return "", errors.New("field num is error")
	}
	var buf bytes.Buffer
	l := len(fields)
	for k, v := range fields {
		buf.WriteString("`" + v + "` = ?")
		if k+1 < l {
			buf.WriteString(",")
		}
	}
	return buf.String(), nil
}

func (q *query) getInsertField(fields []string, data []interface{}) (string, string, error) {
	if len(fields) != len(data) {
		return "", "", errors.New("field num is error")
	}
	var field bytes.Buffer
	var value bytes.Buffer
	l := len(fields)
	field.WriteString("(")
	value.WriteString("(")
	for k, v := range fields {
		field.WriteString("`" + v + "`")
		value.WriteString("?")
		if k+1 < l {
			field.WriteString(",")
			value.WriteString(",")
		}
	}
	field.WriteString(")")
	value.WriteString(")")
	return field.String(), value.String(), nil
}

func parseMap(data ...map[string]interface{}) ([]string, [][]interface{}) {
	if len(data) == 0 {
		return []string{}, [][]interface{}{}
	}
	template := data[0]
	fields := make([]string, len(template))
	for k, _ := range template {
		fields = append(fields, k)
	}
	d := make([][]interface{}, len(data))
	for _, v := range data {
		g := make([]interface{}, len(fields))
		for _, vv := range v {
			g = append(g, vv)
		}
		d = append(d, g)
	}
	return fields, d
}
