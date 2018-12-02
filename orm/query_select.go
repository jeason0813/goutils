package orm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

type SelectQuery interface {
	SelectString() ([]map[string]string, error)
	OneString() (map[string]string, error)

	SelectInterface() ([]map[string]interface{}, error)
	OneInterface() (map[string]interface{}, error)

	Find(result interface{}) error
	Rows() (Rows, error)

	Count() (int64, error)
}

func (q *query) SelectString() ([]map[string]string, error) {
	defer q.release()
	rows, err := q.selectInline()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return rows2Strings(rows)
}

func (q *query) OneString() (map[string]string, error) {
	defer q.release()
	q.Limit(0, 1)
	rows, err := q.selectInline()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	a, err := rows2Strings(rows)
	if err != nil {
		return nil, err
	}
	return a[0], nil
}

func (q *query) SelectInterface() ([]map[string]interface{}, error) {
	defer q.release()
	rows, err := q.selectInline()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return rows2Interfaces(rows)
}

func (q *query) OneInterface() (map[string]interface{}, error) {
	defer q.release()
	q.Limit(0, 1)
	rows, err := q.selectInline()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	a, err := rows2Interfaces(rows)
	if err != nil {
		return nil, err
	}
	return a[0], nil
}

func (q *query) Find(result interface{}) error {
	defer q.release()
	beanValue := reflect.ValueOf(result)
	if beanValue.Kind() != reflect.Ptr {
		return errors.New("needs a pointer to a value")
	} else if beanValue.Elem().Kind() == reflect.Ptr {
		return errors.New("a pointer to a pointer is not allowed")
	}
	one := false
	valueE := beanValue.Elem()

	var schema *Schema
	var err error
	if q.schema != nil {
		schema = q.schema
	}

	switch valueE.Kind() {
	case reflect.Struct:
		q.Limit(0, 1)
		one = true
		if schema != nil {
			schema, err = NewSchema(result)
			if err != nil {
				return err
			}
		}

	case reflect.Slice:
		if schema != nil {
			schema, err = NewSchema(valueE.Index(0).Interface())
			if err != nil {
				return err
			}
		}

	default:
		return errors.New("only type:struct or slice")
	}

	rows, err := q.selectInline()
	if err != nil {
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	if one {
		b := rows.Next()
		if !b {
			return errors.New("result is empty")
		}
		return scanInterface(q.executor, rows, schema, columns, &beanValue)
	} else {
		i := 0
		for rows.Next() {
			r := valueE.Index(i)
			err := scanInterface(q.executor, rows, schema, columns, &r)
			if err != nil {
				return err
			}
			i++
		}
		return nil
	}
}

func (q *query) Rows() (Rows, error) {
	defer q.release()
	rows, err := q.selectInline()
	if err != nil {
		return nil, err
	}
	return newRows(q.executor, q.schema, rows)
}

func (q *query) Count() (int64, error) {
	defer q.release()
	q.FieldAlias("count(*)", "n")
	q.Limit(0, 1)
	rows, err := q.selectInline()
	if err != nil {
		return 0, err
	}
	a, err := rows2Strings(rows)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(a[0]["n"], 10, 0)
}

func (q *query) selectInline() (*sql.Rows, error) {
	if q.table == "" {
		return nil, errors.New("table is empty")
	}
	var s = "SELECT %s FROM %s WHERE %s"
	field, err := q.getField()
	where, args, err := q.getWhere()
	if err != nil {
		return nil, err
	}
	return q.query(fmt.Sprintf(s, field, q.table, where), args...)
}

func value2String(rawValue *reflect.Value) (str string, err error) {
	aa := reflect.TypeOf((*rawValue).Interface())
	vv := reflect.ValueOf((*rawValue).Interface())
	switch aa.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		str = strconv.FormatInt(vv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		str = strconv.FormatUint(vv.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		str = strconv.FormatFloat(vv.Float(), 'f', -1, 64)
	case reflect.String:
		str = vv.String()
	case reflect.Array, reflect.Slice:
		switch aa.Elem().Kind() {
		case reflect.Uint8:
			data := rawValue.Interface().([]byte)
			str = string(data)
			if str == "\x00" {
				str = "0"
			}
		default:
			err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
		}
		// time type
	case reflect.Struct:
		if aa.ConvertibleTo(TimeType) {
			str = vv.Convert(TimeType).Interface().(time.Time).Format(time.RFC3339Nano)
		} else {
			err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
		}
	case reflect.Bool:
		str = strconv.FormatBool(vv.Bool())
	case reflect.Complex128, reflect.Complex64:
		str = fmt.Sprintf("%v", vv.Complex())
		/* TODO: unsupported types below
		   case reflect.Map:
		   case reflect.Ptr:
		   case reflect.Uintptr:
		   case reflect.UnsafePointer:
		   case reflect.Chan, reflect.Func, reflect.Interface:
		*/
	default:
		err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
	}
	return
}

func row2mapStr(rows *sql.Rows, fields []string) (resultsMap map[string]string, err error) {
	result := make(map[string]string)
	scanResultContainers := make([]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		var scanResultContainer interface{}
		scanResultContainers[i] = &scanResultContainer
	}
	if err := rows.Scan(scanResultContainers...); err != nil {
		return nil, err
	}

	for ii, key := range fields {
		rawValue := reflect.Indirect(reflect.ValueOf(scanResultContainers[ii]))
		// if row is null then as empty string
		if rawValue.Interface() == nil {
			result[key] = ""
			continue
		}

		if data, err := value2String(&rawValue); err == nil {
			result[key] = data
		} else {
			return nil, err
		}
	}
	return result, nil
}

func rows2Strings(rows *sql.Rows) (resultsSlice []map[string]string, err error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		result, err := row2mapStr(rows, fields)
		if err != nil {
			return nil, err
		}
		resultsSlice = append(resultsSlice, result)
	}

	return resultsSlice, nil
}

func row2mapInterface(rows *sql.Rows, fields []string) (resultsMap map[string]interface{}, err error) {
	resultsMap = make(map[string]interface{}, len(fields))
	scanResultContainers := make([]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		var scanResultContainer interface{}
		scanResultContainers[i] = &scanResultContainer
	}
	if err := rows.Scan(scanResultContainers...); err != nil {
		return nil, err
	}

	for ii, key := range fields {
		resultsMap[key] = reflect.Indirect(reflect.ValueOf(scanResultContainers[ii])).Interface()
	}
	return
}

func rows2Interfaces(rows *sql.Rows) (resultsSlice []map[string]interface{}, err error) {
	fields, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		result, err := row2mapInterface(rows, fields)
		if err != nil {
			return nil, err
		}
		resultsSlice = append(resultsSlice, result)
	}

	return resultsSlice, nil
}
