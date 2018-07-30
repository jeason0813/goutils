package orm

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"time"
)

type ModelQuery interface {
	Query
	Get() (interface{}, error)
	SoftDelete() (int64, error)
	Recovery() (int64, error)
	Save(record interface{}) (int64, error)
}

func (q *query) setModel(model *Model) *query {
	q.model = model
	q.schema = model.schema
	return q
}

func (q *query) Get() (interface{}, error) {
	defer q.release()
	if q.model == nil {
		return nil, errors.New("model is nil")
	}
	q.Limit(0, 1)

	value := reflect.New(q.model.schema.schemaType)

	rows, err := q.selectInline()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}
		err = scanInterface(q.executor, rows, q.schema, columns, &value)
		return value.Interface(), err
	} else {
		return nil, errors.New("next is error")
	}
}

func (q *query) SoftDelete() (int64, error) {
	defer q.release()
	if q.model == nil {
		return 0, errors.New("model is nil")
	}

	if !q.model.schema.softDelete {
		return 0, errors.New("schema not have soft delete")
	}
	data := make(map[string]interface{})
	data[q.model.schema.deleteTime] = time.UTC
	return q.Update(data)
}

func (q *query) Recovery() (int64, error) {
	defer q.release()
	if q.model == nil {
		return 0, errors.New("model is nil")
	}
	if !q.model.schema.softDelete {
		return 0, errors.New("schema not have soft delete")
	}

	data := make(map[string]interface{})
	data[q.model.schema.deleteTime] = 0
	return q.Update(data)
}

func (q *query) Save(record interface{}) (int64, error) {
	if q.model == nil {
		return 0, errors.New("model is nil")
	}
	primary := q.model.schema.Primary
	if primary == nil {
		return 0, errors.New("Model do not have primary")
	}
	auto := q.model.schema.AutoIncrement
	if auto == "" {
		return 0, errors.New("Model do not have autoincrement")
	}
	value, err := q.model.schema.Get(record, auto)
	if err != nil {
		return 0, err
	}
	data, err := interface2map(q.model.schema, record)
	if err != nil {
		return 0, err
	}
	if value == 0 {
		result, err := q.Insert(data)
		if err != nil {
			return 0, err
		}
		q.model.schema.Set(record, auto, result)
		return result, err
	} else {
		return q.Update(data)
	}
}

func (q *query) modelUpdate(record map[string]interface{}) ([]string, [][]interface{}, error) {
	if q.model == nil {
		return nil, nil, errors.New("model is nil")
	}
	return formatData(q.model.schema, record)
}

func (q *query) modelInsert(records ...map[string]interface{}) ([]string, [][]interface{}, error) {
	if q.model == nil {
		return nil, nil, errors.New("model is nil")
	}
	return formatData(q.model.schema, records...)
}

func formatData(schema *Schema, records ...map[string]interface{}) ([]string, [][]interface{}, error) {
	columns := schema.columns
	fields := schema.fields
	if len(records) == 0 {
		return []string{}, [][]interface{}{}, nil
	}
	fieldsSlice := make([]string, 0, len(fields))
	//originSlice := make([]string, 0, len(fields))
	for f, ff := range fields {
		//originSlice = append(originSlice, f)
		if f == schema.AutoIncrement{
			continue
		}
		fieldsSlice = append(fieldsSlice, ff)
	}
	d := make([][]interface{}, len(records))
	for k, v := range records {
		g := make([]interface{}, len(fieldsSlice))
		for kk, field := range fieldsSlice {
			c, ok := v[field]
			if !ok {
				c = columns[field].d
			}
			g[kk] = c
		}
		d[k] = g
	}
	return fieldsSlice, d, nil
}

func interface2map(schema *Schema, record interface{}) (map[string]interface{}, error) {
	beanValue := reflect.ValueOf(record)
	if beanValue.Kind() != reflect.Ptr {
		return nil, errors.New("needs a pointer to a value")
	} else if beanValue.Elem().Kind() == reflect.Ptr {
		return nil, errors.New("a pointer to a pointer is not allowed")
	}
	data := map[string]interface{}{}
	columns := schema.columns
	fields := schema.fields
	recordValue := reflect.ValueOf(record).Elem()
	for name, colName := range fields {
		var val interface{}
		value := recordValue.FieldByName(name)
		col := columns[colName]
		if value.CanAddr() {
			if structConvert, ok := value.Addr().Interface().(Conversion); ok {
				data, err := structConvert.ToDB()
				if err != nil {
					return nil, err
				} else {
					val = data
				}
				goto APPEND
			}
		}

		if structConvert, ok := value.Interface().(Conversion); ok {
			data, err := structConvert.ToDB()
			if err != nil {
				return nil, err
			} else {
				val = data
			}
			goto APPEND
		}

		if col.t.Kind() == reflect.Ptr {
			if value.IsNil() {
				goto APPEND
			} else if !value.IsValid() {
				continue
			} else {
				// dereference ptr type to instance type
				value = value.Elem()
			}
		}
		switch value.Kind() {
		case reflect.Struct:
			if col.t.ConvertibleTo(TimeType) {
				t := value.Convert(TimeType).Interface().(time.Time)
				val = formatTime(col.sql, t)
			} else if valNul, ok := value.Interface().(driver.Valuer); ok {
				val, _ = valNul.Value()
				if val == nil {
					continue
				}
			} else if col.IsJson() {
				if col.IsBlob() {
					var bytes []byte
					var err error
					bytes, err = json.Marshal(value.Interface())
					if err != nil {
						return nil, err
					}
					val = bytes
				} else if col.IsText() {
					bytes, err := json.Marshal(value.Interface())
					if err != nil {
						return nil, err
					}
					val = string(bytes)
				}
			} else {
				val = value.Interface()
			}
		case reflect.Array, reflect.Slice, reflect.Map:
			if value == reflect.Zero(col.t) {
				val = value.Interface()
			}
			if value.IsNil() || !value.IsValid() || value.Len() == 0 {
				val = value.Interface()
			}

			if col.IsJson() {
				if col.IsBlob() {
					var bytes []byte
					var err error
					if (col.t.Kind() == reflect.Array || col.t.Kind() == reflect.Slice) &&
						col.t.Elem().Kind() == reflect.Uint8 {
						if value.Len() > 0 {
							val = value.Bytes()
						} else {
							continue
						}
					} else {
						bytes, err = json.Marshal(value.Interface())
						if err != nil {
							return nil, err
						}
						val = bytes
					}
				} else if col.IsText() {
					bytes, err := json.Marshal(value.Interface())
					if err != nil {
						return nil, err
					}
					val = string(bytes)
				}
			}
		case reflect.Bool:
			fallthrough
		case reflect.String:
			fallthrough
		case reflect.Int8, reflect.Int16, reflect.Int, reflect.Int32, reflect.Int64:
			fallthrough
		case reflect.Float32, reflect.Float64:
			fallthrough
		case reflect.Uint8, reflect.Uint16, reflect.Uint, reflect.Uint32, reflect.Uint64:
			fallthrough
		default:
			val = value.Interface()
		}
	APPEND:
		data[colName] = val
	}
	return data, nil
}
