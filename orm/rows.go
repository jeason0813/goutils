package orm

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)
type RawBytes []byte
type Rows interface {
	Next() bool
	Scan(row interface{}) error
	With(schema *Schema)
	Close() error
}

type rows struct {
	executor *executor
	rows     *sql.Rows
	columns  []string

	inited bool
	schema *Schema
}

func newRows(executor *executor, schema *Schema, sqlRows *sql.Rows) (*rows, error) {
	columns, err := sqlRows.Columns()
	if err != nil {
		return nil, err
	}
	r := &rows{
		executor: executor,
		rows:     sqlRows,
		columns:  columns,
		schema: schema,
		inited: false,
	}
	return r, nil
}

func (r *rows) Next() bool {
	return r.rows.Next()
}

func (r *rows) With(schema *Schema) {
	r.schema = schema
	r.inited = true
}

func (r *rows) Scan(row interface{}) error {
	var value reflect.Value
	value = reflect.ValueOf(row)
	if !r.inited {
		r.init(row)
	} else {
		if value.Kind() != reflect.Ptr || value.Elem().Kind() != reflect.Struct {
			return errors.New("needs a Struct pointer")
		}
	}
	return scanInterface(r.executor, r.rows, r.schema, r.columns, &value)
}

func (r *rows) Map() (map[string]interface{}, error) {
	resultsMap := make(map[string]interface{}, len(r.columns))
	value := reflect.ValueOf(resultsMap)
	if err := scanMap(r.rows, r.columns, &value); err != nil {
		return nil, err
	}
	return resultsMap, nil
}

func (r *rows) Slice() ([]interface{}, error) {
	resultsSlice := make([]interface{}, len(r.columns))
	value := reflect.ValueOf(resultsSlice)
	if err := scanSlice(r.rows, r.columns, &value); err != nil {
		return nil, err
	}
	return resultsSlice, nil
}

func (r *rows) Close() error {
	return r.rows.Close()
}

func (r *rows) init(row interface{}) error {
	schema, err := NewSchema(row)
	if err != nil {
		return err
	}
	r.schema = schema
	r.inited = true
	return nil
}

func scanInterface(executor *executor, rows *sql.Rows, schema *Schema, columns []string, value *reflect.Value) error {
	t := value.Elem().Type()
	bean := value.Interface()
	switch bean.(type) {
	case sql.NullInt64, sql.NullBool, sql.NullFloat64, sql.NullString:
		return rows.Scan(&bean)
	case *sql.NullInt64, *sql.NullBool, *sql.NullFloat64, *sql.NullString:
		return rows.Scan(bean)
	}
	switch t.Kind() {
	case reflect.Struct:
		return scanStruct(executor, rows, schema, columns, value)
	case reflect.Slice:
		return scanSlice(rows, columns, value)
	case reflect.Map:
		return scanMap(rows, columns, value)
	default:
		return rows.Scan(bean)
	}
	return errors.New("scanInterface is error")
}

func scanStruct(executor *executor, rows *sql.Rows, schema *Schema, sqlColumns []string, value *reflect.Value) error {
	scanResults := make([]interface{}, len(sqlColumns))
	for i := 0; i < len(sqlColumns); i++ {
		var cell []byte
		scanResults[i] = &cell
	}
	valueE := value.Elem()

	columns := schema.columns

	if err := rows.Scan(scanResults...); err != nil {
		return err
	}
	for ii, key := range sqlColumns {
		col, ok := columns[key]
		if !ok {
			continue
		}
		raw := scanResults[ii]
		rawElem := reflect.ValueOf(raw).Elem()
		rawValue := rawElem.Interface()
		// if row is null then ignore
		if rawValue == nil {
			continue
		}
		f := valueE.FieldByName(col.f)
		fieldValue := &f
		// sql包中默认的转码
		//err := convertAssign(fieldValue.Interface(), rawValue)
		//if err == nil {
		//	continue
		//}

		bs, ok := rawValue.([]byte)
		if !ok {
			bs, ok = asBytes(nil, rawElem)
			if !ok {
				continue
			}
		}

		if fieldValue.CanAddr() {
			if structConvert, ok := fieldValue.Addr().Interface().(Conversion); ok {
				if err := structConvert.FromDB(bs); err != nil {
					return err
				}
				continue
			}
		}

		if _, ok := fieldValue.Interface().(Conversion); ok {
			if fieldValue.Kind() == reflect.Ptr && fieldValue.IsNil() {
				fieldValue.Set(reflect.New(fieldValue.Type().Elem()))
			}
			if err := fieldValue.Interface().(Conversion).FromDB(bs); err != nil {
				return err
			}
			continue
		}
		fieldType := fieldValue.Type()
		if col.IsJson() {
			if len(bs) > 0 {
				if fieldType.Kind() == reflect.String {
					fieldValue.SetString(string(bs))
					continue
				}
				if fieldValue.CanAddr() {
					err := json.Unmarshal(bs, fieldValue.Addr().Interface())
					if err != nil {
						return err
					}
				} else {
					x := reflect.New(fieldType)
					err := json.Unmarshal(bs, x.Interface())
					if err != nil {
						return err
					}
					fieldValue.Set(x.Elem())
				}
			}

			continue
		}

		if col.t.Kind() == reflect.Ptr {
			if !value.IsValid() {
				continue
			} else {
				// dereference ptr type to instance type
				v := fieldValue.Elem()
				fieldValue = &v
			}
		}

		switch fieldType.Kind() {
		case BytesType.Kind():
			fieldValue.Set(rawElem)
		case reflect.Complex64, reflect.Complex128, reflect.Slice, reflect.Array, reflect.Map:
			if len(bs) > 0 {
				if fieldValue.CanAddr() {
					err := json.Unmarshal(bs, fieldValue.Addr().Interface())
					if err != nil {
						return err
					}
				} else {
					x := reflect.New(fieldType)
					err := json.Unmarshal(bs, x.Interface())
					if err != nil {
						return err
					}
					fieldValue.Set(x.Elem())
				}
			}
		case reflect.String:
			fieldValue.SetString(string(bs))
		case reflect.Bool:
			v, err := asBool(bs)
			if err != nil {
				return fmt.Errorf("arg %v as bool: %s", key, err.Error())
			}
			fieldValue.Set(reflect.ValueOf(v))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			sdata := string(bs)
			var x int64
			var err error
			// for mysql, when use bit, it returned \x01
			if col.sql == Bit && executor.driver == MYSQL { // !nashtsai! TODO dialect needs to provide conversion interface API
				if len(bs) == 1 {
					x = int64(bs[0])
				} else {
					x = 0
				}
			} else if strings.HasPrefix(sdata, "0x") {
				x, err = strconv.ParseInt(sdata, 16, 64)
			} else if strings.HasPrefix(sdata, "0") {
				x, err = strconv.ParseInt(sdata, 8, 64)
			} else if strings.EqualFold(sdata, "true") {
				x = 1
			} else if strings.EqualFold(sdata, "false") {
				x = 0
			} else {
				x, err = strconv.ParseInt(sdata, 10, 64)
			}
			if err != nil {
				return fmt.Errorf("arg %v as int: %s", key, err.Error())
			}
			fieldValue.SetInt(x)
		case reflect.Float32, reflect.Float64:
			x, err := strconv.ParseFloat(string(bs), 64)
			if err != nil {
				return fmt.Errorf("arg %v as float64: %s", key, err.Error())
			}
			fieldValue.SetFloat(x)
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
			x, err := strconv.ParseUint(string(bs), 10, 64)
			if err != nil {
				return fmt.Errorf("arg %v as int: %s", key, err.Error())
			}
			fieldValue.SetUint(x)
		case reflect.Struct:
			if fieldType.ConvertibleTo(TimeType) {
				x, err := byte2Time(executor, col, bs)
				if err != nil {
					return err
				}
				fieldValue.Set(reflect.ValueOf(x).Convert(fieldType))
			} else if nulVal, ok := fieldValue.Addr().Interface().(sql.Scanner); ok {
				if err := nulVal.Scan(rawValue); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func scanSlice(rows *sql.Rows, columns []string, value *reflect.Value) error {
	vvv := value.Elem()

	newDest := make([]interface{}, len(columns))

	for j := 0; j < len(columns); j++ {
		if j >= vvv.Len() {
			newDest[j] = reflect.New(vvv.Type().Elem()).Interface()
		} else {
			newDest[j] = vvv.Index(j).Addr().Interface()
		}
	}

	err := rows.Scan(newDest...)
	if err != nil {
		return err
	}

	srcLen := vvv.Len()
	for i := srcLen; i < len(columns); i++ {
		vvv = reflect.Append(vvv, reflect.ValueOf(newDest[i]).Elem())
	}
	return nil
}

func scanMap(rows *sql.Rows, columns []string, value *reflect.Value) error {
	newDest := make([]interface{}, len(columns))
	vvv := value.Elem()

	for i, _ := range columns {
		newDest[i] = reflect.New(vvv.Type().Elem()).Interface()
		//v := reflect.New(vvv.Type().Elem())
		//newDest[i] = v.Interface()
	}

	err := rows.Scan(newDest...)
	if err != nil {
		return err
	}

	for i, name := range columns {
		vname := reflect.ValueOf(name)
		vvv.SetMapIndex(vname, reflect.ValueOf(newDest[i]).Elem())
	}

	return nil
}

func value2Bytes(rawValue *reflect.Value) ([]byte, error) {
	str, err := value2String(rawValue)
	if err != nil {
		return nil, err
	}
	return []byte(str), nil
}

func bytes2Value(executor *executor, col *column, fieldValue *reflect.Value, data []byte) error {
	if structConvert, ok := fieldValue.Addr().Interface().(Conversion); ok {
		return structConvert.FromDB(data)
	}

	if structConvert, ok := fieldValue.Interface().(Conversion); ok {
		return structConvert.FromDB(data)
	}

	var v interface{}
	key := col.sql
	fieldType := fieldValue.Type()

	switch fieldType.Kind() {
	case reflect.Complex64, reflect.Complex128:
		x := reflect.New(fieldType)
		if len(data) > 0 {
			err := json.Unmarshal(data, x.Interface())
			if err != nil {
				return err
			}
			fieldValue.Set(x.Elem())
		}
	case reflect.Slice, reflect.Array, reflect.Map:
		v = data
		t := fieldType.Elem()
		k := t.Kind()
		if col.IsText() {
			x := reflect.New(fieldType)
			if len(data) > 0 {
				err := json.Unmarshal(data, x.Interface())
				if err != nil {
					return err
				}
				fieldValue.Set(x.Elem())
			}
		} else if col.IsBlob() {
			if k == reflect.Uint8 {
				fieldValue.Set(reflect.ValueOf(v))
			} else {
				x := reflect.New(fieldType)
				if len(data) > 0 {
					err := json.Unmarshal(data, x.Interface())
					if err != nil {
						return err
					}
					fieldValue.Set(x.Elem())
				}
			}
		} else {
			return errors.New("")
		}
	case reflect.String:
		fieldValue.SetString(string(data))
	case reflect.Bool:
		v, err := asBool(data)
		if err != nil {
			return fmt.Errorf("arg %v as bool: %s", key, err.Error())
		}
		fieldValue.Set(reflect.ValueOf(v))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		sdata := string(data)
		var x int64
		var err error
		// for mysql, when use bit, it returned \x01
		if col.sql == Bit && executor.driver == MYSQL { // !nashtsai! TODO dialect needs to provide conversion interface API
			if len(data) == 1 {
				x = int64(data[0])
			} else {
				x = 0
			}
		} else if strings.HasPrefix(sdata, "0x") {
			x, err = strconv.ParseInt(sdata, 16, 64)
		} else if strings.HasPrefix(sdata, "0") {
			x, err = strconv.ParseInt(sdata, 8, 64)
		} else if strings.EqualFold(sdata, "true") {
			x = 1
		} else if strings.EqualFold(sdata, "false") {
			x = 0
		} else {
			x, err = strconv.ParseInt(sdata, 10, 64)
		}
		if err != nil {
			return fmt.Errorf("arg %v as int: %s", key, err.Error())
		}
		fieldValue.SetInt(x)
	case reflect.Float32, reflect.Float64:
		x, err := strconv.ParseFloat(string(data), 64)
		if err != nil {
			return fmt.Errorf("arg %v as float64: %s", key, err.Error())
		}
		fieldValue.SetFloat(x)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		x, err := strconv.ParseUint(string(data), 10, 64)
		if err != nil {
			return fmt.Errorf("arg %v as int: %s", key, err.Error())
		}
		fieldValue.SetUint(x)
		//Currently only support Time type
	case reflect.Struct:
		// !<winxxp>! 增加支持sql.Scanner接口的结构，如sql.NullString
		if nulVal, ok := fieldValue.Addr().Interface().(sql.Scanner); ok {
			if err := nulVal.Scan(data); err != nil {
				return fmt.Errorf("sql.Scan(%v) failed: %s ", data, err.Error())
			}
		} else {
			if fieldType.ConvertibleTo(TimeType) {
				x, err := byte2Time(executor, col, data)
				if err != nil {
					return err
				}
				v = x
				fieldValue.Set(reflect.ValueOf(v).Convert(fieldType))
			}
		}
	case reflect.Ptr:
		// !nashtsai! TODO merge duplicated codes above
		//typeStr := fieldType.String()
		switch fieldType.Elem().Kind() {
		// case "*string":
		case StringType.Kind():
			x := string(data)
			fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			// case "*bool":
		case BoolType.Kind():
			d := string(data)
			v, err := strconv.ParseBool(d)
			if err != nil {
				return fmt.Errorf("arg %v as bool: %s", key, err.Error())
			}
			fieldValue.Set(reflect.ValueOf(&v).Convert(fieldType))
			// case "*complex64":
		case Complex64Type.Kind():
			var x complex64
			if len(data) > 0 {
				err := json.Unmarshal(data, &x)
				if err != nil {
					return err
				}
				fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			}
			// case "*complex128":
		case Complex128Type.Kind():
			var x complex128
			if len(data) > 0 {
				err := json.Unmarshal(data, &x)
				if err != nil {
					return err
				}
				fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			}
			// case "*float64":
		case Float64Type.Kind():
			x, err := strconv.ParseFloat(string(data), 64)
			if err != nil {
				return fmt.Errorf("arg %v as float64: %s", key, err.Error())
			}
			fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			// case "*float32":
		case Float32Type.Kind():
			var x float32
			x1, err := strconv.ParseFloat(string(data), 32)
			if err != nil {
				return fmt.Errorf("arg %v as float32: %s", key, err.Error())
			}
			x = float32(x1)
			fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			// case "*uint64":
		case Uint64Type.Kind():
			var x uint64
			x, err := strconv.ParseUint(string(data), 10, 64)
			if err != nil {
				return fmt.Errorf("arg %v as int: %s", key, err.Error())
			}
			fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			// case "*uint":
		case UintType.Kind():
			var x uint
			x1, err := strconv.ParseUint(string(data), 10, 64)
			if err != nil {
				return fmt.Errorf("arg %v as int: %s", key, err.Error())
			}
			x = uint(x1)
			fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			// case "*uint32":
		case Uint32Type.Kind():
			var x uint32
			x1, err := strconv.ParseUint(string(data), 10, 64)
			if err != nil {
				return fmt.Errorf("arg %v as int: %s", key, err.Error())
			}
			x = uint32(x1)
			fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			// case "*uint8":
		case Uint8Type.Kind():
			var x uint8
			x1, err := strconv.ParseUint(string(data), 10, 64)
			if err != nil {
				return fmt.Errorf("arg %v as int: %s", key, err.Error())
			}
			x = uint8(x1)
			fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			// case "*uint16":
		case Uint16Type.Kind():
			var x uint16
			x1, err := strconv.ParseUint(string(data), 10, 64)
			if err != nil {
				return fmt.Errorf("arg %v as int: %s", key, err.Error())
			}
			x = uint16(x1)
			fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			// case "*int64":
		case Int64Type.Kind():
			sdata := string(data)
			var x int64
			var err error
			// for mysql, when use bit, it returned \x01
			if col.sql == Bit && executor.driver == MYSQL {
				if len(data) == 1 {
					x = int64(data[0])
				} else {
					x = 0
				}
			} else if strings.HasPrefix(sdata, "0x") {
				x, err = strconv.ParseInt(sdata, 16, 64)
			} else if strings.HasPrefix(sdata, "0") {
				x, err = strconv.ParseInt(sdata, 8, 64)
			} else {
				x, err = strconv.ParseInt(sdata, 10, 64)
			}
			if err != nil {
				return fmt.Errorf("arg %v as int: %s", key, err.Error())
			}
			fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			// case "*int":
		case IntType.Kind():
			sdata := string(data)
			var x int
			var x1 int64
			var err error
			// for mysql, when use bit, it returned \x01
			if col.sql == Bit && executor.driver == MYSQL {
				if len(data) == 1 {
					x = int(data[0])
				} else {
					x = 0
				}
			} else if strings.HasPrefix(sdata, "0x") {
				x1, err = strconv.ParseInt(sdata, 16, 64)
				x = int(x1)
			} else if strings.HasPrefix(sdata, "0") {
				x1, err = strconv.ParseInt(sdata, 8, 64)
				x = int(x1)
			} else {
				x1, err = strconv.ParseInt(sdata, 10, 64)
				x = int(x1)
			}
			if err != nil {
				return fmt.Errorf("arg %v as int: %s", key, err.Error())
			}
			fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			// case "*int32":
		case Int32Type.Kind():
			sdata := string(data)
			var x int32
			var x1 int64
			var err error
			// for mysql, when use bit, it returned \x01
			if col.sql == Bit && executor.driver == MYSQL {
				if len(data) == 1 {
					x = int32(data[0])
				} else {
					x = 0
				}
			} else if strings.HasPrefix(sdata, "0x") {
				x1, err = strconv.ParseInt(sdata, 16, 64)
				x = int32(x1)
			} else if strings.HasPrefix(sdata, "0") {
				x1, err = strconv.ParseInt(sdata, 8, 64)
				x = int32(x1)
			} else {
				x1, err = strconv.ParseInt(sdata, 10, 64)
				x = int32(x1)
			}
			if err != nil {
				return fmt.Errorf("arg %v as int: %s", key, err.Error())
			}
			fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			// case "*int8":
		case Int8Type.Kind():
			sdata := string(data)
			var x int8
			var x1 int64
			var err error
			// for mysql, when use bit, it returned \x01
			if col.sql == Bit && executor.driver == MYSQL {
				if len(data) == 1 {
					x = int8(data[0])
				} else {
					x = 0
				}
			} else if strings.HasPrefix(sdata, "0x") {
				x1, err = strconv.ParseInt(sdata, 16, 64)
				x = int8(x1)
			} else if strings.HasPrefix(sdata, "0") {
				x1, err = strconv.ParseInt(sdata, 8, 64)
				x = int8(x1)
			} else {
				x1, err = strconv.ParseInt(sdata, 10, 64)
				x = int8(x1)
			}
			if err != nil {
				return fmt.Errorf("arg %v as int: %s", key, err.Error())
			}
			fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			// case "*int16":
		case Int16Type.Kind():
			sdata := string(data)
			var x int16
			var x1 int64
			var err error
			// for mysql, when use bit, it returned \x01
			if col.sql == Bit && executor.driver == MYSQL {
				if len(data) == 1 {
					x = int16(data[0])
				} else {
					x = 0
				}
			} else if strings.HasPrefix(sdata, "0x") {
				x1, err = strconv.ParseInt(sdata, 16, 64)
				x = int16(x1)
			} else if strings.HasPrefix(sdata, "0") {
				x1, err = strconv.ParseInt(sdata, 8, 64)
				x = int16(x1)
			} else {
				x1, err = strconv.ParseInt(sdata, 10, 64)
				x = int16(x1)
			}
			if err != nil {
				return fmt.Errorf("arg %v as int: %s", key, err.Error())
			}
			fieldValue.Set(reflect.ValueOf(&x).Convert(fieldType))
			// case "*SomeStruct":
		case reflect.Struct:
			switch fieldType {
			// case "*.time.Time":
			case PtrTimeType:
				x, err := byte2Time(executor, col, data)
				if err != nil {
					return err
				}
				v = x
				fieldValue.Set(reflect.ValueOf(&x))
			default:
				return fmt.Errorf("unsupported struct type in Scan: %s", fieldValue.Type().String())
			}
		default:
			return fmt.Errorf("unsupported type in Scan: %s", fieldValue.Type().String())
		}
	default:
		return fmt.Errorf("unsupported type in Scan: %s", fieldValue.Type().String())
	}

	return nil
}
