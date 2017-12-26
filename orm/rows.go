package orm

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Rows interface {
	Next() bool
	Scan(row interface{}) error
	With(schema *Schema)
	Close() error
}

type rows struct {
	executor *Executor
	rows     *sql.Rows
	columns  []string

	inited bool
	schema *Schema
}

func newRows(executor *Executor, rows *sql.Rows) (*rows, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	r := &rows{
		executor: executor,
		rows:     rows,
		columns:  columns,

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
	scanResultContainers := make([]interface{}, len(r.columns))
	for i := 0; i < len(r.columns); i++ {
		var scanResultContainer interface{}
		scanResultContainers[i] = &scanResultContainer
	}
	if err := r.rows.Scan(scanResultContainers...); err != nil {
		return nil, err
	}

	for ii, key := range r.columns {
		resultsMap[key] = reflect.Indirect(reflect.ValueOf(scanResultContainers[ii])).Interface()
	}
	return resultsMap, nil
}

func (r *rows) Slice() ([]interface{}, error) {
	resultsSlice := make([]interface{}, len(r.columns))
	scanResultContainers := make([]interface{}, len(r.columns))
	for i := 0; i < len(r.columns); i++ {
		var scanResultContainer interface{}
		scanResultContainers[i] = &scanResultContainer
	}
	if err := r.rows.Scan(scanResultContainers...); err != nil {
		return nil, err
	}

	for ii, _ := range r.columns {
		resultsSlice = append(resultsSlice, reflect.Indirect(reflect.ValueOf(scanResultContainers[ii])).Interface())
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

func scanInterface(executor *Executor, rows *sql.Rows, schema *Schema, columns []string, value *reflect.Value) error {
	t := schema.schemaType
	schemaColumns := schema.Columns()
	bean := value.Interface()
	switch bean.(type) {
	case sql.NullInt64, sql.NullBool, sql.NullFloat64, sql.NullString:
		return rows.Scan(&bean)
	case *sql.NullInt64, *sql.NullBool, *sql.NullFloat64, *sql.NullString:
		return rows.Scan(bean)
	}
	switch t.Kind() {
	case reflect.Struct:
		return scanStruct(executor, rows, schemaColumns, columns, value)
	case reflect.Slice:
		return scanSlice(rows, columns, value)
	case reflect.Map:
		return scanMap(rows, columns, value)
	default:
		return rows.Scan(bean)
	}
	return errors.New("scanInterface is error")
}

func scanStruct(executor *Executor, rows *sql.Rows, fields map[string]*column, columns []string, value *reflect.Value) error {
	scanResults := make([]interface{}, len(columns))
	for i := 0; i < len(columns); i++ {
		var cell interface{}
		scanResults[i] = &cell
	}
	if err := rows.Scan(scanResults...); err != nil {
		return err
	}
	var tempMap = make(map[string]int)
	for ii, key := range columns {
		var idx int
		var ok bool
		var lKey = strings.ToLower(key)
		if idx, ok = tempMap[lKey]; !ok {
			idx = 0
		} else {
			idx = idx + 1
		}
		tempMap[lKey] = idx

		col, ok := fields[key]
		if !ok {
			continue
		}
		rawValue := reflect.Indirect(reflect.ValueOf(scanResults[ii]))
		f := value.FieldByName(key)
		fieldValue := &f
		// if row is null then ignore
		if rawValue.Interface() == nil {
			continue
		}

		if fieldValue.CanAddr() {
			if structConvert, ok := fieldValue.Addr().Interface().(Conversion); ok {
				if data, err := value2Bytes(&rawValue); err == nil {
					if err := structConvert.FromDB(data); err != nil {
						return err
					}
				} else {
					return err
				}
				continue
			}
		}

		if _, ok := fieldValue.Interface().(Conversion); ok {
			if data, err := value2Bytes(&rawValue); err == nil {
				if fieldValue.Kind() == reflect.Ptr && fieldValue.IsNil() {
					fieldValue.Set(reflect.New(fieldValue.Type().Elem()))
				}
				fieldValue.Interface().(Conversion).FromDB(data)
			} else {
				return err
			}
			continue
		}

		rawValueType := reflect.TypeOf(rawValue.Interface())
		vv := reflect.ValueOf(rawValue.Interface())

		hasAssigned := false
		fieldType := fieldValue.Type()

		if col.IsJson() {
			var bs []byte
			if rawValueType.Kind() == reflect.String {
				bs = []byte(vv.String())
			} else if rawValueType.ConvertibleTo(BytesType) {
				bs = vv.Bytes()
			} else {
				return fmt.Errorf("unsupported database data type: %s %v", key, rawValueType.Kind())
			}

			hasAssigned = true

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

		fieldType = fieldValue.Type()

		switch fieldType.Kind() {
		case reflect.Complex64, reflect.Complex128:
			var bs []byte
			if rawValueType.Kind() == reflect.String {
				bs = []byte(vv.String())
			} else if rawValueType.ConvertibleTo(BytesType) {
				bs = vv.Bytes()
			}

			hasAssigned = true
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
		case reflect.Slice, reflect.Array:
			switch rawValueType.Kind() {
			case reflect.Slice, reflect.Array:
				switch rawValueType.Elem().Kind() {
				case reflect.Uint8:
					if fieldType.Elem().Kind() == reflect.Uint8 {
						hasAssigned = true
						if col.IsText() {
							x := reflect.New(fieldType)
							err := json.Unmarshal(vv.Bytes(), x.Interface())
							if err != nil {
								return err
							}
							fieldValue.Set(x.Elem())
						} else {
							if fieldValue.Len() > 0 {
								for i := 0; i < fieldValue.Len(); i++ {
									if i < vv.Len() {
										fieldValue.Index(i).Set(vv.Index(i))
									}
								}
							} else {
								for i := 0; i < vv.Len(); i++ {
									fieldValue.Set(reflect.Append(*fieldValue, vv.Index(i)))
								}
							}
						}
					}
				}
			}
		case reflect.String:
			if rawValueType.Kind() == reflect.String {
				hasAssigned = true
				fieldValue.SetString(vv.String())
			}
		case reflect.Bool:
			if rawValueType.Kind() == reflect.Bool {
				hasAssigned = true
				fieldValue.SetBool(vv.Bool())
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			switch rawValueType.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				hasAssigned = true
				fieldValue.SetInt(vv.Int())
			}
		case reflect.Float32, reflect.Float64:
			switch rawValueType.Kind() {
			case reflect.Float32, reflect.Float64:
				hasAssigned = true
				fieldValue.SetFloat(vv.Float())
			}
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
			switch rawValueType.Kind() {
			case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
				hasAssigned = true
				fieldValue.SetUint(vv.Uint())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				hasAssigned = true
				fieldValue.SetUint(uint64(vv.Int()))
			}
		case reflect.Struct:
			if fieldType.ConvertibleTo(TimeType) {
				dbTZ := executor.DatabaseTZ
				if col.zone != nil {
					dbTZ = col.zone
				}

				if rawValueType == TimeType {
					hasAssigned = true

					t := vv.Convert(TimeType).Interface().(time.Time)

					z, _ := t.Zone()
					// set new location if database don't save timezone or give an incorrect timezone
					if len(z) == 0 || t.Year() == 0 || t.Location().String() != dbTZ.String() { // !nashtsai! HACK tmp work around for lib/pq doesn't properly time with location
						t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(),
							t.Minute(), t.Second(), t.Nanosecond(), dbTZ)
					}

					t = t.In(executor.TZLocation)
					fieldValue.Set(reflect.ValueOf(t).Convert(fieldType))
				} else if rawValueType == IntType || rawValueType == Int64Type ||
					rawValueType == Int32Type {
					hasAssigned = true

					t := time.Unix(vv.Int(), 0).In(executor.TZLocation)
					fieldValue.Set(reflect.ValueOf(t).Convert(fieldType))
				} else {
					if d, ok := vv.Interface().([]uint8); ok {
						hasAssigned = true
						t, err := byte2Time(executor, col, d)
						if err != nil {
							hasAssigned = false
						} else {
							fieldValue.Set(reflect.ValueOf(t).Convert(fieldType))
						}
					} else if d, ok := vv.Interface().(string); ok {
						hasAssigned = true
						t, err := str2Time(executor, col, d)
						if err != nil {
							hasAssigned = false
						} else {
							fieldValue.Set(reflect.ValueOf(t).Convert(fieldType))
						}
					} else {
						return fmt.Errorf("rawValueType is %v, value is %v", rawValueType, vv.Interface())
					}
				}
			} else if nulVal, ok := fieldValue.Addr().Interface().(sql.Scanner); ok {
				hasAssigned = true
				if err := nulVal.Scan(vv.Interface()); err != nil {
					hasAssigned = false
				}
			} else if col.IsJson() {
				if rawValueType.Kind() == reflect.String {
					hasAssigned = true
					x := reflect.New(fieldType)
					if len([]byte(vv.String())) > 0 {
						err := json.Unmarshal([]byte(vv.String()), x.Interface())
						if err != nil {
							return err
						}
						fieldValue.Set(x.Elem())
					}
				} else if rawValueType.Kind() == reflect.Slice {
					hasAssigned = true
					x := reflect.New(fieldType)
					if len(vv.Bytes()) > 0 {
						err := json.Unmarshal(vv.Bytes(), x.Interface())
						if err != nil {
							return err
						}
						fieldValue.Set(x.Elem())
					}
				}
			}
		}

		if !hasAssigned {
			//	data, err := value2Bytes(&rawValue)
			//	if err != nil {
			//		return err
			//	}
			//
			//	if err = bytes2Value(executor, col, fieldValue, data); err != nil {
			//		return err
			//	}
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

func bytes2Value(executor *Executor, col *column, fieldValue *reflect.Value, data []byte) error {
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
