package orm

import (
	"errors"
	"reflect"
	"sort"
	"time"

	"github.com/ueffort/goutils/event"
)

type Conversion interface {
	FromDB([]byte) error
	ToDB() ([]byte, error)
}

type Schema struct {
	AutoIncrement string
	Primary       []string

	schemaType    reflect.Type
	schemaPtrType reflect.Type
	columns       map[string]*column
	fields        map[string]string
	record        interface{}

	softDelete bool
	createTime string
	updateTime string
	deleteTime string

	event event.Event
}

func NewSchema(record interface{}) (*Schema, error) {

	s := &Schema{
		AutoIncrement: "",
		Primary:       []string{},
		softDelete:    false,
		event:         event.New(),
	}
	err := s.With(record)
	return s, err
}

func (s *Schema) Default(field string, value interface{}) *Schema {
	s.columns[field].d = value
	return s
}

func (s *Schema) With(record interface{}) error {
	s.fields = make(map[string]string)
	s.columns = make(map[string]*column)
	value := reflect.ValueOf(record)
	if value.Kind() != reflect.Ptr || value.Elem().Kind() != reflect.Struct {
		return errors.New("needs a Struct pointer")
	}
	valueE := value.Elem()
	tp := reflect.TypeOf(record)
	t := tp.Elem()
	for k := 0; k < t.NumField(); k++ {
		col := t.Field(k)
		tag := col.Tag
		v := valueE.FieldByName(col.Name)
		d := v.Interface()
		_, required := tag.Lookup("required")
		t, tset := tag.Lookup("time")

		field := tag.Get("json")
		if field != "" {
			s.fields[col.Name] = field
		} else {
			s.fields[col.Name] = col.Name
		}

		c := &column{
			col.Name,
			col.Type,
			v,
			d,
			tag.Get("sql"),
			required,
			time.UTC,
		}
		if tset {
			switch t {
			case "local":
				c.zone = time.Local
			case "utc":
				c.zone = time.UTC
			default:
				zone, err := time.LoadLocation(t)
				if err != nil {
					return err
				}
				c.zone = zone
			}
		}
		s.columns[s.fields[col.Name]] = c
		key := tag.Get("orm")
		switch key {
		case "auto":
			s.SetAutoIncrement(col.Name)
		case "primary":
			s.SetPrimary(col.Name)
		case "deleteTime":
			s.SoftDelete(col.Name)
		case "createTime":
			s.SetCreateTime(col.Name)
		case "updateTime":
			s.SetUpdateTime(col.Name)
		}
	}
	s.record = record
	s.schemaType = t
	s.schemaPtrType = tp
	return nil
}

func (s *Schema) SetAutoIncrement(field string) *Schema {
	s.AutoIncrement = field
	s.Primary = []string{field}
	return s
}

func (s *Schema) SetPrimary(primary ...string) *Schema {
	s.Primary = append(s.Primary, primary...)
	return s
}

func (s *Schema) SetTime(createTime string, updateTime string, deleteTime string) *Schema {
	s.SetCreateTime(createTime)
	s.SetUpdateTime(updateTime)
	s.SoftDelete(deleteTime)
	return s
}

func (s *Schema) SetCreateTime(createTime string) *Schema {
	if createTime == "" {
		createTime = "CreateTime"
	}
	s.createTime = createTime
	s.columns[s.fields[createTime]].sql = "dateTime"
	return s
}

func (s *Schema) SetUpdateTime(updateTime string) *Schema {
	if updateTime == "" {
		updateTime = "UpdateTime"
	}
	s.updateTime = updateTime
	s.columns[s.fields[updateTime]].sql = "dateTime"
	return s
}

func (s *Schema) SoftDelete(deleteTime string) *Schema {
	if deleteTime == "" {
		deleteTime = "DeleteTime"
	}
	s.softDelete = true
	s.deleteTime = deleteTime
	s.columns[s.fields[deleteTime]].sql = "dateTime"
	return s
}

func (s *Schema) Set(record interface{}, field string, v interface{}) error {
	value := reflect.ValueOf(record)
	elem := value.Elem()
	if value.Kind() != reflect.Ptr || elem.Kind() != reflect.Struct {
		return errors.New("needs a Struct pointer")
	}
	fieldValue := elem.FieldByName(field)
	fieldValue.Set(reflect.ValueOf(v))
	return nil
}

func (s *Schema) Get(record interface{}, field string) (interface{}, error) {
	value := reflect.ValueOf(record)
	elem := value.Elem()
	if value.Kind() != reflect.Ptr || elem.Kind() != reflect.Struct {
		return nil, errors.New("needs a Struct pointer")
	}
	fieldValue := elem.FieldByName(field)
	return fieldValue.Interface(), nil
}

func (s *Schema) On(event string, f func(record interface{}) error) error {
	return s.event.On(event, f)
}

func (s *Schema) Emit(event string, record interface{}) error {
	_, err := s.event.Fire(event, record)
	return err
}

const (
	POSTGRES = "postgres"
	SQLITE   = "sqlite3"
	MYSQL    = "mysql"
	MSSQL    = "mssql"
	ORACLE   = "oracle"
)

const (
	UNKNOW_TYPE = iota
	TEXT_TYPE
	BLOB_TYPE
	TIME_TYPE
	NUMERIC_TYPE
)
const (
	zeroTime0 = "0000-00-00 00:00:00"
	zeroTime1 = "0001-01-01 00:00:00"
)

type column struct {
	f        string
	t        reflect.Type
	v        reflect.Value
	d        interface{}
	sql      string
	required bool
	zone     *time.Location
}

func (c *column) IsType(st int) bool {
	if t, ok := SqlTypes[c.sql]; ok && t == st {
		return true
	}
	return false
}

func (c *column) IsText() bool {
	return c.IsType(TEXT_TYPE)
}

func (c *column) IsBlob() bool {
	return c.IsType(BLOB_TYPE)
}

func (c *column) IsTime() bool {
	return c.IsType(TIME_TYPE)
}

func (c *column) IsNumeric() bool {
	return c.IsType(NUMERIC_TYPE)
}

func (c *column) IsJson() bool {
	return c.sql == Json || c.sql == Jsonb
}

var (
	Bit       = "BIT"
	TinyInt   = "TINYINT"
	SmallInt  = "SMALLINT"
	MediumInt = "MEDIUMINT"
	Int       = "INT"
	Integer   = "INTEGER"
	BigInt    = "BIGINT"

	Enum = "ENUM"
	Set  = "SET"

	Char       = "CHAR"
	Varchar    = "VARCHAR"
	NVarchar   = "NVARCHAR"
	TinyText   = "TINYTEXT"
	Text       = "TEXT"
	Clob       = "CLOB"
	MediumText = "MEDIUMTEXT"
	LongText   = "LONGTEXT"
	Uuid       = "UUID"

	Date       = "DATE"
	DateTime   = "DATETIME"
	Time       = "TIME"
	TimeStamp  = "TIMESTAMP"
	TimeStampz = "TIMESTAMPZ"

	Decimal = "DECIMAL"
	Numeric = "NUMERIC"

	Real   = "REAL"
	Float  = "FLOAT"
	Double = "DOUBLE"

	Binary     = "BINARY"
	VarBinary  = "VARBINARY"
	TinyBlob   = "TINYBLOB"
	Blob       = "BLOB"
	MediumBlob = "MEDIUMBLOB"
	LongBlob   = "LONGBLOB"
	Bytea      = "BYTEA"

	Bool    = "BOOL"
	Boolean = "BOOLEAN"

	Serial    = "SERIAL"
	BigSerial = "BIGSERIAL"

	Json  = "JSON"
	Jsonb = "JSONB"

	SqlTypes = map[string]int{
		Bit:       NUMERIC_TYPE,
		TinyInt:   NUMERIC_TYPE,
		SmallInt:  NUMERIC_TYPE,
		MediumInt: NUMERIC_TYPE,
		Int:       NUMERIC_TYPE,
		Integer:   NUMERIC_TYPE,
		BigInt:    NUMERIC_TYPE,

		Enum:  TEXT_TYPE,
		Set:   TEXT_TYPE,
		Json:  TEXT_TYPE,
		Jsonb: TEXT_TYPE,

		Char:       TEXT_TYPE,
		Varchar:    TEXT_TYPE,
		NVarchar:   TEXT_TYPE,
		TinyText:   TEXT_TYPE,
		Text:       TEXT_TYPE,
		MediumText: TEXT_TYPE,
		LongText:   TEXT_TYPE,
		Uuid:       TEXT_TYPE,
		Clob:       TEXT_TYPE,

		Date:       TIME_TYPE,
		DateTime:   TIME_TYPE,
		Time:       TIME_TYPE,
		TimeStamp:  TIME_TYPE,
		TimeStampz: TIME_TYPE,

		Decimal: NUMERIC_TYPE,
		Numeric: NUMERIC_TYPE,
		Real:    NUMERIC_TYPE,
		Float:   NUMERIC_TYPE,
		Double:  NUMERIC_TYPE,

		Binary:    BLOB_TYPE,
		VarBinary: BLOB_TYPE,

		TinyBlob:   BLOB_TYPE,
		Blob:       BLOB_TYPE,
		MediumBlob: BLOB_TYPE,
		LongBlob:   BLOB_TYPE,
		Bytea:      BLOB_TYPE,

		Bool: NUMERIC_TYPE,

		Serial:    NUMERIC_TYPE,
		BigSerial: NUMERIC_TYPE,
	}

	intTypes  = sort.StringSlice{"*int", "*int16", "*int32", "*int8"}
	uintTypes = sort.StringSlice{"*uint", "*uint16", "*uint32", "*uint8"}
)

// !nashtsai! treat following var as interal const values, these are used for reflect.TypeOf comparison
var (
	c_EMPTY_STRING       string
	c_BOOL_DEFAULT       bool
	c_BYTE_DEFAULT       byte
	c_COMPLEX64_DEFAULT  complex64
	c_COMPLEX128_DEFAULT complex128
	c_FLOAT32_DEFAULT    float32
	c_FLOAT64_DEFAULT    float64
	c_INT64_DEFAULT      int64
	c_UINT64_DEFAULT     uint64
	c_INT32_DEFAULT      int32
	c_UINT32_DEFAULT     uint32
	c_INT16_DEFAULT      int16
	c_UINT16_DEFAULT     uint16
	c_INT8_DEFAULT       int8
	c_UINT8_DEFAULT      uint8
	c_INT_DEFAULT        int
	c_UINT_DEFAULT       uint
	c_TIME_DEFAULT       time.Time
)

var (
	IntType   = reflect.TypeOf(c_INT_DEFAULT)
	Int8Type  = reflect.TypeOf(c_INT8_DEFAULT)
	Int16Type = reflect.TypeOf(c_INT16_DEFAULT)
	Int32Type = reflect.TypeOf(c_INT32_DEFAULT)
	Int64Type = reflect.TypeOf(c_INT64_DEFAULT)

	UintType   = reflect.TypeOf(c_UINT_DEFAULT)
	Uint8Type  = reflect.TypeOf(c_UINT8_DEFAULT)
	Uint16Type = reflect.TypeOf(c_UINT16_DEFAULT)
	Uint32Type = reflect.TypeOf(c_UINT32_DEFAULT)
	Uint64Type = reflect.TypeOf(c_UINT64_DEFAULT)

	Float32Type = reflect.TypeOf(c_FLOAT32_DEFAULT)
	Float64Type = reflect.TypeOf(c_FLOAT64_DEFAULT)

	Complex64Type  = reflect.TypeOf(c_COMPLEX64_DEFAULT)
	Complex128Type = reflect.TypeOf(c_COMPLEX128_DEFAULT)

	StringType = reflect.TypeOf(c_EMPTY_STRING)
	BoolType   = reflect.TypeOf(c_BOOL_DEFAULT)
	ByteType   = reflect.TypeOf(c_BYTE_DEFAULT)
	BytesType  = reflect.SliceOf(ByteType)

	TimeType = reflect.TypeOf(c_TIME_DEFAULT)
)

var (
	PtrIntType   = reflect.PtrTo(IntType)
	PtrInt8Type  = reflect.PtrTo(Int8Type)
	PtrInt16Type = reflect.PtrTo(Int16Type)
	PtrInt32Type = reflect.PtrTo(Int32Type)
	PtrInt64Type = reflect.PtrTo(Int64Type)

	PtrUintType   = reflect.PtrTo(UintType)
	PtrUint8Type  = reflect.PtrTo(Uint8Type)
	PtrUint16Type = reflect.PtrTo(Uint16Type)
	PtrUint32Type = reflect.PtrTo(Uint32Type)
	PtrUint64Type = reflect.PtrTo(Uint64Type)

	PtrFloat32Type = reflect.PtrTo(Float32Type)
	PtrFloat64Type = reflect.PtrTo(Float64Type)

	PtrComplex64Type  = reflect.PtrTo(Complex64Type)
	PtrComplex128Type = reflect.PtrTo(Complex128Type)

	PtrStringType = reflect.PtrTo(StringType)
	PtrBoolType   = reflect.PtrTo(BoolType)
	PtrByteType   = reflect.PtrTo(ByteType)

	PtrTimeType = reflect.PtrTo(TimeType)
)
