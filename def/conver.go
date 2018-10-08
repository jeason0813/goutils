package def

import (
	"reflect"
	"time"
	"unsafe"
)

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

func ToByte(object interface{}) byte {
	v := To(reflect.ValueOf(object), ByteType)
	return v.Interface().(byte)
}

func ToBytes(object interface{}) []byte {
	v := To(reflect.ValueOf(object), BytesType)
	return v.Interface().([]byte)
}

func ToInt(object interface{}) int {
	v := To(reflect.ValueOf(object), IntType)
	return v.Interface().(int)
}

func ToUint(object interface{}) uint {
	v := To(reflect.ValueOf(object), UintType)
	return v.Interface().(uint)
}

func ToUint8(object interface{}) uint8 {
	v := To(reflect.ValueOf(object), Uint8Type)
	return v.Interface().(uint8)
}

func ToString(object interface{}) string {
	v := To(reflect.ValueOf(object), StringType)
	return v.Interface().(string)
}

func ToBool(object interface{}) bool {
	v := To(reflect.ValueOf(object), BoolType)
	return v.Interface().(bool)
}

func To(value reflect.Value, p reflect.Type) reflect.Value {
	if p != value.Type() {
		return value.Convert(p)
	} else {
		return value
	}
}

func str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func bytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
