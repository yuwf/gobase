package mysql

// https://github.com/yuwf/gobase

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
)

type JSONValue[T any] struct {
	Data T
}

func (j *JSONValue[T]) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &j.Data)
}

func (j JSONValue[T]) Value() (driver.Value, error) {
	return json.Marshal(j.Data)
}

// 预定义的一些类型，支持mysql读写

// []string
type JsonStrings []string

func (m *JsonStrings) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonStrings) Value() (driver.Value, error) {
	return json.Marshal(&m)
}
func (m JsonStrings) ToInts() []int {
	var rst []int
	for _, d := range m {
		r, err := strconv.Atoi(d)
		if err == nil {
			rst = append(rst, r)
		}
	}
	return rst
}

func (m JsonStrings) ToInt64s() []int64 {
	var rst []int64
	for _, d := range m {
		r, err := strconv.ParseInt(d, 10, 0)
		if err == nil {
			rst = append(rst, r)
		}
	}
	return rst
}

// [][]byte
type JsonSliceBytes [][]byte

func (m *JsonSliceBytes) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonSliceBytes) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// []int32
type JsonInt32s []int32

func (m *JsonInt32s) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonInt32s) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// []int64
type JsonInt64s []int64

func (m *JsonInt64s) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonInt64s) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// []uint64
type JsonUint64s []uint64

func (m *JsonUint64s) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonUint64s) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// []float64
type JsonFloat64s []float64

func (m *JsonFloat64s) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonFloat64s) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[string]string
type JsonStringString map[string]string

func (m *JsonStringString) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonStringString) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[string]int32
type JsonStringInt32 map[string]int32

func (m *JsonStringInt32) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonStringInt32) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[string]uint32
type JsonStringUint32 map[string]uint32

func (m *JsonStringUint32) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonStringUint32) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[string]int64
type JsonStringInt64 map[string]int64

func (m *JsonStringInt64) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonStringInt64) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[int32]string
type JsonInt32String map[int32]string

func (m *JsonInt32String) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonInt32String) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[int32]int32
type JsonInt32Int32 map[int32]int32

func (m *JsonInt32Int32) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonInt32Int32) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[int32]int64
type JsonInt32Int64 map[int32]int64

func (m *JsonInt32Int64) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonInt32Int64) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[int64]string
type JsonInt64String map[int64]string

func (m *JsonInt64String) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonInt64String) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[int64]int32
type JsonInt64Int32 map[int64]int32

func (m *JsonInt64Int32) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonInt64Int32) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[int64]int64
type JsonInt64Int64 map[int64]int64

func (m *JsonInt64Int64) Scan(src any) error {
	if src == nil {
		return nil
	}
	bytes, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", src)
	}
	return json.Unmarshal(bytes, &m)
}
func (m JsonInt64Int64) Value() (driver.Value, error) {
	return json.Marshal(&m)
}
