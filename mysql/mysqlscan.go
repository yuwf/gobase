package mysql

// https://github.com/yuwf/gobase

import (
	"database/sql/driver"
	"encoding/json"

	_ "github.com/go-sql-driver/mysql"
)

// 预定义的一些类型，支持mysql读写

// []string
type JsonStrings []string

func (m *JsonStrings) Scan(src any) error {
	if src == nil {
		return nil
	}
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonStrings) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// [][]byte
type JsonSliceBytes [][]byte

func (m *JsonSliceBytes) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonSliceBytes) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// []int32
type JsonInt32s []int32

func (m *JsonInt32s) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonInt32s) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// []int64
type JsonInt64s []int64

func (m *JsonInt64s) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonInt64s) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// []uint64
type JsonUint64s []uint64

func (m *JsonUint64s) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonUint64s) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// []float64
type JsonFloat64s []float64

func (m *JsonFloat64s) Scan(src any) error {
	return json.Unmarshal(src.([]byte), &m)
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
	return json.Unmarshal(src.([]byte), &m)
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
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonStringInt32) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[int32]string
type JsonInt32String map[int32]string

func (m *JsonInt32String) Scan(src any) error {
	if src == nil {
		return nil
	}
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonInt32String) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[string]int64
type JsonStringInt64 map[string]int64

func (m *JsonStringInt64) Scan(src any) error {
	if src == nil {
		return nil
	}
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonStringInt64) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[int64]string
type JsonInt64String map[int64]string

func (m *JsonInt64String) Scan(src any) error {
	if src == nil {
		return nil
	}
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonInt64String) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[string]uint64
type JsonStringUint64 map[string]uint64

func (m *JsonStringUint64) Scan(src any) error {
	if src == nil {
		return nil
	}
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonStringUint64) Value() (driver.Value, error) {
	return json.Marshal(&m)
}

// map[uint64]string
type JsonUint64String map[uint64]string

func (m *JsonUint64String) Scan(src any) error {
	if src == nil {
		return nil
	}
	return json.Unmarshal(src.([]byte), &m)
}
func (m JsonUint64String) Value() (driver.Value, error) {
	return json.Marshal(&m)
}
