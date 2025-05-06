package utils

// https://github.com/yuwf/gobase

import (
	"errors"
	"fmt"
	"reflect"
	"unsafe"
)

// 结构信息
type StructValue struct {
	*StructType

	I      interface{}
	V      reflect.Value
	Elemts []reflect.Value
}

// 根据tag获取结构信息 instance可以是结构也可以是结构地址
func GetStructInfoByTag(instance interface{}, tagName string) (*StructValue, error) {
	if instance == nil {
		err := errors.New("pointer is nil")
		return nil, err
	}

	var structtype reflect.Type
	var structvalue reflect.Value
	vo := reflect.ValueOf(instance)
	if vo.Kind() == reflect.Ptr {
		structtype = vo.Elem().Type() // 第一层是指针，第二层是结构
		structvalue = vo.Elem()
		if structtype.Kind() != reflect.Struct {
			err := fmt.Errorf("not struct or struct pointer, is %s", vo.Type().String())
			return nil, err
		}
	} else if vo.Kind() == reflect.Struct {
		structtype = vo.Type()
		structvalue = vo
	} else {
		err := fmt.Errorf("not struct or struct pointer, is %s", vo.Type().String())
		return nil, err
	}

	st, err := GetStructTypeByTypeTag(structtype, tagName)
	if err != nil {
		return nil, err
	}

	elemts := make([]reflect.Value, 0, len(st.Fields))
	ptr := uintptr(structvalue.UnsafeAddr())
	// 遍历每个字段，基于偏移量提取字段值 使用 unsafe.Pointer 来读取字段值
	for _, field := range st.Fields {
		elemts = append(elemts, reflect.NewAt(field.Type, unsafe.Pointer(ptr+field.Offset)).Elem())
	}

	sInfo := &StructValue{
		StructType: st,
		I:          instance,
		V:          structvalue,
		Elemts:     elemts,
	}
	return sInfo, nil
}

// 根据StructType获取结构信息 instance可以是结构也可以是结构地址
func GetStructInfoByStructType(instance interface{}, st *StructType) (*StructValue, error) {
	if instance == nil || st == nil {
		err := errors.New("pointer is nil")
		return nil, err
	}
	// 获取实例的 reflect.Type 和 reflect.Value
	structvalue := reflect.ValueOf(instance)
	if structvalue.Kind() == reflect.Ptr {
		structvalue = structvalue.Elem() // 如果是指针，获取指针指向的结构体值
	}
	if structvalue.Kind() != reflect.Struct || structvalue.Type() != st.T {
		err := fmt.Errorf("%s not match %s", structvalue.Type().String(), st.T.String())
		return nil, err
	}

	elemts := make([]reflect.Value, 0, len(st.Fields))
	ptr := uintptr(structvalue.UnsafeAddr())
	// 遍历每个字段，基于偏移量提取字段值 使用 unsafe.Pointer 来读取字段值
	for _, field := range st.Fields {
		elemts = append(elemts, reflect.NewAt(field.Type, unsafe.Pointer(ptr+field.Offset)).Elem())
	}

	sInfo := &StructValue{
		StructType: st,
		I:          instance,
		V:          structvalue,
		Elemts:     elemts,
	}
	return sInfo, nil
}

func (s *StructValue) ElemsSlice() []interface{} {
	result := make([]interface{}, 0, len(s.Elemts))
	for _, elemt := range s.Elemts {
		result = append(result, elemt.Interface())
	}
	return result
}

func (s *StructValue) TagElemsMap() map[string]interface{} {
	result := make(map[string]interface{})
	for i, elemt := range s.Elemts {
		result[s.Tags[i]] = elemt.Interface()
	}
	return result
}
