package utils

// https://github.com/yuwf/gobase

import (
	"fmt"
	"reflect"
	"strings"
)

// 结构信息
type StructType struct {
	T      reflect.Type
	Tags   []string
	Fields []reflect.StructField
}

// 根据tag获取结构信息 i可以是结构也可以是结构地址
func GetStructTypeByTag[T any](tagName string) (*StructType, error) {
	return GetStructTypeByTypeTag(reflect.TypeOf((*T)(nil)).Elem(), tagName)
}

// 根据tag获取结构信息 i可以是结构也可以是结构地址
func GetStructTypeByTypeTag(structtype reflect.Type, tagName string) (*StructType, error) {
	if structtype.Kind() != reflect.Struct {
		err := fmt.Errorf("not struct, is %s", structtype.String())
		return nil, err
	}

	numField := structtype.NumField()
	tags := make([]string, 0, numField)
	fields := make([]reflect.StructField, 0, numField) // 结构中成员的变量地址
	for i := 0; i < numField; i += 1 {
		field := structtype.Field(i)
		if field.PkgPath != "" {
			continue // 非导出字段 反射时会被忽略
		}
		tag := field.Tag.Get(tagName)
		if tag == "-" || tag == "" {
			continue
		}
		sAt := strings.IndexByte(tag, ',')
		if sAt != -1 {
			tag = tag[0:sAt]
		}

		fields = append(fields, field)
		tags = append(tags, tag)
	}

	sInfo := &StructType{
		T:      structtype,
		Tags:   tags,
		Fields: fields,
	}
	return sInfo, nil
}

func (s *StructType) TagsSlice() []interface{} {
	var tagsI []interface{}
	for i := 0; i < len(s.Tags); i++ {
		tagsI = append(tagsI, s.Tags[i])
	}
	return tagsI
}

func (s *StructType) FindIndexByTag(tag string) int {
	for i, f := range s.Tags {
		if f == tag {
			return i
		}
	}
	return -1
}

// 不区分大消息查找
func (s *StructType) FindIndexByTagFold(tag string) int {
	for i, f := range s.Tags {
		if strings.EqualFold(f, tag) {
			return i
		}
	}
	return -1
}

func (s *StructType) InstanceElems(instance interface{}) []reflect.Value {
	// 获取实例的 reflect.Value
	val := reflect.ValueOf(instance)
	if val.Kind() == reflect.Ptr {
		val = val.Elem() // 如果是指针，获取指针指向的结构体值
	}
	if val.Kind() != reflect.Struct || val.Type() != s.T {
		return nil
	}

	result := make([]reflect.Value, 0, len(s.Fields))
	for _, field := range s.Fields {
		result = append(result, val.FieldByIndex(field.Index))
	}
	return result
}
