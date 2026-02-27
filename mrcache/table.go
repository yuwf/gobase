package mrcache

// https://github.com/yuwf/gobase

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/utils"
)

// 表结构数据
type TableStruct struct {
	*utils.StructType
	RedisTags []string // 存储Reids使用的tag 大小和Tags一致 在组织Redis参数时使用
}

// 根据tag获取结构信息
func GetTableStruct[T any]() (*TableStruct, error) {
	st, err := utils.GetStructTypeByTag[T](DBTag)
	if err != nil {
		return nil, err
	}

	redisTags := make([]string, 0, len(st.Fields))
	for i, field := range st.Fields {
		redisTag := field.Tag.Get(RedisTag)
		if redisTag != "-" && redisTag != "" {
			sAt := strings.IndexByte(redisTag, ',')
			if sAt != -1 {
				redisTag = redisTag[0:sAt]
			}
		} else {
			redisTag = st.Tags[i]
		}

		redisTags = append(redisTags, redisTag)
	}

	table := &TableStruct{
		StructType: st,
		RedisTags:  redisTags,
	}
	return table, nil
}

func (t *TableStruct) RedisTagsInterface() []interface{} {
	var tagsI []interface{}
	for i := 0; i < len(t.RedisTags); i++ {
		tagsI = append(tagsI, t.RedisTags[i])
	}
	return tagsI
}

func (t *TableStruct) GetRedisTagByTag(tag string) interface{} {
	for i, f := range t.Tags {
		if f == tag {
			return t.RedisTags[i]
		}
	}
	return ""
}

func (t *TableStruct) IsBaseType(tp reflect.Type) bool {
	switch tp.Kind() {
	case reflect.Bool:
		return true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	case reflect.Slice:
		if tp.Elem().Kind() == reflect.Uint8 {
			return true
		}
	case reflect.String:
		return true
	}
	return false
}

func (t *TableStruct) IsNumType(tp reflect.Type) bool {
	switch tp.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}

func (t *TableStruct) fmtBaseType(fieldValue interface{}) string {
	switch val := fieldValue.(type) {
	case bool:
		return strconv.FormatBool(val)
	case int:
		return strconv.Itoa(val)
	case int8:
		return strconv.FormatInt(int64(val), 10)
	case int16:
		return strconv.FormatInt(int64(val), 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint:
		return strconv.FormatUint(uint64(val), 10)
	case uint8:
		return strconv.FormatUint(uint64(val), 10)
	case uint16:
		return strconv.FormatUint(uint64(val), 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case []byte:
		return utils.BytesToString(val)
	case string:
		return val
	}
	return fmt.Sprintf("%v", fieldValue) // 最终的，理论上不会到这里
}

func (t *TableStruct) int64Value(fieldValue interface{}) int64 {
	switch val := fieldValue.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	}
	return 0
}

// 为了使Map有序和更好的接受返回值，该modify相关的函数设定的结构
type ModifyData struct {
	data   map[string]interface{} // 原始值
	tags   []string               // key field 要修改的tag
	values []interface{}          // 写入的值   要修改的值
	rsts   []reflect.Value        // 用于接受返回的值，创建的方式不一，写了多个构造函数
}

func (m *ModifyData) RstsFrom(src *utils.StructValue) {
	for i, v := range src.Elemts {
		if at := utils.IndexOf(m.tags, src.Tags[i]); at != -1 {
			if !v.IsValid() || !v.CanInterface() {
				continue
			}
			goredis.InterfaceToValue(v.Interface(), m.rsts[at])
		}
	}
}

func (m *ModifyData) TagsRstsMap() map[string]interface{} {
	result := make(map[string]interface{})
	for i := 0; i < len(m.rsts); i++ {
		result[m.tags[i]] = m.rsts[i].Interface()
	}
	return result
}
