package goredis

// https://github.com/yuwf/gobase

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/yuwf/gobase/utils"
)

// ReplyToValues检查调用
type RedisUnmarshaler interface {
	// reply 是goredis返回的值，类型参考
	RedisUnmarshal(reply any) error
}

// ValueToRedisArg检查调用
type RedisMarshaler interface {
	// 返回的interface{} 必须goredis中(w *Writer) WriteArg(v interface{})支持的类型
	RedisMarshal() (interface{}, error)
}

// json不支持的格式化类型 Complex64，Complex128，Chan，Func，UnsafePointer
// Marshal时会返回一个错误 json: unsupported type: ***
// 所以下面涉及转化的地方也遵循这个规则

// 无法格式化的 返回一个nil
func ValueToRedisArg(v reflect.Value) interface{} {
	if !v.IsValid() || !v.CanInterface() {
		return nil
	}

	// 如果实现了RedisValue接口，调用RedisValue方法
	if vfmt, ok := v.Interface().(RedisMarshaler); ok {
		vfmt, err := vfmt.RedisMarshal()
		if err == nil {
			return vfmt
		}
	} else if v.CanAddr() {
		addr := v.Addr()
		if vfmt, ok := addr.Interface().(RedisMarshaler); ok {
			vfmt, err := vfmt.RedisMarshal()
			if err == nil {
				return vfmt
			}
		}
	}

	switch v.Kind() {
	case reflect.Bool:
		return v.Interface()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Interface()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Interface()
	case reflect.Float32, reflect.Float64:
		return v.Interface()
	case reflect.Complex64:
		return nil
	case reflect.Complex128:
		return nil
	case reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return v.Interface()
		}
		return nil
	case reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return ValueToRedisArg(v.Elem())
	case reflect.Chan:
		return nil
	case reflect.Func:
		return nil
	case reflect.Map:
		if v.IsNil() {
			return nil
		}
		return valueFmtJson(v)
	case reflect.Pointer:
		if v.IsNil() {
			return nil
		}
		return ValueToRedisArg(v.Elem())
	case reflect.Slice:
		if v.IsNil() {
			return nil
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return v.Interface()
		}
		return valueFmtJson(v)
	case reflect.String:
		return v.Interface()
	case reflect.Struct:
		t, ok := v.Interface().(time.Time) // Time类型 存时间戳,毫秒级别
		if ok {
			if t.IsZero() {
				return nil
			}
			return t.UnixMilli()
		}
		return valueFmtJson(v)
	case reflect.UnsafePointer:
		return nil
	}
	return nil
}

func valueFmtJson(v reflect.Value) interface{} {
	data, err := json.Marshal(v.Interface())
	if err == nil {
		return data
	}
	return ""
}

// 过滤掉nil的
func TagElemtNoNilFmt(s *utils.StructValue) []interface{} {
	rst := make([]interface{}, 0, len(s.Tags)*2)
	for i, v := range s.Elemts {
		vfmt := ValueToRedisArg(v)
		if vfmt == nil {
			continue
		}
		rst = append(rst, s.Tags[i])
		rst = append(rst, vfmt)
	}
	return rst
}
