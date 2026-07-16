package goredis

// https://github.com/yuwf/gobase

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// reply 参考 (r *Reader) ReadReply()， 他支持Resp3协议

const typeErrFmt = "%v(%v) not to %v"

var errNoMatch = fmt.Errorf("type not match")

type WarningError struct {
	Errs []error
}

// reply是reids返回的数组，nil或者空数组返回redis.Nil错误
func ReplyToValues(reply interface{}, values []reflect.Value) error {
	if reply == nil {
		return redis.Nil
	}
	switch r := reply.(type) {
	case []interface{}:
		allNil := true
		for _, r2 := range r {
			if r2 != nil {
				allNil = false
			}
		}
		// 如果全部数据为空，返回空数据错误
		if allNil {
			return redis.Nil
		}
		rlen := len(r)
		elen := len(values)
		if rlen < elen {
			return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, reflect.TypeOf(values))
		}
		rindex := rlen
		for i := elen - 1; i >= 0; i -= 1 {
			rindex -= 1
			if r[rindex] == nil {
				continue
			}
			err := ReplyToValue(r[rindex], values[i])
			if err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, reflect.TypeOf(values))
}

// redi读取的reply到Value的转化
// reply:
// -  reply 为nil时，返回redis.Nil错误
// value:
// -  value对象或者所在的结构体必须是通过地址获取的，否则无法设置值，如果value是空指针地址会自动创建对象

func ReplyToValue(reply interface{}, value reflect.Value) (_err_ error) {
	// 防止崩溃
	defer utils.HandlePanic2(func(r any) {
		// 修改返回值
		_err_ = fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, value)
	})
	warn := &WarningError{}
	err := replyToValue(reply, value, warn)
	if err != nil {
		if err == redis.Nil {
			return err
		}
		return fmt.Errorf("%s(%v) can not to %s : %w", reflect.TypeOf(reply).String(), reply, value.Type().String(), err)
	}
	if len(warn.Errs) > 0 {
		log.Warn().Errs("errs", warn.Errs).Msgf("Error ReplyToValue warning")
	}
	return nil
}

func replyToValue(reply interface{}, value reflect.Value, warn *WarningError) error {
	if reply == nil {
		return redis.Nil
	}
	if !value.IsValid() {
		return errors.New("value is not valid")
	}

	dst := value
	// 如果是 interface{} 类型，直接设置值
	if dst.Kind() == reflect.Interface {
		dst.Set(reflect.ValueOf(reply))
		return nil
	}
	for dst.Kind() == reflect.Pointer || dst.Kind() == reflect.Interface {
		if dst.IsNil() {
			if !dst.CanSet() {
				return fmt.Errorf("%s must be addressable", value.Type().String())
			}
			// 如果是 interface{} 类型，直接设置值
			if dst.Kind() == reflect.Interface {
				dst.Set(reflect.ValueOf(reply))
				return nil
			}
			// dst如果是指针 应该填充指针的地址
			dst.Set(reflect.New(dst.Type().Elem()))
		}
		// 如果实现了RedisScanner接口，调用RedisScan方法
		if dst.CanInterface() {
			if scanner, ok := dst.Interface().(RedisUnmarshaler); ok {
				return scanner.RedisUnmarshal(reply)
			}
		}
		dst = dst.Elem()
	}

	// 如果传入的对象是结构体，尝试调用结构体的RedisScanner接口
	if dst.CanAddr() {
		if scanner, ok := dst.Addr().Interface().(RedisUnmarshaler); ok {
			return scanner.RedisUnmarshal(reply)
		}
	}

	if !dst.CanSet() {
		return fmt.Errorf("%s value must be addressable", value.Type().String())
	}

	switch r := reply.(type) {
	case bool:
		return replyBoolToValue(r, dst)
	case int64:
		return replyInt64ToValue(r, dst)
	case float64:
		return replyFloatToValue(r, dst)
	case string:
		return replyStringToValue(r, dst)
	case *big.Int:
		return replyBigIntToValue(r, dst)
	case []interface{}:
		return replySliceToValue(r, dst, warn)
	case map[interface{}]interface{}:
		return replyMapToValue(r, dst, warn)
	}

	return errNoMatch
}

func replyBoolToValue(b bool, dst reflect.Value) error {
	switch dst.Kind() {
	case reflect.Bool:
		dst.SetBool(b)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dst.SetInt(utils.If[int64](b, 1, 0))
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		dst.SetUint(utils.If[uint64](b, 1, 0))
		return nil
	case reflect.Float32, reflect.Float64:
		dst.SetFloat(utils.If[float64](b, 1, 0))
		return nil
	case reflect.String:
		dst.SetString(strconv.FormatBool(b))
		return nil
	}
	return errNoMatch
}
func replyInt64ToValue(i int64, dst reflect.Value) error {
	switch dst.Kind() {
	case reflect.Bool:
		dst.SetBool(i != 0)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dst.SetInt(i)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		dst.SetUint(uint64(i))
		return nil
	case reflect.Float32, reflect.Float64:
		dst.SetFloat(float64(i))
		return nil
	case reflect.String:
		dst.SetString(strconv.FormatInt(i, 10))
		return nil
	}
	return errNoMatch
}

func replyFloatToValue(f float64, dst reflect.Value) error {
	switch dst.Kind() {
	case reflect.Bool:
		dst.SetBool(utils.FloatEqual(f, 0))
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dst.SetInt(int64(f))
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		dst.SetUint(uint64(f))
		return nil
	case reflect.Float32, reflect.Float64:
		dst.SetFloat(f)
		return nil
	case reflect.String:
		dst.SetString(strconv.FormatFloat(f, 'f', -1, 64))
		return nil
	}
	return errNoMatch
}

func replyStringToValue(s string, dst reflect.Value) error {
	switch dst.Kind() {
	case reflect.Bool:
		if len(s) == 0 {
			dst.SetZero()
			return nil
		}
		r, err := strconv.ParseBool(s)
		if err == nil {
			dst.SetBool(r)
			return nil
		}
		return err
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if len(s) == 0 {
			dst.SetZero()
			return nil
		}
		r, err := strconv.ParseInt(s, 10, 0)
		if err == nil {
			dst.SetInt(r)
			return nil
		}
		return err
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if len(s) == 0 {
			dst.SetZero()
			return nil
		}
		r, err := strconv.ParseUint(s, 10, 0)
		if err == nil {
			dst.SetUint(r)
			return nil
		}
		return err
	case reflect.Float32, reflect.Float64:
		if len(s) == 0 {
			dst.SetZero()
			return nil
		}
		r, err := strconv.ParseFloat(s, 64)
		if err == nil {
			dst.SetFloat(r)
			return nil
		}
		return err
	case reflect.Array:
		// if dst.Type().Elem().Kind() == reflect.Uint8 {
		// 	reflect.Copy(dst, src)
		// 	return true, nil
		// }
	case reflect.Slice:
		if dst.Type().Elem().Kind() == reflect.Uint8 {
			dst.SetBytes([]byte(s)) // 不同类型都要拷贝，用StringToBytes并不是拷贝，外出修改会崩溃
			return nil
		}
	case reflect.String:
		dst.SetString(s)
		return nil
	case reflect.Complex64, reflect.Complex128, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return errNoMatch // json不支持这些类型
	case reflect.Struct:
		if dst.CanAddr() && dst.Addr().CanInterface() {
			t, ok := dst.Addr().Interface().(*time.Time)
			if ok {
				if len(s) == 0 {
					dst.SetZero()
					return nil
				}
				r, err := strconv.ParseInt(s, 10, 0)
				if err == nil {
					l := len(s)
					if l == 10 {
						*t = time.Unix(r, 0)
						return nil
					} else if l == 13 {
						*t = time.UnixMilli(r)
						return nil
					} else if l == 16 {
						*t = time.UnixMicro(r)
						return nil
					} else if l == 19 {
						*t = time.Unix(r/1e9, r%1e9)
						return nil
					} else {
						// 不转化
						return nil
					}
				} else {
					err := t.UnmarshalText(utils.StringToBytes(s))
					if err != nil {
						return fmt.Errorf("time parse error, %s", err.Error())
					} else {
						return nil
					}
				}
			}
		}
	}
	// 其他对象尝试通过json转化 必须指针，否则没有写的必要
	if len(s) > 0 && dst.CanAddr() && dst.Addr().CanInterface() {
		err := json.Unmarshal(utils.StringToBytes(s), dst.Addr().Interface())
		if err == nil {
			return nil
		}
		return err
	}
	return errNoMatch
}

func replyBigIntToValue(r *big.Int, dst reflect.Value) error {
	switch dst.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dst.SetInt(r.Int64())
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		dst.SetUint(r.Uint64())
		return nil
	case reflect.Float32, reflect.Float64:
		f, _ := r.Float64()
		dst.SetFloat(f)
		return nil
	case reflect.String:
		dst.SetString(r.String())
		return nil
	}
	return errNoMatch
}

func replySliceToValue(s []interface{}, dst reflect.Value, warn *WarningError) error {
	src := reflect.ValueOf(s)
	switch dst.Kind() {
	case reflect.Bool:
		if src.Type().Elem().Kind() == reflect.Uint8 {
			if len(src.Bytes()) == 0 {
				dst.SetZero()
				return nil
			}
			r, err := strconv.ParseBool(utils.BytesToString(src.Bytes()))
			if err == nil {
				dst.SetBool(r)
				return nil
			}
			return err
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if src.Type().Elem().Kind() == reflect.Uint8 {
			if len(src.Bytes()) == 0 {
				dst.SetZero()
				return nil
			}
			r, err := strconv.ParseInt(utils.BytesToString(src.Bytes()), 10, 0)
			if err == nil {
				dst.SetInt(r)
				return nil
			}
			return err
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if src.Type().Elem().Kind() == reflect.Uint8 {
			if len(src.Bytes()) == 0 {
				dst.SetZero()
				return nil
			}
			r, err := strconv.ParseUint(utils.BytesToString(src.Bytes()), 10, 0)
			if err == nil {
				dst.SetUint(r)
				return nil
			}
			return err
		}
	case reflect.Float32, reflect.Float64:
		if src.Type().Elem().Kind() == reflect.Uint8 {
			if len(src.Bytes()) == 0 {
				dst.SetZero()
				return nil
			}
			r, err := strconv.ParseFloat(utils.BytesToString(src.Bytes()), 64)
			if err == nil {
				dst.SetFloat(r)
				return nil
			}
			return err
		}
	case reflect.Array:
		if dst.Type().Elem() == src.Type().Elem() {
			reflect.Copy(dst, src)
			return nil
		}
	case reflect.Map:
		newMap := reflect.MakeMap(dst.Type())
		// 先保留原数据
		for _, key := range dst.MapKeys() {
			val := dst.MapIndex(key)
			newMap.SetMapIndex(key, val)
		}
		keytype := dst.Type().Key()
		elemtype := dst.Type().Elem()
		for i := 0; i+1 < len(s); i += 2 {
			// key解析错了，跳过
			key := reflect.New(keytype).Elem()
			if err := replyToValue(s[i], key, warn); err != nil {
				if err != redis.Nil {
					warn.Errs = append(warn.Errs, err)
				}
				continue
			}
			// value解析错了，填充零值
			value := reflect.New(elemtype).Elem()
			if err := replyToValue(s[i+1], value, warn); err != nil {
				if err != redis.Nil {
					warn.Errs = append(warn.Errs, err)
				}
				value = reflect.Zero(elemtype)
			}
			newMap.SetMapIndex(key, value)
		}
		dst.Set(newMap)
		return nil
	case reflect.Slice:
		newSlice := reflect.MakeSlice(dst.Type(), 0, dst.Len()+src.Len())
		// 先保留原数据
		for i := 0; i < dst.Len(); i++ {
			newSlice = reflect.Append(newSlice, dst.Index(i))
		}
		elemtype := dst.Type().Elem()
		for i := 0; i < len(s); i++ {
			elem := reflect.New(elemtype).Elem()
			if err := replyToValue(s[i], elem, warn); err != nil {
				if err != redis.Nil {
					warn.Errs = append(warn.Errs, err)
				}
				newSlice = reflect.Append(newSlice, reflect.Zero(elemtype))
			} else {
				newSlice = reflect.Append(newSlice, elem)
			}
		}
		dst.Set(newSlice)
		return nil
	case reflect.String:
		if src.Type().Elem().Kind() == reflect.Uint8 {
			dst.SetString(utils.BytesToString(src.Bytes()))
			return nil
		}
	case reflect.Complex64, reflect.Complex128, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return nil // json不支持这些类型
	}
	// 如果是[]byte 尝试通过json转化
	if src.Type().Elem().Kind() == reflect.Uint8 && len(src.Bytes()) > 0 && dst.CanAddr() && dst.Addr().CanInterface() {
		err := json.Unmarshal(src.Bytes(), dst.Addr().Interface())
		if err == nil {
			return nil
		}
		return err
	}
	return errNoMatch
}

func replyMapToValue(m map[interface{}]interface{}, dst reflect.Value, warn *WarningError) error {
	switch dst.Kind() {
	case reflect.Map:
		newMap := reflect.MakeMap(dst.Type())
		// 先保留原数据
		for _, key := range dst.MapKeys() {
			val := dst.MapIndex(key)
			newMap.SetMapIndex(key, val)
		}
		keytype := dst.Type().Key()
		elemtype := dst.Type().Elem()
		for k, v := range m {
			// key解析错了，跳过
			key := reflect.New(keytype).Elem()
			if err := replyToValue(k, key, warn); err != nil {
				if err != redis.Nil {
					warn.Errs = append(warn.Errs, err)
				}
				continue
			}
			// value解析错了，填充零值
			value := reflect.New(elemtype).Elem()
			if err := replyToValue(v, value, warn); err != nil {
				if err != redis.Nil {
					warn.Errs = append(warn.Errs, err)
				}
				value = reflect.Zero(elemtype)
			}
			newMap.SetMapIndex(key, value)
		}
		dst.Set(newMap)
		return nil
	case reflect.Slice:
		newSlice := reflect.MakeSlice(dst.Type(), 0, dst.Len()+len(m)*2)
		// 先保留原数据
		for i := 0; i < dst.Len(); i++ {
			newSlice = reflect.Append(newSlice, dst.Index(i))
		}
		elemtype := dst.Type().Elem()
		for k, v := range m {
			elem := reflect.New(elemtype).Elem()
			if err := replyToValue(k, elem, warn); err != nil {
				if err != redis.Nil {
					warn.Errs = append(warn.Errs, err)
				}
				newSlice = reflect.Append(newSlice, reflect.Zero(elemtype))
			} else {
				newSlice = reflect.Append(newSlice, elem)
			}
			elem = reflect.New(elemtype).Elem()
			if err := replyToValue(v, elem, warn); err != nil {
				if err != redis.Nil {
					warn.Errs = append(warn.Errs, err)
				}
				newSlice = reflect.Append(newSlice, reflect.Zero(elemtype))
			} else {
				newSlice = reflect.Append(newSlice, elem)
			}
		}
		dst.Set(newSlice)
		return nil
	}
	return errNoMatch
}
