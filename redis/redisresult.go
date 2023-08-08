package redis

// https://github.com/yuwf/gobase

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gobase/utils"

	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog/log"
)

const typeErrFmt = "%v(%v) not to %v"

// Redis结果绑定，用这种方式认为不需要太关心Redis执行结果真的正确与否
type RedisResultBind interface {
	// BindKind : Bool, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr, Float32, Float64, String, []byte
	// 传入的参数为对象的地址
	Bind(v interface{}) error

	// BindKind : Slice
	// SliceElemKind : Bool, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr, Float32, Float64, String
	// 传入的参数为Slice的地址 nil的Slice也可以
	BindSlice(v interface{}) error

	// BindType : Map
	// MapElemType : Bool, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr, Float32, Float64, String
	// 传入的参数为Map的地址 nil的Map也可以
	BindMap(v interface{}) error

	// 针对HMGET命令 调用Cmd时，参数不需要包括field
	// 结构成员首字母需要大写，tag中必须是包含 `redis:"hello"`  其中hello就表示在redis中存储的field名称
	// 结构成员类型 : Bool, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr, Float32, Float64, String, []byte
	// 传入的参数为结构的地址
	HMGetBindObj(v interface{}) error
	// 组织参数使用，和HMGetBindObj对称
	_HMSetObjArgs(v interface{}) error

	// BindType : Struct
	// 结构支持json格式化
	// 传入的参数为结构的地址
	BindJsonObj(v interface{}) error

	// BindType : []Struct 或者 []*Struct
	// SliceElemType 支持json格式化
	// 传入的参数为Slice的地址 nil的Slice也可以
	BindJsonObjSlice(v interface{}) error
}

// Redis命令封装 并实现RedisResultBind接口
// 注意Redis执行结果为空时(ErrNil)，并不认为是错误
type RedisCommond struct {
	// 命令名和参数
	Cmd     string
	Args    []interface{}
	CmdDesc string // 命令的描述

	Caller *utils.CallerDesc

	// 绑定回调
	callback func(reply interface{}) error

	// 执行结果 Reids直接调用命令使用，如果Reply和Err说明命令没有执行
	Reply   interface{}
	Err     error
	Elapsed time.Duration
}

func (c *RedisCommond) CmdString() string {
	var str strings.Builder
	str.WriteString(c.Cmd)
	if len(c.CmdDesc) > 0 {
		str.WriteString("(" + c.CmdDesc + ")")
	}
	for i, arg := range c.Args {
		if i == 8 {
			s := fmt.Sprintf(" (%d)...", len(c.Args)-i)
			str.WriteString(s)
			break
		}
		str.WriteString(" ")
		switch v := arg.(type) {
		case string:
			if len(v) > 32 && str.Len() > 256 {
				str.WriteString(v[0:32] + "...")
			} else if len(v) > 128 {
				str.WriteString(v[0:128] + "...")
			} else {
				str.WriteString(v)
			}
		case []byte:
			if len(v) > 32 && str.Len() > 256 {
				str.WriteString(string(v[0:32]) + "...")
			} else if len(v) > 128 {
				str.WriteString(string(v[0:128]) + "...")
			} else {
				str.WriteString(string(v))
			}
		default:
			s := fmt.Sprint(v)
			if len(s) > 32 && str.Len() > 256 {
				str.WriteString(s[0:32] + "...")
			} else if len(s) > 128 {
				str.WriteString(s[0:128] + "...")
			} else {
				str.WriteString(s)
			}
		}
	}
	return str.String()
}

func (c *RedisCommond) ReplyString() string {
	if c.Reply == nil {
		return ""
	}
	var str strings.Builder
	fmtInterface(c.Reply, &str)
	return str.String()
}

func fmtInterface(r interface{}, str *strings.Builder) {
	if str.Len() > 0 {
		str.WriteString(" ")
	}
	switch v := r.(type) {
	case string:
		if len(v) > 32 && str.Len() > 256 {
			str.WriteString(v[0:32] + "...")
		} else if len(v) > 128 {
			str.WriteString(v[0:128] + "...")
		} else {
			str.WriteString(v)
		}
	case []byte:
		if len(v) > 32 && str.Len() > 256 {
			str.WriteString(string(v[0:32]) + "...")
		} else if len(v) > 128 {
			str.WriteString(string(v[0:128]) + "...")
		} else {
			str.WriteString(string(v))
		}
	case []interface{}:
		for i, r2 := range v {
			if i == 8 {
				s := fmt.Sprintf(" (%d)...", len(v)-i)
				str.WriteString(s)
				break
			}
			fmtInterface(r2, str)
		}
	default:
		s := fmt.Sprint(v)
		if len(s) > 32 && str.Len() > 256 {
			str.WriteString(s[0:32] + "...")
		} else if len(s) > 128 {
			str.WriteString(s[0:128] + "...")
		} else {
			str.WriteString(s)
		}
	}
}

func (c *RedisCommond) Bind(v interface{}) error {
	// 参数检查
	vo := reflect.ValueOf(v)
	if vo.Kind() != reflect.Ptr {
		err := errors.New("Bind Param Kind must be Pointer")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
		return err
	}
	if vo.IsNil() {
		err := errors.New("Bind Pointer is nil")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
		return err
	}
	elem := vo.Elem()
	if !elem.CanSet() {
		err := errors.New("Param Kind must be CanSet")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
		return err
	}
	// 初始化默认值
	switch elem.Kind() {
	case reflect.Bool:
		elem.SetBool(false)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		elem.SetInt(0)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		elem.SetUint(0)
	case reflect.Float32, reflect.Float64:
		elem.SetFloat(0)
	case reflect.String:
		elem.SetString("")
	case reflect.Slice:
		if elem.Type().Elem().Kind() != reflect.Uint8 {
			err := errors.New("SliceElem Kind must be Uint8(byte)")
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
			return err
		}
		elem.SetBytes([]byte{})
	default:
		err := errors.New("Param not support " + fmt.Sprint(elem.Type()))
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
		return err
	}
	// 绑定回到函数
	c.callback = func(reply interface{}) error {
		switch r := reply.(type) {
		case int64:
			return int64Helper(r, elem)
		case string:
			return stringHelper(r, elem)
		case []byte:
			return bytesHelper(r, elem)
		case []interface{}:
			return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, elem.Type())
		case nil:
			// 空值 也认为ok 绑定的值上面已设置为空了
			return nil
		case redis.Error:
			return r
		}
		return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, elem.Type())
	}
	// 直接调用的Redis 此时已经有结果值了
	if c.Reply != nil || c.Err != nil {
		if c.Err != nil {
			return c.Err
		}
		err := c.callback(c.Reply)
		if err != nil {
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
			return err
		}
	}
	return nil
}

func (c *RedisCommond) BindSlice(v interface{}) error {
	// 参数检查
	vt := reflect.TypeOf(v)
	if vt.Kind() != reflect.Ptr {
		err := errors.New("Bind Param Kind must be Slice Pointer")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindSlice fail")
		return err
	}
	if vt.Elem().Kind() != reflect.Slice {
		err := errors.New("Bind Param Kind must be Slice Pointer")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindSlice fail")
		return err
	}
	elemtype := vt.Elem().Elem() // 第一层是slice，第二层是slice中的元素
	vo := reflect.ValueOf(v)
	sli := vo.Elem() // 第一层是slice的地址 第二层是slice sli是v的一个拷贝
	if sli.IsNil() {
		sli = reflect.MakeSlice(vt.Elem(), 0, 0)
		ind := reflect.Indirect(vo)
		ind.Set(sli)
	}
	// 绑定回调函数
	c.callback = func(reply interface{}) error {
		// 因为slice的地址在追加时一直变化最后给v重新赋值
		defer func() {
			ind := reflect.Indirect(vo)
			ind.Set(sli)
		}()

		switch r := reply.(type) {
		case int64:
			v := reflect.New(elemtype).Elem()
			err := int64Helper(r, v)
			if err != nil {
				return err
			}
			sli = reflect.Append(sli, v)
		case string:
			v := reflect.New(elemtype).Elem()
			err := stringHelper(r, v)
			if err != nil {
				return err
			}
			sli = reflect.Append(sli, v)
		case []byte:
			v := reflect.New(elemtype).Elem()
			err := bytesHelper(r, v)
			if err != nil {
				return err
			}
			sli = reflect.Append(sli, v)
		case []interface{}:
			for i := range r {
				v := reflect.New(elemtype).Elem()
				err := interfaceHelper(r[i], v)
				if err != nil {
					// 只打印一个错误日志，不返回，后面继续往sli里面写入
					log.Error().Err(err).Str("pos", c.Caller.Pos()).Msg("SliceElem Bind Error")
				}
				sli = reflect.Append(sli, v)
			}
			return nil
		case nil:
			// 空值
			return nil
		case redis.Error:
			return r
		}
		return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, sli.Type())
	}
	// 直接调用的Redis 此时已经有结果值了
	if c.Reply != nil || c.Err != nil {
		if c.Err != nil {
			return c.Err
		}
		err := c.callback(c.Reply)
		if err != nil {
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindSlice fail")
			return err
		}
	}
	return nil
}

func (c *RedisCommond) BindMap(v interface{}) error {
	// 参数检查
	vt := reflect.TypeOf(v)
	if vt.Kind() != reflect.Ptr {
		err := errors.New("Bind Param Kind must be Map Pointer")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindMap fail")
		return err
	}
	if vt.Elem().Kind() != reflect.Map {
		err := errors.New("Bind Param Kind must be Map Pointer")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindMap fail")
		return err
	}
	keytype := vt.Elem().Key()
	elemtype := vt.Elem().Elem()
	vo := reflect.ValueOf(v)
	m := vo.Elem() // 第一层是map的地址 第二层是map
	if m.IsNil() {
		m = reflect.MakeMap(vt.Elem())
		ind := reflect.Indirect(vo)
		ind.Set(m)
	}
	// 绑定回调函数
	c.callback = func(reply interface{}) error {
		switch r := reply.(type) {
		case int64:
		case string:
		case []byte:
		case []interface{}:
			for i := 0; i+1 < len(r); i += 2 {
				key := reflect.New(keytype).Elem()
				okKey := interfaceHelper(r[i], key)
				if okKey != nil {
					// 只打印一个错误日志，不返回
					log.Error().Err(okKey).Str("pos", c.Caller.Pos()).Msg("MapKey Bind Error")
					continue
				}
				value := reflect.New(elemtype).Elem()
				okValue := interfaceHelper(r[i+1], value)
				if okValue != nil {
					// 只打印一个错误日志，不返回，后面继续往m里面写入
					log.Error().Err(okValue).Str("pos", c.Caller.Pos()).Msg("MapElem Bind Error")
				}
				m.SetMapIndex(key, value)
			}
			return nil
		case nil:
			// 空值
			return nil
		case redis.Error:
			return r
		}
		return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, m.Type())
	}
	// 直接调用的Redis 此时已经有结果值了
	if c.Reply != nil || c.Err != nil {
		if c.Err != nil {
			return c.Err
		}
		err := c.callback(c.Reply)
		if err != nil {
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindMap fail")
			return err
		}
	}
	return nil
}

func (c *RedisCommond) HMGetBindObj(v interface{}) error {
	// 参数检查
	vo := reflect.ValueOf(v)
	if vo.Kind() != reflect.Ptr {
		err := errors.New("Bind Param Kind must be Pointer")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult HMGetBindObj fail")
		return err
	}
	if vo.IsNil() {
		err := errors.New("Bind Param Pointer is nil")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult HMGetBindObj fail")
		return err
	}
	structtype := vo.Elem().Type() // 第一层是指针，第二层是结构
	structvalue := vo.Elem()
	if structtype.Kind() != reflect.Struct {
		err := errors.New("Bind Param Kind must be Struct")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult HMGetBindObj fail")
		return err
	}
	numfield := structtype.NumField()
	elemts := []reflect.Value{} // 结构中成员的变量地址
	for i := 0; i < numfield; i += 1 {
		tag := structtype.Field(i).Tag.Get("redis")
		if tag == "-" || tag == "" {
			continue
		}
		v := structvalue.Field(i)
		if v.CanSet() {
			c.Args = append(c.Args, tag)
			elemts = append(elemts, v)
		}
	}
	if len(elemts) == 0 {
		err := errors.New("StructMem invalid")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult HMGetBindObj fail")
		return err
	}

	c.callback = func(reply interface{}) error {
		switch r := reply.(type) {
		case int64:
			return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, structtype)
		case string:
			return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, structtype)
		case []byte:
			return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, structtype)
		case []interface{}:
			rlen := len(r)
			elen := len(elemts)
			if rlen < elen {
				return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, structtype)
			}
			rindex := rlen
			for i := elen - 1; i >= 0; i -= 1 {
				rindex -= 1
				err := interfaceHelper(r[rindex], elemts[i])
				if err != nil {
					//只打印一个错误日志，不返回
					log.Error().Err(err).Str("pos", c.Caller.Pos()).Msg("StructElem Bind Error")
				}
			}
			return nil
		case nil:
			// 空值
			return nil
		case redis.Error:
			return r
		}
		return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, structtype)
	}
	// 直接调用的Redis 此时已经有结果值了
	if c.Reply != nil || c.Err != nil {
		if c.Err != nil {
			return c.Err
		}
		err := c.callback(c.Reply)
		if err != nil {
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult HMGetBindObj fail")
			return err
		}
	}
	return nil
}

func (c *RedisCommond) _HMSetObjArgs(v interface{}) error {
	// 验证参数
	vo := reflect.ValueOf(v)
	if vo.Kind() != reflect.Ptr {
		return errors.New("Param Kind must be Pointer")
	}
	if vo.IsNil() {
		return errors.New("Param Pointer is nil")
	}
	structtype := vo.Elem().Type() // 第一层是指针，第二层是结构
	structvalue := vo.Elem()
	if structtype.Kind() != reflect.Struct {
		return errors.New("Param Kind must be Struct")
	}

	argsNum := len(c.Args) // 先记录下进来时的参数个数
	// 组织参数
	numfield := structtype.NumField()
	for i := 0; i < numfield; i += 1 {
		tag := structtype.Field(i).Tag.Get("redis")
		if tag == "-" || tag == "" {
			continue
		}
		v := structvalue.Field(i)
		if !v.CanAddr() {
			continue
		}
		switch v.Kind() {
		case reflect.Bool:
			fallthrough
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fallthrough
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			fallthrough
		case reflect.Float32, reflect.Float64:
			fallthrough
		case reflect.String:
			c.Args = append(c.Args, tag)
			c.Args = append(c.Args, v.Interface())
		case reflect.Slice:
			if v.Type().Elem().Kind() != reflect.Uint8 {
				log.Error().Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult _HMSetObjArgs SliceElem Kind must be Uint8(byte)")
				return errors.New("SliceElem Kind must be Uint8(byte)")
			}
			c.Args = append(c.Args, tag)
			c.Args = append(c.Args, v.Interface())
		default:
			return errors.New("Param not support " + fmt.Sprint(v.Type()))
		}
	}
	if len(c.Args) == argsNum {
		return errors.New("StructMem invalid")
	}
	return nil
}

func (c *RedisCommond) BindJsonObj(v interface{}) error {
	// 参数检查
	vo := reflect.ValueOf(v)
	if vo.Kind() != reflect.Ptr {
		err := errors.New("Bind Param Kind must be Pointer")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindJsonObj fail")
		return err
	}
	if vo.IsNil() {
		err := errors.New("Bind Param Pointer is nil")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindJsonObj fail")
		return err
	}
	structtype := vo.Elem().Type() // 第一层是指针，第二层是结构
	if structtype.Kind() != reflect.Struct {
		err := errors.New("Bind Param Kind must be Struct")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindJsonObj fail")
		return err
	}

	c.callback = func(reply interface{}) error {
		switch r := reply.(type) {
		case int64:
			return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, structtype)
		case string:
			return json.Unmarshal([]byte(r), v)
		case []byte:
			return json.Unmarshal(r, v)
		case []interface{}:
			return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, structtype)
		case nil:
			// 空值
			return nil
		case redis.Error:
			return r
		}
		return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, structtype)
	}
	// 直接调用的Redis 此时已经有结果值了
	if c.Reply != nil || c.Err != nil {
		if c.Err != nil {
			return c.Err
		}
		err := c.callback(c.Reply)
		if err != nil {
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindJsonObj fail")
			return err
		}
	}
	return nil
}

func (c *RedisCommond) BindJsonObjSlice(v interface{}) error {
	// 参数检查
	vt := reflect.TypeOf(v)
	if vt.Kind() != reflect.Ptr {
		err := errors.New("Bind Param Kind must be Pointer")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindJsonObjSlice fail")
		return err
	}
	if vt.Elem().Kind() != reflect.Slice {
		err := errors.New("Bind Param Kind must be Slice")
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindJsonObjSlice fail")
		return err
	}
	elemtype := vt.Elem().Elem() // 第一层是slice，第二层是slice中的元素
	if elemtype.Kind() == reflect.Pointer {
		// 元素是指针，指针指向的类型必须是结构
		if elemtype.Elem().Kind() != reflect.Struct {
			err := errors.New("Bind Param Elem Kind must be Struct or *Struct")
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisPipeline BindJsonObjSlice Bind Param Kind must be Struct or *Struct")
			return err
		}
	} else if elemtype.Kind() != reflect.Struct {
		err := errors.New("Bind Param Elem Kind must be Struct or *Struct")
		log.Error().Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindJsonObjSlice fail")
		return err
	}

	vo := reflect.ValueOf(v)
	sli := vo.Elem() // 第一层是slice的地址 第二层是slice sli是v的一个拷贝
	if sli.IsNil() {
		sli = reflect.MakeSlice(vt.Elem(), 0, 0)
		ind := reflect.Indirect(vo)
		ind.Set(sli)
	}

	// 绑定回调函数
	c.callback = func(reply interface{}) error {
		// 因为slice的地址在追加时一直变化最后给v重新赋值
		defer func() {
			ind := reflect.Indirect(vo)
			ind.Set(sli)
		}()

		switch r := reply.(type) {
		case int64:
			return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, sli.Type())
		case string:
			return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, sli.Type())
		case []byte:
			return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, sli.Type())
		case []interface{}:
			for i := range r {
				var v reflect.Value
				if elemtype.Kind() == reflect.Pointer {
					v = reflect.New(elemtype.Elem())
				} else {
					v = reflect.New(elemtype).Elem()
				}

				switch r2 := r[i].(type) {
				case int64:
				case string:
					if elemtype.Kind() == reflect.Pointer {
						json.Unmarshal([]byte(r2), v.Interface())
					} else {
						json.Unmarshal([]byte(r2), v.Addr().Interface())
					}
					sli = reflect.Append(sli, v)
				case []byte:
					if elemtype.Kind() == reflect.Pointer {
						json.Unmarshal(r2, v.Interface())
					} else {
						json.Unmarshal(r2, v.Addr().Interface())
					}
					sli = reflect.Append(sli, v)
				case []interface{}:
				case nil:
				case redis.Error:
				}
			}
			return nil
		case nil:
			// 空值
			return nil
		case redis.Error:
			return r
		}
		return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, sli.Type())
	}
	// 直接调用的Redis 此时已经有结果值了
	if c.Reply != nil || c.Err != nil {
		if c.Err != nil {
			return c.Err
		}
		err := c.callback(c.Reply)
		if err != nil {
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindJsonObjSlice fail")
			return err
		}
	}
	return nil
}

func int64Helper(r int64, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(r != 0)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(r)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		v.SetUint(uint64(r))
	case reflect.Float32, reflect.Float64:
		v.SetFloat(float64(r))
	case reflect.String:
		v.SetString(strconv.FormatInt(r, 10))
	default:
		return fmt.Errorf(typeErrFmt, reflect.TypeOf(r), r, v.Type())
	}
	return nil
}

func stringHelper(r string, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Bool:
		b, err := strconv.ParseBool(r)
		if err != nil {
			return errors.New(fmt.Sprintf(typeErrFmt, reflect.TypeOf(r), r, v.Type()) + " Parse:" + err.Error())
		}
		v.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(r, 10, 0)
		if err != nil {
			return errors.New(fmt.Sprintf(typeErrFmt, reflect.TypeOf(r), r, v.Type()) + " Parse:" + err.Error())
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n, err := strconv.ParseUint(r, 10, 0)
		if err != nil {
			return errors.New("Parse:" + err.Error())
		}
		v.SetUint(n)
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(r, 0)
		if err != nil {
			return errors.New(fmt.Sprintf(typeErrFmt, reflect.TypeOf(r), r, v.Type()) + " Parse:" + err.Error())
		}
		v.SetFloat(n)
	case reflect.String:
		v.SetString(r)
	case reflect.Slice: // 用[]byte接受返回值
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf(typeErrFmt, reflect.TypeOf(r), r, v.Type())
		}
		v.SetBytes([]byte(r))
	default:
		return fmt.Errorf(typeErrFmt, reflect.TypeOf(r), r, v.Type())
	}
	return nil
}

func bytesHelper(r []byte, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Bool:
		b, err := strconv.ParseBool(string(r))
		if err != nil {
			return errors.New(fmt.Sprintf(typeErrFmt, reflect.TypeOf(r), r, v.Type()) + " Parse:" + err.Error())
		}
		v.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(string(r), 10, 0)
		if err != nil {
			return errors.New(fmt.Sprintf(typeErrFmt, reflect.TypeOf(r), r, v.Type()) + " Parse:" + err.Error())
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n, err := strconv.ParseUint(string(r), 10, 0)
		if err != nil {
			return errors.New("Parse:" + err.Error())
		}
		v.SetUint(n)
	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(string(r), 0)
		if err != nil {
			return errors.New(fmt.Sprintf(typeErrFmt, reflect.TypeOf(r), r, v.Type()) + " Parse:" + err.Error())
		}
		v.SetFloat(n)
	case reflect.String:
		v.SetString(string(r))
	case reflect.Slice: // 用[]byte接受返回值
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf(typeErrFmt, reflect.TypeOf(r), r, v.Type())
		}
		v.SetBytes(r)
	default:
		return fmt.Errorf(typeErrFmt, reflect.TypeOf(r), r, v.Type())
	}
	return nil
}

func interfaceHelper(r interface{}, v reflect.Value) error {
	switch r2 := r.(type) {
	case int64:
		return int64Helper(r2, v)
	case string:
		return stringHelper(r2, v)
	case []byte:
		return bytesHelper(r2, v)
	case []interface{}:
		// 两层 类似SCAN这么的命令 待完善
	case nil:
		return nil
	case redis.Error:
		return r2
	}
	return fmt.Errorf(typeErrFmt, reflect.TypeOf(r), r, v.Type())
}
