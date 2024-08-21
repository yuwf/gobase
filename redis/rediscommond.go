package redis

// https://github.com/yuwf/gobase

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/context"
)

const typeErrFmt = "%v(%v) not to %v"

var RedisTag = "redis"

// Redis命令封装
// 注意Redis执行结果为空时(ErrNil)，并不认为是错误
type RedisCommond struct {
	// 命令名和参数
	ctx     context.Context
	Cmd     string
	Args    []interface{}
	CmdDesc string // 命令的描述

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
		err := errors.New("bind param kind must be pointer")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond Bind fail")
		return err
	}
	if vo.IsNil() {
		err := errors.New("bind param pointer is nil")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond Bind fail")
		return err
	}
	elem := vo.Elem()
	if !elem.CanSet() {
		err := errors.New("bind param kind must be canset")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond Bind fail")
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
			err := errors.New("sliceelem kind must be Uint8(byte)")
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond Bind fail")
			return err
		}
		elem.SetBytes([]byte{})
	default:
		err := errors.New("param not support " + fmt.Sprint(elem.Type()))
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond Bind fail")
		return err
	}
	// 绑定回到函数
	c.callback = func(reply interface{}) error {
		if reply == nil {
			return nil
		}
		return utils.InterfaceToValue(reply, elem)
	}
	// 直接调用的Redis 此时已经有结果值了
	if c.Reply != nil || c.Err != nil {
		if c.Err != nil {
			return c.Err
		}
		err := c.callback(c.Reply)
		if err != nil {
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond Bind fail")
			return err
		}
	}
	return nil
}

func (c *RedisCommond) BindSlice(v interface{}) error {
	// 参数检查
	vt := reflect.TypeOf(v)
	if vt.Kind() != reflect.Ptr {
		err := errors.New("bind param kind must be slice Pointer")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindSlice fail")
		return err
	}
	if vt.Elem().Kind() != reflect.Slice {
		err := errors.New("bind param kind must be slice Pointer")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindSlice fail")
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
		if reply == nil {
			return nil
		}
		// 因为slice的地址在追加时一直变化最后给v重新赋值
		defer func() {
			ind := reflect.Indirect(vo)
			ind.Set(sli)
		}()

		switch r := reply.(type) {
		case int64:
			v := reflect.New(elemtype).Elem()
			err := utils.InterfaceToValue(r, v)
			if err != nil {
				return err
			}
			sli = reflect.Append(sli, v)
		case string:
			v := reflect.New(elemtype).Elem()
			err := utils.InterfaceToValue(r, v)
			if err != nil {
				return err
			}
			sli = reflect.Append(sli, v)
		case []byte:
			v := reflect.New(elemtype).Elem()
			err := utils.InterfaceToValue(r, v)
			if err != nil {
				return err
			}
			sli = reflect.Append(sli, v)
		case []interface{}:
			for i := range r {
				v := reflect.New(elemtype).Elem()
				err := utils.InterfaceToValue(r[i], v)
				if err != nil {
					return err
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
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindSlice fail")
			return err
		}
	}
	return nil
}

func (c *RedisCommond) BindMap(v interface{}) error {
	// 参数检查
	vt := reflect.TypeOf(v)
	if vt.Kind() != reflect.Ptr {
		err := errors.New("bind param kind must be map pointer")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindMap fail")
		return err
	}
	if vt.Elem().Kind() != reflect.Map {
		err := errors.New("bind param kind must be map pointer")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindMap fail")
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
				if r[i] == nil {
					continue
				}
				key := reflect.New(keytype).Elem()
				err := utils.InterfaceToValue(r[i], key)
				if err != nil {
					return err
				}
				value := reflect.New(elemtype).Elem()
				if r[i+1] == nil {
					err = utils.InterfaceToValue(r[i+1], value)
					if err != nil {
						return err
					}
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
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindMap fail")
			return err
		}
	}
	return nil
}

func (c *RedisCommond) BindValues(values []reflect.Value) error {
	c.callback = func(reply interface{}) error {
		switch r := reply.(type) {
		case int64:
		case string:
		case []byte:
		case []interface{}:
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
				err := utils.InterfaceToValue(r[rindex], values[i])
				if err != nil {
					return err
				}
			}
			return nil
		case nil:
			// 空值
			return nil
		case redis.Error:
			return r
		}
		return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, reflect.TypeOf(values))
	}
	// 直接调用的Redis 此时已经有结果值了
	if c.Reply != nil || c.Err != nil {
		if c.Err != nil {
			return c.Err
		}
		err := c.callback(c.Reply)
		if err != nil {
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond HMGetObj fail")
			return err
		}
	}
	return nil
}

func (c *RedisCommond) BindJsonObj(v interface{}) error {
	// 参数检查
	vo := reflect.ValueOf(v)
	if vo.Kind() != reflect.Ptr {
		err := errors.New("bind param kind must be pointer")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObj fail")
		return err
	}
	if vo.IsNil() {
		err := errors.New("bind param pointer is nil")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObj fail")
		return err
	}
	structtype := vo.Elem().Type() // 第一层是指针，第二层是结构
	if structtype.Kind() != reflect.Struct {
		err := errors.New("bind param kind must be struct")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObj fail")
		return err
	}

	c.callback = func(reply interface{}) error {
		switch r := reply.(type) {
		case int64:
		case string:
			return json.Unmarshal(utils.StringToBytes(r), v)
		case []byte:
			return json.Unmarshal(r, v)
		case []interface{}:
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
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObj fail")
			return err
		}
	}
	return nil
}

func (c *RedisCommond) BindJsonObjSlice(v interface{}) error {
	// 参数检查
	vt := reflect.TypeOf(v)
	if vt.Kind() != reflect.Ptr {
		err := errors.New("bind param kind must be pointer")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObjSlice fail")
		return err
	}
	if vt.Elem().Kind() != reflect.Slice {
		err := errors.New("bind param kind must be slice")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObjSlice fail")
		return err
	}
	elemtype := vt.Elem().Elem() // 第一层是slice，第二层是slice中的元素
	if elemtype.Kind() == reflect.Pointer {
		// 元素是指针，指针指向的类型必须是结构
		if elemtype.Elem().Kind() != reflect.Struct {
			err := errors.New("bind param elem kind must be struct or *struct")
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisPipeline BindJsonObjSlice bind param kind must be struct or *Struct")
			return err
		}
	} else if elemtype.Kind() != reflect.Struct {
		err := errors.New("bind param elem kind must be struct or *struct")
		utils.LogCtx(log.Error(), c.ctx).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObjSlice fail")
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
		case string:
		case []byte:
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
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObjSlice fail")
			return err
		}
	}
	return nil
}
