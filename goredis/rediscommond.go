package goredis

// https://github.com/yuwf/gobase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const typeErrFmt = "%v(%v) not to %v"

var RedisTag = "redis"

// 获取第一个key位置
func GetFirstKeyPos(cmd redis.Cmder) int {
	vo := reflect.ValueOf(cmd).Elem()
	keyPos := vo.FieldByName("keyPos")
	pos := keyPos.Int()
	if pos != 0 {
		return int(pos)
	}
	switch cmd.Name() {
	case "eval", "evalsha", "eval_ro", "evalsha_ro":
		if fmt.Sprint(cmd.Args()[2]) != "0" {
			return 3
		}
		return 0
	case "publish":
		return 1
	case "memory":
		if fmt.Sprint(cmd.Args()[1]) == "usage" {
			return 2
		}
	}
	return 1
}

type RedisCommond struct {
	// 命令名和参数
	ctx     context.Context
	Cmd     redis.Cmder   // 命令对象
	Cmds    []redis.Cmder // 管道的使用
	CmdDesc string        // 命令的描述
	Elapsed time.Duration // 耗时
	// 绑定回调
	callback func(reply interface{}) error // 如果命令失败 不会回调， redis.Nil返回的空错误也认为是一种错误也认为是错误

	nscallback func() *redis.Cmd // 专门为管道中执行Script预留的变量
}

func (c *RedisCommond) CmdString() string {
	var str strings.Builder
	if len(c.CmdDesc) > 0 {
		str.WriteString("(" + c.CmdDesc + ")")
	}
	if c.Cmd != nil {
		for i, arg := range c.Cmd.Args() {
			if str.Len() > 128 {
				s := fmt.Sprintf(" (%d)...", len(c.Cmd.Args())-i)
				str.WriteString(s)
				break
			}
			if i > 0 {
				str.WriteString(" ")
			}

			appendArg(&str, arg)
		}
	}
	if c.Cmds != nil {
		for n, cmd := range c.Cmds {
			if str.Len() > 128 {
				str.WriteString(fmt.Sprintf(" ...(%d) ", len(c.Cmds)-n))
				break
			}
			if n > 0 {
				str.WriteString(",")
			}
			str.WriteString(fmt.Sprintf("%d:", n))
			for i, arg := range cmd.Args() {
				if str.Len() > 128 {
					s := fmt.Sprintf(" ...(%d)", len(cmd.Args())-i)
					str.WriteString(s)
					break
				}
				if i > 0 {
					str.WriteString(" ")
				}
				appendArg(&str, arg)
			}
		}
	}
	return str.String()
}

func (c *RedisCommond) ReplyString() string {
	var str strings.Builder
	if c.Cmd != nil {
		cmdValue := reflect.ValueOf(c.Cmd)
		valFun, ok := cmdValue.Type().MethodByName("Val")
		if ok && valFun.Type.NumIn() == 1 && valFun.Type.NumOut() == 1 {
			rst := valFun.Func.Call([]reflect.Value{cmdValue})
			val := rst[0].Interface()
			appendArg(&str, val)
		} else if ok && valFun.Type.NumIn() == 1 && valFun.Type.NumOut() == 2 {
			rst := valFun.Func.Call([]reflect.Value{cmdValue})
			val := rst[1].Interface()
			appendArg(&str, val)
		}
	}
	if c.Cmds != nil {
		for n, cmd := range c.Cmds {
			if str.Len() > 128 {
				str.WriteString(fmt.Sprintf(" ...(%d) ", len(c.Cmds)-n))
				break
			}
			if n > 0 {
				str.WriteString(",")
			}
			str.WriteString(fmt.Sprintf("%d:", n))
			if cmd.Err() != nil {
				str.WriteString("<" + cmd.Err().Error() + ">")
				continue
			}
			cmdValue := reflect.ValueOf(cmd)
			valFun, ok := cmdValue.Type().MethodByName("Val")
			if ok && valFun.Type.NumIn() == 1 && valFun.Type.NumOut() == 1 {
				rst := valFun.Func.Call([]reflect.Value{cmdValue})
				val := rst[0].Interface()
				appendArg(&str, val)
			} else if ok && valFun.Type.NumIn() == 1 && valFun.Type.NumOut() == 2 {
				rst := valFun.Func.Call([]reflect.Value{cmdValue})
				val := rst[1].Interface()
				appendArg(&str, val)
			}
		}
	}

	return str.String()
}

// 参考 go-redis/internal/arg.go
func appendArg(str *strings.Builder, v interface{}) {
	switch v := v.(type) {
	case nil:
		str.WriteString("<nil>")
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
	case int:
		str.WriteString(strconv.FormatInt(int64(v), 10))
	case int8:
		str.WriteString(strconv.FormatInt(int64(v), 10))
	case int16:
		str.WriteString(strconv.FormatInt(int64(v), 10))
	case int32:
		str.WriteString(strconv.FormatInt(int64(v), 10))
	case int64:
		str.WriteString(strconv.FormatInt(int64(v), 10))
	case uint:
		str.WriteString(strconv.FormatInt(int64(v), 10))
	case uint8:
		str.WriteString(strconv.FormatInt(int64(v), 10))
	case uint16:
		str.WriteString(strconv.FormatInt(int64(v), 10))
	case uint32:
		str.WriteString(strconv.FormatInt(int64(v), 10))
	case uint64:
		str.WriteString(strconv.FormatInt(int64(v), 10))
	case float32:
		str.WriteString(strconv.FormatFloat(float64(v), 'f', -1, 64))
	case float64:
		str.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
	case bool:
		if v {
			str.WriteString("true")
		} else {
			str.WriteString("false")
		}
	case time.Time:
		str.WriteString(v.Format(time.RFC3339Nano))
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
		utils.LogCtx(log.Error(), c.ctx).Err(err).Msg("RedisCommond Bind fail")
		return err
	}
	if vo.IsNil() {
		err := errors.New("bind param pointer is nil")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Msg("RedisCommond Bind fail")
		return err
	}
	elem := vo.Elem()
	if !elem.CanSet() {
		err := errors.New("bind param kind must be canset")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Msg("RedisCommond Bind fail")
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
			utils.LogCtx(log.Error(), c.ctx).Err(err).Msg("RedisCommond Bind fail")
			return err
		}
		elem.SetBytes([]byte{})
	default:
		err := errors.New("param not support " + fmt.Sprint(elem.Type()))
		utils.LogCtx(log.Error(), c.ctx).Err(err).Msg("RedisCommond Bind fail")
		return err
	}
	// 绑定回到函数
	c.callback = func(reply interface{}) error {
		if reply == nil {
			return nil
		}
		return InterfaceToValue(reply, elem)
	}
	// 直接调用的Redis 此时已经有结果值了
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond Bind fail")
			return err
		}
		err := c.callback(cmd.Val())
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
			err := InterfaceToValue(r, v)
			if err != nil {
				return err
			}
			sli = reflect.Append(sli, v)
		case string:
			v := reflect.New(elemtype).Elem()
			err := InterfaceToValue(r, v)
			if err != nil {
				return err
			}
			sli = reflect.Append(sli, v)
		case []byte:
			v := reflect.New(elemtype).Elem()
			err := InterfaceToValue(r, v)
			if err != nil {
				return err
			}
			sli = reflect.Append(sli, v)
		case []interface{}:
			for i := range r {
				v := reflect.New(elemtype).Elem()
				if r[i] != nil {
					err := InterfaceToValue(r[i], v)
					if err != nil {
						return err
					}
				}
				sli = reflect.Append(sli, v)
			}
			return nil
		case redis.Error:
			return r
		}
		return fmt.Errorf(typeErrFmt, reflect.TypeOf(reply), reply, sli.Type())
	}
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindSlice fail")
			return err
		}
		err := c.callback(cmd.Val())
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
				err := InterfaceToValue(r[i], key)
				if err != nil {
					return err
				}
				value := reflect.New(elemtype).Elem()
				if r[i+1] != nil {
					err = InterfaceToValue(r[i+1], value)
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
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindMap fail")
			return err
		}
		err := c.callback(cmd.Val())
		if err != nil {
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindMap fail")
			return err
		}
	}
	return nil
}

func (c *RedisCommond) BindValue(value reflect.Value) error {
	// 绑定回到函数
	c.callback = func(reply interface{}) error {
		if reply == nil {
			return nil
		}
		return InterfaceToValue(reply, value)
	}
	// 直接调用的Redis 此时已经有结果值了
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindValue fail")
			return err
		}
		err := c.callback(cmd.Val())
		if err != nil {
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindValue fail")
			return err
		}
	}
	return nil
}

// 如果命令中返回的数据全部为nil，返回redis.Nil错误
func (c *RedisCommond) BindValues(values []reflect.Value) error {
	c.callback = func(reply interface{}) error {
		switch r := reply.(type) {
		case int64:
		case string:
		case []byte:
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
				err := InterfaceToValue(r[rindex], values[i])
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
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindValues fail")
			return err
		}
		err := c.callback(cmd.Val())
		if err != nil {
			if err != redis.Nil {
				utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindValues fail")
			}
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
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObj fail")
			return err
		}
		err := c.callback(cmd.Val())
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
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisPipeline BindJsonObjSlice bind param kind must be struct or *struct")
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
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObjSlice fail")
			return err
		}
		err := c.callback(cmd.Val())
		if err != nil {
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObjSlice fail")
			return err
		}
	}
	return nil
}

func (c *RedisCommond) BindJsonObjMap(v interface{}) error {
	// 参数检查
	vt := reflect.TypeOf(v)
	if vt.Kind() != reflect.Ptr {
		err := errors.New("bind param kind must be pointer")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObjMap fail")
		return err
	}
	if vt.Elem().Kind() != reflect.Map {
		err := errors.New("bind param kind must be map")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObjMap fail")
		return err
	}
	// keytype只能是基础类型
	keytype := vt.Elem().Key()
	isBaseType := false
	switch keytype.Kind() {
	case reflect.Bool:
		isBaseType = true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		isBaseType = true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		isBaseType = true
	case reflect.Slice:
		if keytype.Elem().Kind() == reflect.Uint8 {
			isBaseType = true
		}
	case reflect.String:
		isBaseType = true
	}
	if !isBaseType {
		err := errors.New("bind param key must be base type")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObjMap fail")
		return err
	}

	elemtype := vt.Elem().Elem() // 第一层是map，第二层是slice中的元素
	if elemtype.Kind() == reflect.Pointer {
		// 元素是指针，指针指向的类型必须是结构
		if elemtype.Elem().Kind() != reflect.Struct {
			err := errors.New("bind param elem kind must be struct or *struct")
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisPipeline BindJsonObjMap bind param kind must be struct or *struct")
			return err
		}
	} else if elemtype.Kind() != reflect.Struct {
		err := errors.New("bind param elem kind must be struct or *struct")
		utils.LogCtx(log.Error(), c.ctx).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObjMap fail")
		return err
	}

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
				err := InterfaceToValue(r[i], key)
				if err != nil {
					return err
				}

				var value reflect.Value
				if elemtype.Kind() == reflect.Pointer {
					value = reflect.New(elemtype.Elem())
				} else {
					value = reflect.New(elemtype).Elem()
				}
				switch r2 := r[i+1].(type) {
				case int64:
				case string:
					if elemtype.Kind() == reflect.Pointer {
						json.Unmarshal([]byte(r2), value.Interface())
					} else {
						json.Unmarshal([]byte(r2), value.Addr().Interface())
					}
					m.SetMapIndex(key, value)
				case []byte:
					if elemtype.Kind() == reflect.Pointer {
						json.Unmarshal(r2, value.Interface())
					} else {
						json.Unmarshal(r2, value.Addr().Interface())
					}
					m.SetMapIndex(key, value)
				case []interface{}:
				case nil:
				case redis.Error:
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
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObjMap fail")
			return err
		}
		err := c.callback(cmd.Val())
		if err != nil {
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", c.CmdString()).Msg("RedisCommond BindJsonObjMap fail")
			return err
		}
	}
	return nil
}
