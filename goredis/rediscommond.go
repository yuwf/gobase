package goredis

// https://github.com/yuwf

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gobase/utils"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const CtxKey_nonilerr = "nonilerr" // 命令移除空错误 值：不受限制 一般写1
const CtxKey_nolog = "nolog"       // 不打印掉应该能日志，错误日志还会打印 值：不受限制 一般写1

const CtxKey_rediscmd = "rediscmd"     // 值：RedisCommand对象 一般情况内部使用
const CtxKey_caller = "caller"         // 值：CallerDesc对象 一般情况内部使用
const CtxKey_cmddesc = "cmddesc"       // 值：字符串 命令描述 一般情况内部使用
const CtxKey_noscript = "noscript"     // 屏蔽NOSCRIPT的错误提示(在使用Reids.Run命令时建议使用)， 值：不受限制 一般情况内部使用
const CtxKey_scriptname = "scriptname" // 值：字符串 优化日志输出 一般情况内部使用

const typeErrFmt = "%v(%v) not to %v"

func IsNilError(err error) bool {
	return err == redis.Nil
}

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
	Cmd     redis.Cmder       // 命令对象
	Cmds    []redis.Cmder     // 管道的使用
	CmdDesc string            // 命令的描述
	Elapsed time.Duration     // 耗时
	Caller  *utils.CallerDesc // 调用位置

	// 绑定回调
	callback func(reply interface{}) error
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
		err := errors.New("Bind Param Kind must be Pointer")
		log.Error().Err(err).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
		return err
	}
	if vo.IsNil() {
		err := errors.New("Bind Pointer is nil")
		log.Error().Err(err).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
		return err
	}
	elem := vo.Elem()
	if !elem.CanSet() {
		err := errors.New("Param Kind must be CanSet")
		log.Error().Err(err).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
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
			log.Error().Err(err).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
			return err
		}
		elem.SetBytes([]byte{})
	default:
		err := errors.New("Param not support " + fmt.Sprint(elem.Type()))
		log.Error().Err(err).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
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
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
			return err
		}
		err := c.callback(cmd.Val())
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
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
			return err
		}
		err := c.callback(cmd.Val())
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
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
			return err
		}
		err := c.callback(cmd.Val())
		if err != nil {
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult BindMap fail")
			return err
		}
	}
	return nil
}

func (c *RedisCommond) HMGetBindObj(v interface{}) error {
	// 参数检查
	_, elemts, structtype, err := hmgetObjArgs(nil, v)
	if err != nil {
		log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult HMGetBindObj fail")
		return err
	}
	return c.hmgetCallback(elemts, structtype)
}

func (c *RedisCommond) hmgetCallback(elemts []reflect.Value, structtype reflect.Type) error {
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
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
			return err
		}
		err := c.callback(cmd.Val())
		if err != nil {
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult HMGetBindObj fail")
			return err
		}
	}
	return nil
}

// 把v对象的tag数据 写到args中
/*
type Test struct {
	F1 int `redis:"f1"`
	F2 int `redis:"f2"`
}
上面的对象写入args中的格式为  "f1" "f2"
*/
func hmgetObjArgs(args []interface{}, v interface{}) ([]interface{}, []reflect.Value, reflect.Type, error) {
	// 参数检查
	vo := reflect.ValueOf(v)
	if vo.Kind() != reflect.Ptr {
		err := errors.New("Bind Param Kind must be Pointer")
		return args, nil, nil, err
	}
	if vo.IsNil() {
		err := errors.New("Bind Param Pointer is nil")
		return args, nil, nil, err
	}
	structtype := vo.Elem().Type() // 第一层是指针，第二层是结构
	structvalue := vo.Elem()
	if structtype.Kind() != reflect.Struct {
		err := errors.New("Bind Param Kind must be Struct")
		return args, nil, nil, err
	}

	argsNum := len(args) // 先记录下进来时的参数个数
	// 组织参数
	numfield := structtype.NumField()
	elemts := []reflect.Value{} // 结构中成员的变量地址
	for i := 0; i < numfield; i += 1 {
		tag := structtype.Field(i).Tag.Get("redis")
		if tag == "-" || tag == "" {
			continue
		}
		v := structvalue.Field(i)
		if v.CanSet() {
			elemts = append(elemts, v)
			args = append(args, tag)
		}
	}
	if len(args) == argsNum {
		err := errors.New("StructMem invalid")
		return args, nil, nil, err
	}
	return args, elemts, structtype, nil
}

// 把v对象的数据和tag数据 写到args中
/*
type Test struct {
	F1 int `redis:"f1"`
	F2 int `redis:"f2"`
}
上面的对象写入args中的格式为  "f1" F1 "f2" F2
*/
func hmsetObjArgs(args []interface{}, v interface{}) ([]interface{}, error) {
	// 验证参数
	vo := reflect.ValueOf(v)
	if vo.Kind() != reflect.Ptr {
		return args, errors.New("Param Kind must be Pointer")
	}
	if vo.IsNil() {
		return args, errors.New("Param Pointer is nil")
	}
	structtype := vo.Elem().Type() // 第一层是指针，第二层是结构
	structvalue := vo.Elem()
	if structtype.Kind() != reflect.Struct {
		return args, errors.New("Param Kind must be Struct")
	}

	argsNum := len(args) // 先记录下进来时的参数个数
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
			args = append(args, tag)
			args = append(args, v.Interface())
		case reflect.Slice:
			if v.Type().Elem().Kind() != reflect.Uint8 {
				return args, errors.New("SliceElem Kind must be Uint8(byte)")
			}
			args = append(args, tag)
			args = append(args, v.Interface())
		default:
			return args, errors.New("Param not support " + fmt.Sprint(v.Type()))
		}
	}
	if len(args) == argsNum {
		return args, errors.New("StructMem invalid")
	}
	return args, nil
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
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
			return err
		}
		err := c.callback(cmd.Val())
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
	if c.Cmd != nil {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		cmd, ok := c.Cmd.(*redis.Cmd)
		if !ok {
			// 调用绑定的都应该是Cmd才对
			err := fmt.Errorf("%s not *redis.Cmd", reflect.TypeOf(c.Cmd))
			log.Error().Err(err).Str("cmd", c.CmdString()).Str("pos", c.Caller.Pos()).Msg("RedisResult Bind fail")
			return err
		}
		err := c.callback(cmd.Val())
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
