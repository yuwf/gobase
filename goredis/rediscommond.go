package goredis

// https://github.com/yuwf/gobase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/yuwf/gobase/utils"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

var RedisTag = "redis"

// 支持绑定的Redis命令
type RedisCommond struct {
	*redis.Cmd
	ctx       context.Context
	processed bool // 是否已经处理完了 区别管道和直接调用的Redis

	// 绑定回调
	callback   func(reply interface{}) error // 如果命令失败 不会回调， redis.Nil返回的空错误也认为是一种错误也认为是错误
	nscallback func() *redis.Cmd             // 专门为管道中执行Script预留的变量
}

func (c *RedisCommond) Bind(v interface{}) error {
	// 参数检查
	vo := reflect.ValueOf(v)
	if vo.Kind() != reflect.Ptr && vo.Kind() != reflect.Interface {
		err := errors.New("bind param kind must be pointer")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Msg("RedisCommond Bind fail")
		return err
	}
	if vo.IsNil() {
		err := errors.New("bind param pointer is nil")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Msg("RedisCommond Bind fail")
		return err
	}
	if !vo.Elem().CanSet() {
		err := errors.New("bind param must be addressable")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Msg("RedisCommond Bind fail")
		return err
	}

	// 绑定回到函数
	c.callback = func(reply interface{}) error {
		return ReplyToValue(reply, vo)
	}
	// 直接调用的Redis 此时已经有结果值了
	if c.processed {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		err := c.callback(c.Cmd.Val())
		if err != nil {
			if err != redis.Nil {
				utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond Bind fail")
			}
			return err
		}
	}
	return nil
}

func (c *RedisCommond) BindValue(value reflect.Value) error {
	// 绑定回到函数
	c.callback = func(reply interface{}) error {
		return ReplyToValue(reply, value)
	}
	// 直接调用的Redis 此时已经有结果值了
	if c.processed {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		err := c.callback(c.Cmd.Val())
		if err != nil {
			if err != redis.Nil {
				utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindValue fail")
			}
			return err
		}
	}
	return nil
}

// 如果命令中返回的数据全部为nil，返回redis.Nil错误
func (c *RedisCommond) BindValues(values []reflect.Value) error {
	c.callback = func(reply interface{}) error {
		return ReplyToValues(reply, values)
	}
	// 直接调用的Redis 此时已经有结果值了
	if c.processed {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		err := c.callback(c.Cmd.Val())
		if err != nil {
			if err != redis.Nil {
				utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindValues fail")
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
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindJsonObj fail")
		return err
	}
	if vo.IsNil() {
		err := errors.New("bind param pointer is nil")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindJsonObj fail")
		return err
	}
	structtype := vo.Elem().Type() // 第一层是指针，第二层是结构
	if structtype.Kind() != reflect.Struct {
		err := errors.New("bind param kind must be struct")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindJsonObj fail")
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
	if c.processed {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		err := c.callback(c.Cmd.Val())
		if err != nil {
			if err != redis.Nil {
				utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindJsonObj fail")
			}
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
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindJsonObjSlice fail")
		return err
	}
	if vt.Elem().Kind() != reflect.Slice {
		err := errors.New("bind param kind must be slice")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindJsonObjSlice fail")
		return err
	}
	elemtype := vt.Elem().Elem() // 第一层是slice，第二层是slice中的元素
	if elemtype.Kind() == reflect.Pointer {
		// 元素是指针，指针指向的类型必须是结构
		if elemtype.Elem().Kind() != reflect.Struct {
			err := errors.New("bind param elem kind must be struct or *struct")
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisPipeline BindJsonObjSlice bind param kind must be struct or *struct")
			return err
		}
	} else if elemtype.Kind() != reflect.Struct {
		err := errors.New("bind param elem kind must be struct or *struct")
		utils.LogCtx(log.Error(), c.ctx).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindJsonObjSlice fail")
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
	if c.processed {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		err := c.callback(c.Cmd.Val())
		if err != nil {
			if err != redis.Nil {
				utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindJsonObjSlice fail")
			}
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
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindJsonObjMap fail")
		return err
	}
	if vt.Elem().Kind() != reflect.Map {
		err := errors.New("bind param kind must be map")
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindJsonObjMap fail")
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
		utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindJsonObjMap fail")
		return err
	}

	elemtype := vt.Elem().Elem() // 第一层是map，第二层是slice中的元素
	if elemtype.Kind() == reflect.Pointer {
		// 元素是指针，指针指向的类型必须是结构
		if elemtype.Elem().Kind() != reflect.Struct {
			err := errors.New("bind param elem kind must be struct or *struct")
			utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisPipeline BindJsonObjMap bind param kind must be struct or *struct")
			return err
		}
	} else if elemtype.Kind() != reflect.Struct {
		err := errors.New("bind param elem kind must be struct or *struct")
		utils.LogCtx(log.Error(), c.ctx).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindJsonObjMap fail")
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
				err := ReplyToValue(r[i], key)
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
	if c.processed {
		if c.Cmd.Err() != nil {
			return c.Cmd.Err()
		}
		err := c.callback(c.Cmd.Val())
		if err != nil {
			if err != redis.Nil {
				utils.LogCtx(log.Error(), c.ctx).Err(err).Str("cmd", CmdString(c.ctx, c.Cmd, nil)).Msg("RedisCommond BindJsonObjMap fail")
			}
			return err
		}
	}
	return nil
}
