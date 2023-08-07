package goredis

// https://github.com/yuwf

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/yuwf/gobase/utils"
)

// 使用Redis做服务器注册和发现使用

// Reids中存储注册服务器各字段的分割符号，所以也要要求服务器字符串字段不能包含下面这个分隔符, 允许外部修改
var RegSep string = "&"

// 多长时间没更新就认为是取消注册了，单位秒, 允许外部修改
// Register对象的注册频率:RegExprieTime/2
var RegExprieTime = 8

const regCheckLockerFmt = "_{%s}_locker_"
const regListFmt = "_{%s}_list_"
const regChannelFmt = "_{%s}_channel_"

// 服务发现相关的脚本

// 注册服务器
// Key1 服务器注册发现的key，zset结构
// Key2 有效的服务器列表拼接的串
// ARGV1 RegExprieTime*1000 毫秒
// ARGV2 发布订阅channel
// ARGV3... 添加的服务器
var registerScirpt = NewScript(`
	redis.replicate_commands()
	-- 读取时间
	local t = redis.call('TIME')
	local stamp = tonumber(t[1])*1000 + tonumber(t[2])/1000

	local score
	for i = 3, #ARGV do
		if not score then
			score = redis.call('ZSCORE', KEYS[1], ARGV[i])
		end
		redis.call('ZADD', KEYS[1], stamp, ARGV[i])
	end

	-- 判断之前的是否还有效，有效不需要发布
	if score and tonumber(score) > stamp-tonumber(ARGV[1]) then
		return 
	end
	-- 读取所有服务器
	local slist = redis.call('ZREVRANGEBYSCORE', KEYS[1], stamp, stamp-tonumber(ARGV[1]))
	table.sort(slist)
	local str = ""
	for i = 1, #slist do
		if i > 1 then 
			str = str .. ","
		end
		str = str .. slist[i]
	end
	-- 存下来，并发布变化
	local reg = "reg"
	for i = 3, #ARGV do
		reg = reg .. " " .. ARGV[i]
	end
	redis.call("SET", KEYS[2], str)
	redis.call("PUBLISH", ARGV[2], reg)
`)

// 注册服务器
// Key1 服务器注册发现的key，zset结构
// Key2 有效的服务器列表拼接的串
// ARGV1 RegExprieTime*1000 毫秒
// ARGV2 发布订阅channel
// ARGV3... 添加的服务器
var deregisterScirpt = NewScript(`
	redis.replicate_commands()
	-- 读取时间
	local t = redis.call('TIME')
	local stamp = tonumber(t[1])*1000 + tonumber(t[2])/1000

	local score
	for i = 3, #ARGV do
		if not score then
			score = redis.call('ZSCORE', KEYS[1], ARGV[i])
		end
		redis.call('ZREM', KEYS[1], ARGV[i])
	end

	-- 判断之前的是否还有效，无效不需要发布
	if not (score and tonumber(score) > stamp-tonumber(ARGV[1])) then
		return
	end
	
	-- 读取所有服务器
	local slist = redis.call('ZREVRANGEBYSCORE', KEYS[1], stamp, stamp-tonumber(ARGV[1]))
	table.sort(slist)
	local str = ""
	for i = 1, #slist do
		if i > 1 then 
			str = str .. ","
		end
		str = str .. slist[i]
	end
	-- 存下来，并发布变化
	local dereg = "dereg"
	for i = 3, #ARGV do
		dereg = dereg .. " " .. ARGV[i]
	end
	redis.call("SET", KEYS[2], str)
	redis.call("PUBLISH", ARGV[2], dereg)
`)

// 检查服务器变化
// Key1 服务器注册发现的key，zset结构
// Key2 有效的服务器列表拼接的串
// Key3 锁，检查抢占的锁
// ARGV1 RegExprieTime*1000 毫秒
// ARGV2 发布订阅channel
// ARGV3 检查抢占的锁的UUID
var checkServicesScirpt = NewScript(`
	redis.replicate_commands()
	-- 先抢占下
	local v = redis.call("GET", KEYS[3])
	if not v then
		redis.call("SET", KEYS[3], ARGV[3], "PX", ARGV[1])
	elseif v == ARGV[3] then
		--redis.call("PEXPIRE", KEYS[3], ARGV[1])
	else
		return 0
	end
	-- 读取时间
	local t = redis.call('TIME')
	local stamp = tonumber(t[1])*1000 + tonumber(t[2])/1000
	-- 读取所有服务器
	local slist = redis.call('ZREVRANGEBYSCORE', KEYS[1], stamp, stamp-tonumber(ARGV[1]))
	table.sort(slist)
	local str = ""
	for i = 1, #slist do
		if i > 1 then 
			str = str .. ","
		end
		str = str .. slist[i]
	end
	-- 检查是否有变化
	local rstr = redis.call("GET", KEYS[2])
	if str == rstr then
		return 1
	end
	-- 存下来，并发布变化
	redis.call("SET", KEYS[2], str)
	redis.call("PUBLISH", ARGV[2], "check")

	-- 删除过期的
	redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, stamp-tonumber(ARGV[1])-1)
	return 2
`)

// RegistryInfo 服务注册信息
type RegistryInfo struct {
	RegistryName   string `json:"registryname,omitempty"`   // 注册的名字 组名
	RegistryID     string `json:"registryid,omitempty"`     // 注册的ID 服务器唯一ID
	RegistryAddr   string `json:"registryaddr,omitempty"`   // 服务器对外暴露的地址
	RegistryPort   int    `json:"registryport,omitempty"`   // 服务器对外暴露的端口
	RegistryScheme string `json:"registryscheme,omitempty"` // 服务器使用的协议
}

func (r *RegistryInfo) MarshalZerologObject(e *zerolog.Event) {
	if r != nil {
		e.Str("ServiceName", r.RegistryName).
			Str("NodeId", r.RegistryID).
			Str("Addr", r.RegistryAddr).
			Int("Port", r.RegistryPort).
			Str("Scheme", r.RegistryScheme)
	}
}

type Register struct {
	r      *Redis
	key    string
	values []string
	state  int32 // 注册状态 原子操作 0：未注册 1：注册中 2：已注册
	ctx    context.Context
	// 退出检查使用
	quit chan int
}

// key是按照zset写入
func (r *Redis) CreateRegister(key string, cfg *RegistryInfo) *Register {
	register := &Register{
		r:      r,
		key:    key,
		values: []string{},
		state:  0,
		ctx:    context.WithValue(context.WithValue(context.TODO(), CtxKey_nolog, 1), CtxKey_nonilerr, 1), // 不要日志 不要nil错误
		quit:   make(chan int),
	}
	// 生成注册的value
	register.values = append(register.values, strings.Join([]string{cfg.RegistryName, cfg.RegistryID, cfg.RegistryAddr, strconv.Itoa(cfg.RegistryPort), cfg.RegistryScheme}, RegSep))
	log.Info().Str("Info", strings.Join(register.values, ",")).Msg("RedisRegister Create success")
	return register
}

func (r *Redis) CreateRegisterEx(key string, cfgs []*RegistryInfo) *Register {
	register := &Register{
		r:      r,
		key:    key,
		values: []string{},
		state:  0,
		ctx:    context.WithValue(context.WithValue(context.TODO(), CtxKey_nolog, 1), CtxKey_nonilerr, 1), // 不要日志 不要nil错误
		quit:   make(chan int),
	}
	// 生成注册的value
	for _, cfg := range cfgs {
		register.values = append(register.values, strings.Join([]string{cfg.RegistryName, cfg.RegistryID, cfg.RegistryAddr, strconv.Itoa(cfg.RegistryPort), cfg.RegistryScheme}, RegSep))
	}
	log.Info().Str("Info", strings.Join(register.values, ",")).Msg("RedisRegister Create success")
	return register
}

func (r *Register) Reg() error {
	if r.r == nil {
		err := errors.New("Redis is nil")
		log.Error().Err(err).Msg("RedisRegister Reg fail")
		return err
	}

	if !atomic.CompareAndSwapInt32(&r.state, 0, 1) {
		log.Error().Str("Info", strings.Join(r.values, ",")).Msg("RedisRegister already register")
		return nil
	}

	// 先写一次Redis，确保能注册成功
	err := r.zadd()
	if err != nil {
		atomic.StoreInt32(&r.state, 0)
		return err
	}

	// 开启协程
	go r.loop()
	atomic.StoreInt32(&r.state, 2)

	log.Info().Str("Info", strings.Join(r.values, ",")).Msg("RedisRegister register success")
	return nil
}

func (r *Register) DeReg() error {
	if !atomic.CompareAndSwapInt32(&r.state, 2, 0) {
		log.Error().Str("Info", strings.Join(r.values, ",")).Msg("RedisRegister not register")
		return nil
	}

	r.quit <- 1
	<-r.quit

	log.Info().Str("Info", strings.Join(r.values, ",")).Msgf("RedisRegister deregistered")
	return nil
}

func (r *Register) loop() {
	// 定时写Redis
	for {
		timer := time.NewTimer(time.Duration(RegExprieTime) / 2 * time.Second)
		select {
		case <-timer.C:
			r.zadd()

		case <-r.quit:
			// 删除写的Redis
			r.zrem()
			r.quit <- 1

			if !timer.Stop() {
				select {
				case <-timer.C: // try to drain the channel
				default:
				}
			}
			return
		}
	}
}

func (r *Register) zadd() error {
	ctx := context.WithValue(r.ctx, "cmddesc", "Register")
	ctx = context.WithValue(ctx, "caller", utils.GetCallerDesc(1))
	listKey := fmt.Sprintf(regListFmt, r.key)    // 服务器列表
	channel := fmt.Sprintf(regChannelFmt, r.key) // 广播channel
	keys := []string{r.key, listKey}
	args := []interface{}{RegExprieTime * 1000, channel}
	for _, v := range r.values {
		args = append(args, v)
	}
	cmd := r.r.DoScript(ctx, registerScirpt, keys, args...)
	if cmd.Err() != nil {
		// 错误了 在来一次
		cmd = r.r.DoScript(ctx, registerScirpt, keys, args...)
	}
	return cmd.Err()
}

func (r *Register) zrem() error {
	ctx := context.WithValue(r.ctx, "cmddesc", "Register")
	ctx = context.WithValue(ctx, "caller", utils.GetCallerDesc(1))
	listKey := fmt.Sprintf(regListFmt, r.key)    // 服务器列表
	channel := fmt.Sprintf(regChannelFmt, r.key) // 广播channel
	keys := []string{r.key, listKey}
	args := []interface{}{RegExprieTime * 1000, channel}
	for _, v := range r.values {
		args = append(args, v)
	}
	cmd := r.r.DoScript(ctx, deregisterScirpt, keys, args...)
	if cmd.Err() != nil {
		// 错误了 在来一次
		cmd = r.r.DoScript(ctx, deregisterScirpt, keys, args...)
	}
	return cmd.Err()
}
