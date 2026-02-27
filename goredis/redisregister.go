package goredis

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// 使用Redis做服务器注册和发现使用

// Reids中存储注册服务器各字段的分割符号，所以也要要求服务器字符串字段不能包含下面这个分隔符, 允许外部修改
var RegSep string = "&"

// 多长时间没更新就认为是取消注册了，单位秒, 允许外部修改
// Register对象的注册频率:RegExprieTime/2
var RegExprieTime = 8

// 服务发现相关的脚本

// 注册服务器，定时调用注册
// Key1 服务器注册发现的key，zset结构，若数据发生变化会向Key1命名的channel发送通知
// ARGV1 RegExprieTime 秒
// ARGV2 描述
// ARGV3... 注册的服务器
var registerScirpt = NewScript(`
	-- 有写入操作，开启复制模式，否则下面的获取时间错误
	redis.replicate_commands()
	-- 读取时间
	local t = redis.call('TIME')
	local stamp = tonumber(t[1]) + tonumber(t[2])/1000000
	local expireat = stamp-tonumber(ARGV[1])

	local publish = false -- 是否需要发布
	for i = 3, #ARGV do
		-- 判断之前的是否还有效，有效不需要发布
		local score = redis.call('ZSCORE', KEYS[1], ARGV[i])
		if not (score and tonumber(score) > expireat) then
			publish = true
		end
		redis.call('ZADD', KEYS[1], stamp, ARGV[i])
	end

	if publish then
		-- 读取所有服务器，存下来
		local slist = redis.call('ZREVRANGEBYSCORE', KEYS[1], stamp, expireat)
		local str = ""
		if slist and #slist > 0 then
			table.sort(slist)
			str = table.concat(slist,",")
		end
		redis.call("SET", KEYS[1] .. "_list", str)
		-- 发布变化 通知信息样式 desc:第一个[(剩余个数)]
		redis.call("PUBLISH", KEYS[1], ARGV[2] .. ":" .. ARGV[3] .. ((#ARGV > 3) and "..(".. tostring(#ARGV-3) ..")" or "" ))
	end
`)

// 注册服务器
// Key1 服务器注册发现的key，zset结构，若数据发生变化会向Key1命名的channel发送通知
// ARGV1 RegExprieTime 秒
// ARGV2 描述
// ARGV3... 删除的服务器
var deregisterScirpt = NewScript(`
	-- 有写入操作，开启复制模式，否则下面的获取时间错误
	redis.replicate_commands()
	-- 读取时间
	local t = redis.call('TIME')
	local stamp = tonumber(t[1]) + tonumber(t[2])/1000000
	local expireat = stamp-tonumber(ARGV[1])

	local publish = false -- 是否需要发布
	for i = 3, #ARGV do
		-- 判断之前的是否还有效，无效不需要发布
		local score = redis.call('ZSCORE', KEYS[1], ARGV[i])
		if score and tonumber(score) > expireat then
			publish = true
		end
		redis.call('ZREM', KEYS[1], ARGV[i])
	end

	if publish then
		-- 读取所有服务器，存下来
		local slist = redis.call('ZREVRANGEBYSCORE', KEYS[1], stamp, expireat)
		local str = ""
		if slist and #slist > 0 then
			table.sort(slist)
			str = table.concat(slist,",")
		end
		redis.call("SET", KEYS[1] .. "_list", str)
		-- 发布变化 通知信息样式 desc:第一个[(剩余个数)]
		redis.call("PUBLISH", KEYS[1], ARGV[2] .. ":" .. ARGV[3] .. ((#ARGV > 3) and "..(".. tostring(#ARGV-3) ..")" or "" ))
	end
`)

// 检查服务器变化
// Key1 服务器注册发现的key，zset结构，若数据发生变化会向Key1命名的channel发送通知
// ARGV1 RegExprieTime 秒
// ARGV2 检查抢占的锁的UUID
var checkServicesScirpt = NewScript(`
	-- 先抢占下
	local checkkey = KEYS[1] .. "_check"
	local v = redis.call("GET", checkkey)
	if not v then
		redis.call("SET", checkkey, ARGV[2], "EX", ARGV[1])
	elseif v == ARGV[2] then
		-- OK
	else
		return 0
	end

	-- 有写入操作，开启复制模式，否则下面的获取时间错误
	redis.replicate_commands()
	-- 读取时间
	local t = redis.call('TIME')
	local stamp = tonumber(t[1]) + tonumber(t[2])/1000000
	local expireat = stamp-tonumber(ARGV[1])

	-- 读取所有服务器，有变化，存下来
	local slist = redis.call('ZREVRANGEBYSCORE', KEYS[1], stamp, expireat)
	local str = ""
	if slist and #slist > 0 then
		table.sort(slist)
		str = table.concat(slist,",")
	end
	-- 检查是否有变化
	local listkey = KEYS[1] .. "_list"
	local rstr = redis.call("GET", listkey)
	rstr = rstr and rstr or ""
	if str == rstr then
		return 1
	end
	redis.call("SET", listkey, str)
	-- 发布变化 通知信息样式 check:uuid
	redis.call("PUBLISH", KEYS[1], "check:" .. ARGV[2])

	-- 删除过期的
	redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, expireat-1)
	return 2
`)

// 读取服务器
// Key1 服务器注册发现的key，zset结构
// ARGV1 RegExprieTime 秒
var readRegisterScirpt = NewScript(`
	local t = redis.call('TIME')
	local stamp = tonumber(t[1]) + tonumber(t[2])/1000000
	local expireat = stamp-tonumber(ARGV[1])
	return redis.call('ZREVRANGEBYSCORE', KEYS[1], stamp, expireat)
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
	// 不可修改
	ctx context.Context
	r   *Redis
	key string

	// 只有
	mu     sync.Mutex
	values []string

	state int32    // 注册状态 原子操作 0：未注册 1：注册中 2：已注册
	quit  chan int // 退出检查使用
}

// key是按照zset写入
func (r *Redis) CreateRegister(key string, cfg *RegistryInfo) *Register {
	register := &Register{
		ctx:    context.WithValue(utils.CtxSetNolog(context.TODO()), CtxKey_nonilerr, 1), // 不要日志 不要nil错误
		r:      r,
		key:    key,
		values: []string{},
		state:  0,
		quit:   make(chan int),
	}
	if cfg != nil {
		// 生成注册的value
		register.values = append(register.values, strings.Join([]string{cfg.RegistryName, cfg.RegistryID, cfg.RegistryAddr, strconv.Itoa(cfg.RegistryPort), cfg.RegistryScheme}, RegSep))
	}
	return register
}

func (r *Redis) CreateRegisters(key string, cfgs []*RegistryInfo) *Register {
	register := &Register{
		ctx:    context.WithValue(utils.CtxSetNolog(context.TODO()), CtxKey_nonilerr, 1), // 不要日志 不要nil错误
		r:      r,
		key:    key,
		values: []string{},
		state:  0,
		quit:   make(chan int),
	}
	// 生成注册的value
	for _, cfg := range cfgs {
		register.values = append(register.values, strings.Join([]string{cfg.RegistryName, cfg.RegistryID, cfg.RegistryAddr, strconv.Itoa(cfg.RegistryPort), cfg.RegistryScheme}, RegSep))
	}
	return register
}

func (r *Register) Add(cfg *RegistryInfo) error {
	value := strings.Join([]string{cfg.RegistryName, cfg.RegistryID, cfg.RegistryAddr, strconv.Itoa(cfg.RegistryPort), cfg.RegistryScheme}, RegSep)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, v := range r.values {
		if v == value {
			return nil // 存在了
		}
	}

	if atomic.LoadInt32(&r.state) != 0 {
		args := []interface{}{RegExprieTime, "add", value}
		err := r.zadd(args)
		if err != nil {
			log.Error().Str("Info", value).Msg("RedisRegister Add")
			return err
		}
	}
	r.values = append(r.values, value)

	log.Info().Str("Info", value).Msg("RedisRegister Add")
	return nil
}

func (r *Register) Remove(cfg *RegistryInfo) error {
	value := strings.Join([]string{cfg.RegistryName, cfg.RegistryID, cfg.RegistryAddr, strconv.Itoa(cfg.RegistryPort), cfg.RegistryScheme}, RegSep)
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, v := range r.values {
		if v == value {

			if atomic.LoadInt32(&r.state) != 0 {
				args := []interface{}{RegExprieTime, "rem", value}
				err := r.zrem(args)
				if err != nil {
					log.Error().Str("Info", value).Msg("RedisRegister Remove")
					return err
				}
			}

			r.values = append(r.values[:i], r.values[i+1:]...)

			log.Info().Str("Info", value).Msg("RedisRegister Remove")
			return nil
		}
	}
	return nil
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
	// 生成注册的value
	if len(r.values) > 0 {
		args := []interface{}{RegExprieTime, "reg"}
		for _, v := range r.values {
			args = append(args, v)
		}
		err := r.zadd(args)
		if err != nil {
			atomic.StoreInt32(&r.state, 0)
			return err
		}
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
			if len(r.values) > 0 {
				args := []interface{}{RegExprieTime, "update"}
				r.mu.Lock()
				for _, v := range r.values {
					args = append(args, v)
				}
				r.zadd(args)
				r.mu.Unlock()
			}

		case <-r.quit:
			// 删除写的Redis
			if len(r.values) > 0 {
				args := []interface{}{RegExprieTime, "quit"}
				r.mu.Lock()
				for _, v := range r.values {
					args = append(args, v)
				}
				r.zrem(args)
				r.mu.Unlock()
			}

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

// args第一个倒计时，其他是values
// 线程安全
func (r *Register) zadd(args []interface{}) error {
	ctx := context.WithValue(r.ctx, CtxKey_cmddesc, "Register")
	cmd := r.r.DoScript(ctx, registerScirpt, []string{r.key}, args...)
	if cmd.Err() != nil {
		// 错误了 在来一次
		cmd = r.r.DoScript(ctx, registerScirpt, []string{r.key}, args...)
	}
	return cmd.Err()
}

func (r *Register) zrem(args []interface{}) error {
	ctx := context.WithValue(r.ctx, CtxKey_cmddesc, "Register")
	cmd := r.r.DoScript(ctx, deregisterScirpt, []string{r.key}, args...)
	if cmd.Err() != nil {
		// 错误了 在来一次
		cmd = r.r.DoScript(ctx, deregisterScirpt, []string{r.key}, args...)
	}
	return cmd.Err()
}
