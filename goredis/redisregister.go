package goredis

// https://github.com/yuwf/gobase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// 多长时间没更新就认为是取消注册了，单位秒, 允许外部修改
// Register对象的注册频率:RegExprieTime/2
var RegExprieTime = 8

var checkAllScirpt = LuaMd5Script + (`
local function checkAll(key,expire,expireat,desc)
	-- 读取所有服务器
	local rst = redis.call("HGETALL", KEYS[1])

	-- 组织成map
	local smap = {}
	for i = 1, #rst, 2 do
		smap[rst[i]] = rst[i + 1]
	end

	-- 过滤
	local slist = {}        -- 有效服务器
	local delfields = {}  -- 过期服务器和时间字段
	for field in pairs(smap) do
		if string.sub(field, 1, 6) ~= "_inn_:" then
			-- 判断对应的时间是否过期了
			local timefield = "_inn_:time:" .. field
			local timevalue = tonumber(smap[timefield])
			if not timevalue then
				-- 没有时间字段 直接删除
				table.insert(delfields, field)
			elseif timevalue > expireat then
				table.insert(slist, field) -- 有效服务器
			elseif timevalue < expireat - expire then
				-- 超过了2倍过期时间，删除服务器和时间字段
				table.insert(delfields, field)
				table.insert(delfields, timefield)
			end
		elseif string.sub(field, 1, 11) == "_inn_:time:" then
			-- 时间字段存在，判断对应的字段是否存在，不存在删除时间字段，防止数据异常
			local field_ = string.sub(field, 12)
			if not smap[field_] then
				table.insert(delfields, field)
			end
		end
	end

	if #delfields > 0 then
		redis.call("HDEL", key, unpack(delfields))
	end

	-- 排序 计算md5
	table.sort(slist)
	local ctx = md5.new()
	for _, item in ipairs(slist) do
		ctx:update(smap[item])
	end
	local digest = md5.tohex(ctx:finish())

	-- 对比md5是否变化
	local checkdatafield = "_inn_:checkdata"
	local olddigest = smap[checkdatafield]
	olddigest = olddigest and olddigest or ""
	if digest == olddigest then
		return
	end
	redis.call("HSET", key, checkdatafield, digest)
	-- 发布变化 通知信息样式 desc
	redis.call("PUBLISH", key, desc)
end
`)

// 服务发现相关的脚本

// 注册服务器，定时调用注册
// Key 服务器注册发现的key，HASH结构，若数据发生变化会向Key1命名的channel发送通知
// ARGV1 RegExprieTime 秒
// ARGV2 描述
// ARGV3 ARGV4... 格式:key:value
var registerScirpt = NewScript(checkAllScirpt + `
	-- 有写入操作，开启复制模式，否则下面的获取时间错误
	redis.replicate_commands()
	-- 读取时间
	local expire = tonumber(ARGV[1])
	local t = redis.call('TIME')
	local stamp = tonumber(t[1]) + tonumber(t[2])/1000000
	local expireat = stamp-expire -- 有效数据的最早注册时间

	local fields = {}
	local setvalues = {}
	for i = 3, #ARGV, 2 do
		setvalues[ARGV[i]] = ARGV[i+1]
		table.insert(fields, ARGV[i])
		table.insert(fields, "_inn_:time:" .. ARGV[i])
	end
	local values = redis.call("HMGET", KEYS[1], unpack(fields))

	local publish = false -- 因修改数据导致需要发布
	local sets = {}
	for i = 1, #values, 2 do
		-- 判断之前的是否还有效，有效不需要发布
		local field = fields[i]
		local timefield = fields[i+1]
		local score = tonumber(values[i+1])
		if not (score and score > expireat) then
			publish = true
		end
		if values[i] ~= setvalues[field] then
			publish = true
			table.insert(sets, field)
			table.insert(sets, setvalues[field])
			table.insert(sets, timefield)
			table.insert(sets, stamp)
		else
			table.insert(sets, timefield)
			table.insert(sets, stamp)
		end
	end
	if #sets > 0 then
		redis.call('HMSET', KEYS[1], unpack(sets))
		redis.call("EXPIRE", KEYS[1], expire*10) -- 整个key的过期时间
	end

	if publish then
		-- 通知信息样式 desc:第一个[(剩余个数)]
		checkAll(KEYS[1], expire, expireat, ARGV[2] .. ":" .. ARGV[3] .. ((#ARGV > 3) and "..(".. tostring((#ARGV-4)/2) ..")" or "" ))
	end
	return 'OK'
`)

// 更新服务器注册的时间，如果有不存在的服务器，返回-1
// Key 服务器注册发现的key
// ARGV1 RegExprieTime 秒
// ARGV2 描述
// ARGV3 ARGV4... 格式:key..
var updateRegisterScirpt = NewScript(checkAllScirpt + `
	-- 有写入操作，开启复制模式，否则下面的获取时间错误
	redis.replicate_commands()
	-- 读取时间
	local expire = tonumber(ARGV[1])
	local t = redis.call('TIME')
	local stamp = tonumber(t[1]) + tonumber(t[2])/1000000
	local expireat = stamp-expire -- 有效数据的最早注册时间

	local timefields = {}
	for i = 3, #ARGV do
		local field = ARGV[i]
		local timefield = "_inn_:time:" .. field
		table.insert(timefields, timefield)
	end
	if #timefields == 0 then
		return 0
	end

	local times = redis.call('HMGET', KEYS[1], unpack(timefields))
	local settimes = {}

	local publish = false -- 因修改数据导致需要发布
	for i = 1, #times do
		local score = tonumber(times[i])
		if not score then
			return -1 -- 不存在的服务器
		end
		if not (score > expireat) then -- 服务器过期了
			publish = true
		end
		table.insert(settimes, timefields[i])
		table.insert(settimes, stamp)
	end

	redis.call('HMSET', KEYS[1], unpack(settimes))
	redis.call("EXPIRE", KEYS[1], expire*10) -- 整个key的过期时间

	if publish then
		-- 通知信息样式 desc:第一个[(剩余个数)]
		checkAll(KEYS[1], expire, expireat, ARGV[2] .. ":" .. ARGV[3] .. ((#ARGV > 3) and "..(".. tostring((#ARGV-3)) ..")" or "" ))
	end
	return #times
`)

// 取消注册服务器
// Key 服务器注册发现的key，若数据发生变化会向Key命名的channel发送通知
// ARGV1 RegExprieTime 秒
// ARGV2 描述
// ARGV3... 删除的服务器
var deregisterScirpt = NewScript(checkAllScirpt + `
	-- 有写入操作，开启复制模式，否则下面的获取时间错误
	redis.replicate_commands()
	-- 读取时间
	local expire = tonumber(ARGV[1])
	local t = redis.call('TIME')
	local stamp = tonumber(t[1]) + tonumber(t[2])/1000000
	local expireat = stamp-expire -- 有效数据的最早注册时间

	local publish = false -- 是否需要发布
	local slist = {}
	for i = 3, #ARGV do
		-- 判断之前的是否还有效，无效不需要发布
		local field = ARGV[i]
		local timefield = "_inn_:time:" .. field
		if not publish then
			local score = tonumber(redis.call('HGET', KEYS[1], timefield))
			if score and score > expireat then
				publish = true
			end
		end
		table.insert(slist, field)
		table.insert(slist, timefield)
	end
	if #slist > 0 then
		redis.call('HDEL', KEYS[1], unpack(slist))
	end
	
	if publish then
		-- 通知信息样式 desc:第一个[(剩余个数)]
		checkAll(KEYS[1], expire, expireat, ARGV[2] .. ":" .. ARGV[3] .. ((#ARGV > 3) and "..(".. tostring(#ARGV-3) ..")" or "" ))
	end
	return 'OK'
`)

// 检查是否有服务器是否有变化，有变化就发送订阅
// 检查也是抢占时，谁抢到了，谁来负责检查和删除过期的服务器
// Key 服务器注册发现的key
// ARGV1 RegExprieTime 秒
// ARGV2 检查抢占的锁的UUID
var checkServicesScirpt = NewScript(checkAllScirpt + `
	-- 有写入操作，开启复制模式，否则下面的获取时间错误
	redis.replicate_commands()
	-- 读取时间
	local expire = tonumber(ARGV[1])
	local t = redis.call('TIME')
	local stamp = tonumber(t[1]) + tonumber(t[2])/1000000
	local expireat = stamp-expire -- 有效数据的最早注册时间

	-- 先抢占下
	local checkfield = "_inn_:check"
	local checktimefield = "_inn_:checktime"
	local leader = redis.call("HMGET", KEYS[1], checkfield, checktimefield)
	local checkuuid = leader[1]
	local checktime = tonumber(leader[2])
	if not checkuuid then
		redis.call("HSET", KEYS[1], checkfield, ARGV[2], checktimefield, stamp)
	elseif checkuuid == ARGV[2] then
		-- 延期
		redis.call("HSET", KEYS[1], checktimefield, stamp)
	else
		-- 是否过期
		if checktime and checktime > expireat then
			return 0
		end
		-- 过期 重新抢占
		redis.call("HSET", KEYS[1], checkfield, ARGV[2], checktimefield, stamp)
	end

	checkAll(KEYS[1], expire, expireat, "check:" .. ARGV[2])
	return 1
`)

// 读取服务器 有效的服务器
// Key1 服务器注册发现的key，HASH结构
// ARGV1 RegExprieTime 秒
var readRegisterScirpt = NewScript(`
	-- 读取时间
	local expire = tonumber(ARGV[1])
	local t = redis.call('TIME')
	local stamp = tonumber(t[1]) + tonumber(t[2])/1000000
	local expireat = stamp-expire -- 有效数据的最早注册时间

	-- 读取所有服务器
	local rst = redis.call("HGETALL", KEYS[1])

	-- 组织成map
	local smap = {}
	for i = 1, #rst, 2 do
		smap[rst[i]] = rst[i + 1]
	end

	-- 过滤
	local slist = {}        -- 有效服务器
	for field, value in pairs(smap) do
		if string.sub(field, 1, 6) ~= "_inn_:" then
			-- 判断对应的时间是否过期了
			local timefield = "_inn_:time:" .. field
			local score = tonumber(smap[timefield])
			if score and score > expireat then
				table.insert(slist, value)
			end
		end
	end
	return slist
`)

// RegistryInfo 服务注册信息
type RegistryInfo struct {
	// 下面的字段为注册信息，他们会连接起来作为唯一值
	Name   string `json:"name,omitempty"`   // 注册的名字 组名
	ID     string `json:"id,omitempty"`     // 注册的ID 服务器唯一ID
	Addr   string `json:"addr,omitempty"`   // 服务器对外暴露的地址
	Port   int    `json:"port,omitempty"`   // 服务器对外暴露的端口
	Scheme string `json:"scheme,omitempty"` // 服务器使用的协议

	Meta map[string]string `json:"meta,omitempty"` // 元数据 不参与连接
}

func (r *RegistryInfo) Key() string {
	return fmt.Sprintf("%s:%s:%s:%s:%s", r.Name, r.ID, r.Addr, strconv.Itoa(r.Port), r.Scheme)
}

func (r *RegistryInfo) MarshalZerologObject(e *zerolog.Event) {
	if r != nil {
		e.Str("Name", r.Name).
			Str("ID", r.ID).
			Str("Addr", r.Addr).
			Int("Port", r.Port).
			Str("Scheme", r.Scheme)
	}
}

type Register struct {
	// 不可修改
	ctx context.Context
	r   *Redis
	key string

	// 只有
	mu       sync.Mutex
	register map[string][]byte // 注册信息，key为注册信息连接的key，value为元数据

	state int32    // 注册状态 原子操作 0：未注册 1：注册中 2：已注册
	quit  chan int // 退出检查使用
}

// key是按照zset写入,如果key没有hash标签，就添加hash标签
func (r *Redis) CreateRegister(key string, cfg *RegistryInfo) *Register {
	register := &Register{
		ctx:      utils.CtxSetNolog(context.TODO()), // 不要日志
		r:        r,
		key:      key,
		register: make(map[string][]byte),
		state:    0,
		quit:     make(chan int),
	}
	if cfg != nil {
		register.register[cfg.Key()], _ = json.Marshal(cfg)
	}
	return register
}

func (r *Redis) CreateRegisters(key string, cfgs []*RegistryInfo) *Register {
	register := &Register{
		ctx:      utils.CtxSetNolog(context.TODO()), // 不要日志
		r:        r,
		key:      key,
		register: make(map[string][]byte),
		state:    0,
		quit:     make(chan int),
	}
	// 生成注册的value
	for _, cfg := range cfgs {
		if cfg != nil {
			register.register[cfg.Key()], _ = json.Marshal(cfg)
		}
	}
	return register
}

func (r *Register) Add(cfg *RegistryInfo) error {
	if cfg == nil {
		err := errors.New("RegistryInfo is nil")
		log.Error().Err(err).Msg("RedisRegister Add")
		return err
	}
	key := cfg.Key()
	value, _ := json.Marshal(cfg)
	r.mu.Lock()
	defer r.mu.Unlock()
	if v, ok := r.register[key]; ok {
		if bytes.Equal(v, value) {
			return nil // 存在了
		}
	}

	if atomic.LoadInt32(&r.state) != 0 {
		args := []interface{}{RegExprieTime, "add", key, value}
		err := r.add(args)
		if err != nil {
			log.Error().Str("Info", key).Msg("RedisRegister Add")
			return err
		}
	}
	r.register[key] = value

	log.Info().Str("Info", key).Interface("Meta", cfg.Meta).Msg("RedisRegister Add")
	return nil
}

func (r *Register) Remove(cfg *RegistryInfo) error {
	key := cfg.Key()
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.register[key]; ok {
		if atomic.LoadInt32(&r.state) != 0 {
			args := []interface{}{RegExprieTime, "rem", key}
			err := r.rem(args)
			if err != nil {
				log.Error().Str("Info", key).Msg("RedisRegister Remove")
				return err
			}
		}
		delete(r.register, key)
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
		log.Error().Str("Info", r.log()).Msg("RedisRegister already register")
		return nil
	}

	// 先写一次Redis，确保能注册成功
	// 生成注册的value
	if len(r.register) > 0 {
		args := []interface{}{RegExprieTime, "reg"}
		for k, v := range r.register {
			args = append(args, k)
			args = append(args, v)
		}
		err := r.add(args)
		if err != nil {
			atomic.StoreInt32(&r.state, 0)
			return err
		}
	}

	// 开启协程
	go r.loop()
	atomic.StoreInt32(&r.state, 2)

	log.Info().Str("Info", r.log()).Msg("RedisRegister register success")
	return nil
}

func (r *Register) DeReg() error {
	if !atomic.CompareAndSwapInt32(&r.state, 2, 0) {
		log.Error().Str("Info", r.log()).Msg("RedisRegister not register")
		return nil
	}

	r.quit <- 1
	<-r.quit

	log.Info().Str("Info", r.log()).Msg("RedisRegister deregistered")
	return nil
}

func (r *Register) log() string {
	keys := []string{}
	for k := range r.register {
		keys = append(keys, k)
	}
	return strings.Join(keys, ",")
}

func (r *Register) loop() {
	// 定时写Redis
	timer := time.NewTimer(time.Duration(RegExprieTime) / 2 * time.Second)
	for {
		select {
		case <-timer.C:
			args := []interface{}{RegExprieTime, "update"}
			r.mu.Lock()
			for k := range r.register {
				args = append(args, k)
			}
			r.mu.Unlock()
			if len(args) > 2 {
				result, err := r.updateTime(args)
				if err == nil {
					if result == -1 {
						// 如果有服务器不存在，就重新注册
						args := []interface{}{RegExprieTime, "update"}
						r.mu.Lock()
						for k, v := range r.register {
							args = append(args, k)
							args = append(args, v)
						}
						r.mu.Unlock()
						if len(args) > 2 {
							r.add(args)
						}
					}
				}
			}
			timer.Reset(time.Duration(RegExprieTime) / 2 * time.Second) // 重置定时器
		case <-r.quit:
			// 删除写的Redis
			args := []interface{}{RegExprieTime, "quit"}
			r.mu.Lock()
			for k := range r.register {
				args = append(args, k)
			}
			r.mu.Unlock()
			if len(args) > 2 {
				r.rem(args)
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
func (r *Register) add(args []interface{}) error {
	ctx := utils.CtxAddLog(r.ctx, "Register", r.key)
	cmd := r.r.Script(ctx, registerScirpt, []string{r.key}, args...)
	if cmd.Err() != nil {
		// 错误了 在来一次
		cmd = r.r.Script(ctx, registerScirpt, []string{r.key}, args...)
	}
	return cmd.Err()
}

func (r *Register) updateTime(args []interface{}) (int, error) {
	ctx := utils.CtxAddLog(r.ctx, "Register", r.key)
	var result int
	err := r.r.Script(ctx, updateRegisterScirpt, []string{r.key}, args...).Bind(&result)
	if err != nil {
		// 错误了 在来一次
		err = r.r.Script(ctx, updateRegisterScirpt, []string{r.key}, args...).Bind(&result)
	}
	return result, err
}

func (r *Register) rem(args []interface{}) error {
	ctx := utils.CtxAddLog(r.ctx, "Register", r.key)
	cmd := r.r.Script(ctx, deregisterScirpt, []string{r.key}, args...)
	if cmd.Err() != nil {
		// 错误了 在来一次
		cmd = r.r.Script(ctx, deregisterScirpt, []string{r.key}, args...)
	}
	return cmd.Err()
}
