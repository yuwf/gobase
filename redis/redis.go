package redis

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"time"

	"gobase/utils"

	"github.com/rs/zerolog/log"

	"github.com/gomodule/redigo/redis"
)

type Config struct {
	Addr          string `json:"addr,omitempty"` //地址,host:port
	Passwd        string `json:"passwd,omitempty"`
	DB            int    `json:"db,omitempty"`
	SSl           int    `json:"ssl,omitempty"`
	PoolSize      int    `json:"poolsize,omitempty"`      //连接池大小
	MaxActiveConn int    `json:"maxactiveconn,omitempty"` //最大活跃连接数量
}

// 待优化 放到Config中
var (
	redisMaxIdle         = 50
	redisIdleTimeout     = 300
	redisMaxConnLifetime = 3600
)

var defaultRedis *Redis

type Redis struct {
	pool *redis.Pool

	// 执行命令时的回调 不使用锁，默认要求提前注册好 管道部分待完善
	hook []func(ctx context.Context, cmd *RedisCommond)
}

func DefaultRedis() *Redis {
	return defaultRedis
}

func InitDefaultRedis(cfg *Config) (*Redis, error) {
	var err error
	defaultRedis, err = NewRedis(cfg)
	return defaultRedis, err
}

func NewRedis(cfg *Config) (*Redis, error) {
	// 创建一个pool
	pool := &redis.Pool{
		MaxIdle:         redisMaxIdle,
		MaxActive:       cfg.MaxActiveConn,
		Wait:            true,
		IdleTimeout:     time.Duration(redisIdleTimeout) * time.Second,
		MaxConnLifetime: time.Duration(redisMaxConnLifetime) * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", cfg.Addr,
				redis.DialPassword(cfg.Passwd),
				redis.DialDatabase(cfg.DB),
				redis.DialConnectTimeout(4*time.Second),
				redis.DialReadTimeout(4*time.Second),
				redis.DialWriteTimeout(4*time.Second),
				redis.DialUseTLS(cfg.SSl == 1),
			)

			if err != nil {
				log.Error().Err(err).Str("addr", cfg.Addr).Str("passwd", cfg.Passwd).Int("db", cfg.DB).Int("ssl", cfg.SSl).Msg("Redis Dial Fail")
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	// 测试一条连接
	conn := pool.Get()
	if nil != conn.Err() {
		log.Error().Err(conn.Err()).Str("addr", cfg.Addr).Str("passwd", cfg.Passwd).Int("db", cfg.DB).Int("ssl", cfg.SSl).Msg("Redis Conn Fail")
		return nil, conn.Err()
	}
	defer conn.Close()

	log.Info().Str("addr", cfg.Addr).Str("passwd", cfg.Passwd).Int("db", cfg.DB).Int("ssl", cfg.SSl).Msg("Redis Conn Success")

	r := &Redis{}
	r.pool = pool
	return r, nil
}

// Pool 暴露原始对象
func (r *Redis) Pool() *redis.Pool {
	return r.pool
}

func (r *Redis) RegHook(f func(ctx context.Context, cmd *RedisCommond)) {
	r.hook = append(r.hook, f)
}

func (r *Redis) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	redisCmd := r.do(ctx, cmd, args...)
	return redisCmd.Reply, redisCmd.Err
}

func (r *Redis) Do2(ctx context.Context, cmd string, args ...interface{}) RedisResultBind {
	return r.do(ctx, cmd, args...)
}

//do 执行命令
func (r *Redis) do(ctx context.Context, cmd string, args ...interface{}) *RedisCommond {
	redisCmd := &RedisCommond{
		Cmd:  cmd,
		Args: args,
	}
	if ctx != nil {
		caller := ctx.Value("caller")
		if caller != nil {
			redisCmd.Caller, _ = caller.(*utils.CallerDesc)
		}
	}
	if redisCmd.Caller == nil {
		redisCmd.Caller = utils.GetCallerDesc(2)
	}
	r.docmd(ctx, redisCmd)
	return redisCmd
}

func (r *Redis) docmd(ctx context.Context, redisCmd *RedisCommond) {
	if r.pool == nil {
		log.Error().Str("pos", redisCmd.Caller.Pos()).Msg("Redis pool is nil")
		redisCmd.Err = errors.New("Redis pool is nil")
		return
	}

	if len(redisCmd.CmdDesc) == 0 && ctx != nil {
		cmddesc := ctx.Value("cmddesc")
		if cmddesc != nil {
			redisCmd.CmdDesc, _ = cmddesc.(string)
		}
	}

	entry := time.Now()
	con := r.pool.Get()
	defer con.Close()
	redisCmd.Reply, redisCmd.Err = con.Do(redisCmd.Cmd, redisCmd.Args...)
	redisCmd.Elapsed = time.Since(entry)

	if redisCmd.Err != nil {
		log.Error().Err(redisCmd.Err).Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Msg("Redis cmd fail")
	} else {
		logOut := true
		if ctx != nil {
			nolog := ctx.Value("nolog")
			if nolog != nil {
				logOut = false
			}
		}
		if logOut {
			log.Debug().Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
				Str("cmd", redisCmd.CmdString()).
				Str("pos", redisCmd.Caller.Pos()).
				Str("reply", redisCmd.ReplyString()).
				Msg("Redis cmd success")
		}
	}

	// 回调
	for _, f := range r.hook {
		f(ctx, redisCmd)
	}
}

func (r *Redis) DoCmdInt(ctx context.Context, cmd string, args ...interface{}) (int, error) {
	var result int
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Int(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdInt fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdInt64(ctx context.Context, cmd string, args ...interface{}) (int64, error) {
	var result int64
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Int64(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdInt64 fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdUint64(ctx context.Context, cmd string, args ...interface{}) (uint64, error) {
	var result uint64
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Uint64(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdUint64 fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdFloat64(ctx context.Context, cmd string, args ...interface{}) (float64, error) {
	var result float64
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Float64(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdFloat64 fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdString(ctx context.Context, cmd string, args ...interface{}) (string, error) {
	var result string
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.String(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdString fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdBytes(ctx context.Context, cmd string, args ...interface{}) ([]byte, error) {
	var result []byte
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Bytes(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdBytes fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdBool(ctx context.Context, cmd string, args ...interface{}) (bool, error) {
	var result bool
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Bool(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdBool fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdFloat64s(ctx context.Context, cmd string, args ...interface{}) ([]float64, error) {
	var result []float64
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Float64s(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdFloat64s fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdStrings(ctx context.Context, cmd string, args ...interface{}) ([]string, error) {
	var result []string
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Strings(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdStrings fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdByteSlices(ctx context.Context, cmd string, args ...interface{}) ([][]byte, error) {
	var result [][]byte
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.ByteSlices(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdByteSlices fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdInt64s(ctx context.Context, cmd string, args ...interface{}) ([]int64, error) {
	var result []int64
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Int64s(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdInt64s fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdInts(ctx context.Context, cmd string, args ...interface{}) ([]int, error) {
	var result []int
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Ints(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdInts fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdStringMap(ctx context.Context, cmd string, args ...interface{}) (map[string]string, error) {
	var result map[string]string
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.StringMap(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdStringMap fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdIntMap(ctx context.Context, cmd string, args ...interface{}) (map[string]int, error) {
	var result map[string]int
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.IntMap(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdIntMap fail")
	}
	return result, redisCmd.Err
}

func (r *Redis) DoCmdInt64Map(ctx context.Context, cmd string, args ...interface{}) (map[string]int64, error) {
	var result map[string]int64
	redisCmd := r.do(ctx, cmd, args...)
	if redisCmd.Err != nil {
		return result, redisCmd.Err
	}
	result, redisCmd.Err = redis.Int64Map(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil && redisCmd.Err != redis.ErrNil {
		log.Error().Err(redisCmd.Err).
			Str("cmd", redisCmd.CmdString()).
			Str("pos", redisCmd.Caller.Pos()).
			Str("reply", redisCmd.ReplyString()).
			Msg("Redis DoCmdInt64Map fail")
	}
	return result, redisCmd.Err
}

// 参数v 参考RedisResultBind.HMGetBindObj的说明
func (r *Redis) HMGetObj(ctx context.Context, key string, v interface{}) error {
	redisCmd := &RedisCommond{
		Cmd:    "HMGET",
		Caller: utils.GetCallerDesc(1),
	}
	// 组织参数
	redisCmd.Args = append(redisCmd.Args, key)
	err := redisCmd.HMGetBindObj(v)
	if err != nil {
		return err
	}

	r.docmd(ctx, redisCmd)
	if redisCmd.Err != nil {
		return redisCmd.Err
	}
	// 回调
	redisCmd.Err = redisCmd.callback(redisCmd.Reply)
	if err != nil {
		log.Error().Err(redisCmd.Err).Str("cmd", redisCmd.CmdString()).Str("pos", redisCmd.Caller.Pos()).Msg("Redis HMGetObj Bind Error")
	}
	return redisCmd.Err
}

// 参数v 参考RedisResultBind.HMGetBindObj的说明
func (r *Redis) HMSetObj(ctx context.Context, key string, v interface{}) error {
	redisCmd := &RedisCommond{
		Cmd:    "HMSET",
		Caller: utils.GetCallerDesc(1),
	}
	redisCmd.Args = append(redisCmd.Args, key)
	redisCmd.Err = redisCmd._HMSetObjArgs(v)
	if redisCmd.Err != nil {
		log.Error().Str("cmd", redisCmd.CmdString()).Str("pos", redisCmd.Caller.Pos()).Msg("Redis HMSetObj Param error")
		return redisCmd.Err
	}

	r.docmd(ctx, redisCmd)
	return redisCmd.Err
}
