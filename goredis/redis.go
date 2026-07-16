package goredis

// https://github.com/yuwf/gobase

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"strings"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// GET HGET LPOP RPOP SPOP 会返回空错误
// lua脚本没有返回值也是redis.Nil
// Bind相关 也会可能会返回redis.Nil
// redis.Nil错误，内部不会按错误输出日志
func IsNil(err error) bool {
	return err == redis.Nil
}

type Config struct {
	Master string   `json:"master,omitempty"` // 不为空就创建哨兵模式的连接
	Addrs  []string `json:"addrs,omitempty"`  // host:port 地址数<=1 创建单节点连接 否则创建多节点
	Passwd string   `json:"passwd,omitempty"` // 秘钥
	DB     int      `json:"db,omitempty"`     // 只有单节点模式使用
	TSL    bool     `json:"tsl,omitempty"`    // 是否使用TSL连接
}

// Redis对象
type Redis struct {
	redis.UniversalClient

	// 执行命令时的回调 不使用锁，默认要求提前注册好
	hook []func(ctx context.Context, cmd redis.Cmder, cmds []redis.Cmder, elapsed time.Duration)
}

var defaultRedis *Redis

func DefaultRedis() *Redis {
	return defaultRedis
}

func InitDefaultRedis(cfg *Config) (*Redis, error) {
	var err error
	defaultRedis, err = NewRedis(cfg)
	return defaultRedis, err
}

func NewRedis(cfg *Config) (*Redis, error) {
	//参考 redis.Options 说明
	options := &redis.UniversalOptions{
		MasterName: cfg.Master,
		Addrs:      cfg.Addrs,

		//钩子函数
		//仅当客户端执行命令需要从连接池获取连接时，如果连接池需要新建连接则会调用此钩子函数
		OnConnect: func(ctx context.Context, conn *redis.Conn) error {
			return nil
		},

		Password: cfg.Passwd, //密码
		DB:       cfg.DB,     // redis数据库

		//命令执行失败时的重试策略
		MaxRetries:      3,                      // 命令执行失败时，最多重试多少次, -1表示不重试 0表示重试3次
		MinRetryBackoff: 8 * time.Millisecond,   //每次计算重试间隔时间的下限，默认8毫秒，-1表示取消间隔
		MaxRetryBackoff: 512 * time.Millisecond, //每次计算重试间隔时间的上限，默认512毫秒，-1表示取消间隔

		//超时
		DialTimeout:  4 * time.Second, //连接建立超时时间
		ReadTimeout:  3 * time.Second, //读超时，默认3秒， -1表示取消读超时
		WriteTimeout: 3 * time.Second, //写超时，默认等于读超时

		//连接池设置
		PoolFIFO:     true,            // 连接池使用FIFO管理
		PoolSize:     0,               // 连接池最大socket连接数，默认为10倍CPU数， 10 * runtime.NumCPU
		PoolTimeout:  4 * time.Second, //当所有连接都处在繁忙状态时，客户端等待可用连接的最大等待时长，默认为读超时+1秒。
		MinIdleConns: 10,              //在启动阶段创建指定数量的Idle连接，并长期维持idle状态的连接数不少于指定数量
		//MaxIdleConns: 256, // 最大的空闲连接数
		//ConnMaxIdleTime: time.Minute, // 空闲的最大时间
		//ConnMaxLifetime
	}
	if cfg.TSL {
		options.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	client := redis.NewUniversalClient(options)
	r := &Redis{UniversalClient: client}

	// 测试连接
	cmd := client.Ping(context.TODO())
	if cmd.Err() != nil {
		client.Close()
		log.Error().Err(cmd.Err()).Str("addr", strings.Join(cfg.Addrs, ",")).Str("passwd", cfg.Passwd).Int("db", cfg.DB).Bool("tsl", cfg.TSL).Msg("Redis Conn Fail")
		return nil, cmd.Err()
	}
	client.AddHook(&hook{redis: r})

	log.Info().Str("addr", strings.Join(cfg.Addrs, ",")).Str("passwd", cfg.Passwd).Int("db", cfg.DB).Bool("tsl", cfg.TSL).Msg("Redis Conn Success")
	return r, nil
}

func (r *Redis) RegHook(f func(ctx context.Context, cmd redis.Cmder, cmds []redis.Cmder, elapsed time.Duration)) {
	r.hook = append(r.hook, f)
}

// 支持返回值绑定的函数
func (r *Redis) Cmd(ctx context.Context, args ...interface{}) *RedisCommond {
	cmd := r.Do(ctx, args...)
	return &RedisCommond{
		Cmd:       cmd,
		ctx:       ctx,
		processed: true,
	}
}

func (r *Redis) Script(ctx context.Context, script *RedisScript, keys []string, args ...interface{}) *RedisCommond {
	cmd := script.script.Run(ctx, r.UniversalClient, keys, args...)
	return &RedisCommond{
		Cmd:       cmd,
		ctx:       ctx,
		processed: true,
	}
}

// 针对HMGET命令 调用Cmd时，参数不需要包括field
// 结构成员首字母需要大写，tag中必须是包含 `redis:"hello"`  其中hello就表示在redis中存储的field名称
// 结构成员类型 : Bool, Int, Int8, Int16, Int32, Int64, Uint, Uint8, Uint16, Uint32, Uint64, Uintptr, Float32, Float64, String, []byte
// 结构成员其他类型 : 通过Json转化
// 传入的参数为结构的地址
func (r *Redis) HMGetObj(ctx context.Context, key string, v interface{}) error {
	// 获取结构数据
	sInfo, err := utils.GetStructInfoByTag(v, RedisTag)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Redis HMSetObj Param error")
		return err
	}
	if len(sInfo.Tags) == 0 {
		return nil // 没有值要读取，直接返回
	}

	args := []interface{}{"hmget", key}
	args = append(args, sInfo.TagsSlice()...)
	rst := r.Do(ctx, args...)
	if rst.Err() != nil {
		return rst.Err()
	}
	// 绑定返回值
	return ReplyToValues(rst.Val(), sInfo.Elemts)
}

// 参数v 参考Redis.HMGetObj的说明
func (r *Redis) HMSetObj(ctx context.Context, key string, v interface{}) error {
	sInfo, err := utils.GetStructInfoByTag(v, RedisTag)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Redis HMSetObj Param error")
		return err
	}
	fargs := TagElemtNoNilFmt(sInfo)
	if len(fargs) == 0 {
		return nil // 没有值写入，直接返回
	}
	args := []interface{}{"hmset", key}
	args = append(args, fargs...)
	rst := r.Do(ctx, args...)
	return rst.Err()
}

func (r *Redis) SetJson(ctx context.Context, key string, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Redis SetJson Param error")
		return err
	}
	args := []interface{}{"set", key, b}
	rst := r.Do(ctx, args...)
	return rst.Err()
}

func (r *Redis) HSetJson(ctx context.Context, key, field string, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("Redis SetJson Param error")
		return err
	}
	args := []interface{}{"hset", key, field, b}
	rst := r.Do(ctx, args...)
	return rst.Err()
}
