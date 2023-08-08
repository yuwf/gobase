package goredis

// https://github.com/yuwf/gobase

import (
	"context"

	"gobase/utils"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// 支持绑定的管道
type RedisPipeline struct {
	redis.Pipeliner
}

func (r *Redis) NewPipeline() *RedisPipeline {
	pipeline := &RedisPipeline{
		Pipeliner: r.Pipeline(),
	}
	return pipeline
}

// 统一的命令
func (p *RedisPipeline) Cmd(ctx context.Context, args ...interface{}) RedisResultBind {
	redisCmd := &RedisCommond{
		Caller: utils.GetCallerDesc(1),
	}
	p.Pipeliner.Do(context.WithValue(ctx, CtxKey_rediscmd, redisCmd), args...)
	return redisCmd
}

// 此函数提交的管道命令，不会产生nil的错误
func (p *RedisPipeline) ExecNoNil(ctx context.Context) ([]redis.Cmder, error) {
	return p.Pipeliner.Exec(context.WithValue(ctx, CtxKey_nonilerr, 1))
}

// 参数v 参考RedisResultBind.HMGetBindObj的说明
func (p *RedisPipeline) HMGetObj(ctx context.Context, key string, v interface{}) error {
	redisCmd := &RedisCommond{
		Caller: utils.GetCallerDesc(1),
	}
	// 组织参数
	args := []interface{}{"hmget", key}
	args, elemts, structtype, err := hmgetObjArgs(args, v)
	if err != nil {
		return err
	}
	redisCmd.hmgetCallback(elemts, structtype) // 管道里这个不会返回错误

	cmd := p.Pipeliner.Do(context.WithValue(ctx, CtxKey_rediscmd, redisCmd), args...)
	return cmd.Err()
}

// 参数v 参考RedisResultBind.HMGetBindObj的说明
func (p *RedisPipeline) HMSetObj(ctx context.Context, key string, v interface{}) error {
	caller := utils.GetCallerDesc(1)
	// 组织参数
	args := []interface{}{"hmset", key}
	args, err := hmsetObjArgs(args, v)
	if err != nil {
		log.Error().Err(err).Str("pos", caller.Pos()).Msg("RedisPipeline HMSetObj Param error")
		return err
	}
	cmd := p.Pipeliner.Do(context.WithValue(ctx, CtxKey_caller, caller), args...)
	return cmd.Err()
}
