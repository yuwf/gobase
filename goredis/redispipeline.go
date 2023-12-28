package goredis

// https://github.com/yuwf/gobase

import (
	"context"

	"github.com/yuwf/gobase/utils"

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

// 参数v 参考Redis.HMGetObj的说明
func (p *RedisPipeline) HMGetObj(ctx context.Context, key string, v interface{}) error {
	redisCmd := &RedisCommond{
		Caller: utils.GetCallerDesc(1),
	}
	// 组织参数
	fargs, elemts, structtype, err := hmgetObjArgs(v)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Str("pos", redisCmd.Caller.Pos()).Msg("RedisPipeline HMGetObj Param error")
		return err
	}
	redisCmd.hmgetCallback(elemts, structtype) // 管道里这个不会返回错误

	args := []interface{}{"hmget", key}
	args = append(args, fargs...)
	cmd := p.Pipeliner.Do(context.WithValue(ctx, CtxKey_rediscmd, redisCmd), args...)
	return cmd.Err()
}

// 参数v 参考Redis.HMGetObj的说明
func (p *RedisPipeline) HMSetObj(ctx context.Context, key string, v interface{}) error {
	caller := utils.GetCallerDesc(1)
	// 组织参数
	fargs, err := hmsetObjArgs(v)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Str("pos", caller.Pos()).Msg("RedisPipeline HMSetObj Param error")
		return err
	}
	args := []interface{}{"hmset", key}
	args = append(args, fargs...)
	cmd := p.Pipeliner.Do(context.WithValue(ctx, CtxKey_caller, caller), args...)
	return cmd.Err()
}
