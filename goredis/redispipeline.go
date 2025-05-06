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
	r *Redis
}

func (r *Redis) NewPipeline() *RedisPipeline {
	pipeline := &RedisPipeline{
		Pipeliner: r.Pipeline(),
		r:         r,
	}
	return pipeline
}

// 统一的命令
func (p *RedisPipeline) Cmd(ctx context.Context, args ...interface{}) *RedisCommond {
	ctx = context.WithValue(utils.CtxCaller(ctx, 1), CtxKey_addcaller, 1)
	redisCmd := &RedisCommond{
		ctx: ctx,
	}
	p.Pipeliner.Do(context.WithValue(ctx, CtxKey_rediscmd, redisCmd), args...)
	return redisCmd
}

func (p *RedisPipeline) Do2(ctx context.Context, args ...interface{}) *RedisCommond {
	ctx = context.WithValue(utils.CtxCaller(ctx, 1), CtxKey_addcaller, 1)
	redisCmd := &RedisCommond{
		ctx: ctx,
	}
	p.Pipeliner.Do(context.WithValue(ctx, CtxKey_rediscmd, redisCmd), args...)
	return redisCmd
}

// 此函数提交的管道命令，不会产生nil的错误
func (p *RedisPipeline) ExecNoNil(ctx context.Context) ([]redis.Cmder, error) {
	return p.Pipeliner.Exec(context.WithValue(ctx, CtxKey_nonilerr, 1))
}

// 管道结合脚本，管道先按evalsha执行，管道中所有命令执行完之后，如果有脚本未加载的错误就再执行一次，所以这种管道无法保证命令顺序
func (p *RedisPipeline) Script(ctx context.Context, script *RedisScript, keys []string, args ...interface{}) *redis.Cmd {
	ctx = context.WithValue(utils.CtxCaller(ctx, 1), CtxKey_addcaller, 1)
	redisCmd := &RedisCommond{
		ctx: ctx,
	}
	redisCmd.nscallback = func() *redis.Cmd {
		// 直接同步调用
		return script.script.Eval(ctx, p.r, keys, args...)
	}

	param := make([]interface{}, 0, 3+len(keys)+len(args))
	param = append(param, "evalsha")
	param = append(param, script.script.Hash())
	param = append(param, len(keys))
	for _, key := range keys {
		param = append(param, key)
	}
	param = append(param, args...)

	return p.Pipeliner.Do(context.WithValue(ctx, CtxKey_rediscmd, redisCmd), param...)
}

func (p *RedisPipeline) Script2(ctx context.Context, script *RedisScript, keys []string, args ...interface{}) *RedisCommond {
	ctx = context.WithValue(utils.CtxCaller(ctx, 1), CtxKey_addcaller, 1)
	redisCmd := &RedisCommond{
		ctx: ctx,
	}
	redisCmd.nscallback = func() *redis.Cmd {
		// 直接同步调用
		return script.script.Eval(ctx, p.r, keys, args...)
	}

	param := make([]interface{}, 0, 3+len(keys)+len(args))
	param = append(param, "evalsha")
	param = append(param, script.script.Hash())
	param = append(param, len(keys))
	for _, key := range keys {
		param = append(param, key)
	}
	param = append(param, args...)

	p.Pipeliner.Do(context.WithValue(ctx, CtxKey_rediscmd, redisCmd), param...)
	return redisCmd
}

// 参数v 参考Redis.HMGetObj的说明
func (p *RedisPipeline) HMGetObj(ctx context.Context, key string, v interface{}) error {
	ctx = context.WithValue(utils.CtxCaller(ctx, 1), CtxKey_addcaller, 1)
	redisCmd := &RedisCommond{
		ctx: ctx,
	}
	// 获取结构数据
	sInfo, err := utils.GetStructInfoByTag(v, RedisTag)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("RedisPipeline HMSetObj Param error")
		return err
	}
	if len(sInfo.Tags) == 0 {
		return nil // 没有值要读取，直接返回
	}

	redisCmd.BindValues(sInfo.Elemts) // 管道里这个不会返回错误

	args := []interface{}{"hmget", key}
	args = append(args, sInfo.TagsSlice()...)
	cmd := p.Pipeliner.Do(context.WithValue(ctx, CtxKey_rediscmd, redisCmd), args...)
	return cmd.Err()
}

// 参数v 参考Redis.HMGetObj的说明
func (p *RedisPipeline) HMSetObj(ctx context.Context, key string, v interface{}) error {
	ctx = context.WithValue(utils.CtxCaller(ctx, 1), CtxKey_addcaller, 1)
	sInfo, err := utils.GetStructInfoByTag(v, RedisTag)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("RedisPipeline HMSetObj Param error")
		return err
	}
	fargs := TagElemtNoNilFmt(sInfo)
	if len(fargs) == 0 {
		return nil // 没有值写入，直接返回
	}
	// 组织参数
	args := []interface{}{"hmset", key}
	args = append(args, fargs...)
	cmd := p.Pipeliner.Do(ctx, args...)
	return cmd.Err()
}
