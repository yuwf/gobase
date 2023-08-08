package goredis

// https://github.com/yuwf/gobase

import (
	"context"

	"gobase/utils"

	"github.com/redis/go-redis/v9"
)

type RedisScript struct {
	name   string
	script *redis.Script
}

func NewScript(src string) *RedisScript {
	ret := &RedisScript{
		script: redis.NewScript(src),
	}
	return ret
}

func NewScriptWithName(name, src string) *RedisScript {
	ret := &RedisScript{
		name:   name,
		script: redis.NewScript(src),
	}
	return ret
}

func (r *Redis) DoScript(ctx context.Context, script *RedisScript, keys []string, args ...interface{}) *redis.Cmd {
	ctx = context.WithValue(ctx, CtxKey_noscript, 1) // 屏蔽NOSCRIPT的错误日志
	return script.script.Run(ctx, r.UniversalClient, keys, args...)
}

func (r *Redis) DoScript2(ctx context.Context, script *RedisScript, keys []string, args ...interface{}) RedisResultBind {
	redisCmd := &RedisCommond{
		Caller: utils.GetCallerDesc(1),
	}
	ctx = context.WithValue(ctx, CtxKey_noscript, 1) // 屏蔽NOSCRIPT的错误日志
	script.script.Run(context.WithValue(ctx, CtxKey_rediscmd, redisCmd), r.UniversalClient, keys, args...)
	return redisCmd
}
