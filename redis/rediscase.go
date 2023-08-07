package redis

import (
	"context"

	"github.com/gomodule/redigo/redis"
)

// 具体命令接口,每个工程自己定制
func IsNilError(err error) bool {
	return err == redis.ErrNil
}

func (r *Redis) HasKey(ctx context.Context, key string) bool {
	redisCmd := r.do(ctx, "EXISTS", key)
	res, err := redis.Int(redisCmd.Reply, redisCmd.Err)
	if err != nil {
		return false
	}
	return res == 1
}
