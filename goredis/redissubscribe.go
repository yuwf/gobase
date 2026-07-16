package goredis

// https://github.com/yuwf/gobase

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// 过度下redis的订阅对象，方便后期的扩展
// 【注意，PubSub在低版本不能跨分片订阅的，这个我理解的还是不够透彻】
type Subscribe struct {
	*redis.PubSub        // 支持redis断线重连 Receive系列函数会阻塞等待消息
	r             *Redis // Redis对象
}

// 创建一个订阅对象
func (r *Redis) CreateSubscribe(ctx context.Context) *Subscribe {
	return &Subscribe{
		PubSub: r.Subscribe(ctx),
		r:      r,
	}
}
