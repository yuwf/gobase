package backend

// https://github.com/yuwf/gobase

import (
	"github.com/yuwf/gobase/consul"
	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/redis"
)

type HttpEvent[T any] interface {
	// consul服务器配置过滤器，返回符合条件的服务器
	ConsulFilter(confs []*consul.RegistryInfo) []*ServiceConfig

	// redis服务器配置过滤器，返回符合条件的服务器
	RedisFilter(confs []*redis.RegistryInfo) []*ServiceConfig

	// goredis服务器配置过滤器，返回符合条件的服务器
	GoRedisFilter(confs []*goredis.RegistryInfo) []*ServiceConfig
}

// HttpEventHandler HttpEvent的内置实现
// 如果不想实现HttpEvent的所有接口，可以继承它实现部分方法
type HttpEventHandler[T any] struct {
}

func (*HttpEventHandler[T]) ConsulFilter(confs []*consul.RegistryInfo) []*ServiceConfig {
	return []*ServiceConfig{}
}
func (*HttpEventHandler[T]) RedisFilter(confs []*redis.RegistryInfo) []*ServiceConfig {
	return []*ServiceConfig{}
}
func (*HttpEventHandler[T]) GoRedisFilter(confs []*goredis.RegistryInfo) []*ServiceConfig {
	return []*ServiceConfig{}
}

// Hook
type HttpHook[T any] interface {
	// 添加服务器
	OnAdd(ts *HttpService[T])
	// 移除一个服务器，彻底移除
	OnRemove(ts *HttpService[T])

	// 连接成功
	OnConnected(ts *HttpService[T])
	// 连接掉线
	OnDisConnect(ts *HttpService[T])
}
