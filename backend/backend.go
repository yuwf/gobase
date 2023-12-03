package backend

// https://github.com/yuwf/gobase

import (
	"gobase/consul"
	"gobase/goredis"
	"gobase/redis"
)

// 服务配置
type ServiceConfig struct {
	// 要求字符串类型的字段小写且去掉前后空格
	ServiceName string `json:"servicename,omitempty"` // 服务器类型名，用来分组【内部会转化成去空格的小写】
	ServiceId   string `json:"serviceid,omitempty"`   // 服务器唯一ID【内部会转化成去空格的小写】
	ServiceAddr string `json:"serviceaddr,omitempty"` // 服务器对外暴露的地址
	ServicePort int    `json:"serviceport,omitempty"` // 服务器对外暴露的端口
	RoutingTag  string `json:"routertag,omitempty"`   // 支持路由tag group内再次进行分组【内部会转化成去空格的小写】
}
type ServiceIdConfMap = map[string]*ServiceConfig
type ServiceNameConfMap = map[string]ServiceIdConfMap

// 创建TcpBackend，使用Consul做服务器发现
func NewTcpBackendWithConsul[T any](consulAddr, tag string, event TcpEvent[T]) (*TcpBackend[T], error) {
	watcher, err := consul.CreateClient(consulAddr, "http")
	if err != nil {
		return nil, err
	}
	tb := &TcpBackend[T]{
		group:   map[string]*TcpGroup[T]{},
		event:   event,
		watcher: watcher,
	}
	// 服务发现部分
	watcher.WatchServices(tag, func(infos []*consul.RegistryInfo) {
		if tb.event != nil {
			confs := tb.event.ConsulFilter(infos)
			tb.updateServices(confs)
		}
	})
	return tb, nil
}

// 创建TcpBackend，使用Redis做服务器发现
// key 表示服务器发现的key
// serverName 表示监听哪些服务器 为空表示监听全部的服务器
func NewTcpBackendWithRedis[T any](cfg *redis.Config, key string, serverNames []string, event TcpEvent[T]) (*TcpBackend[T], error) {
	watcher, err := redis.NewRedis(cfg)
	if err != nil {
		return nil, err
	}
	tb := &TcpBackend[T]{
		group:   map[string]*TcpGroup[T]{},
		event:   event,
		watcher: watcher,
	}
	// 服务发现部分
	watcher.WatchServices(key, serverNames, func(infos []*redis.RegistryInfo) {
		if tb.event != nil {
			confs := tb.event.RedisFilter(infos)
			tb.updateServices(confs)
		}
	})
	return tb, nil
}
func NewTcpBackendWithGoRedis[T any](cfg *goredis.Config, key string, serverNames []string, event TcpEvent[T]) (*TcpBackend[T], error) {
	watcher, err := goredis.NewRedis(cfg)
	if err != nil {
		return nil, err
	}
	tb := &TcpBackend[T]{
		group:   map[string]*TcpGroup[T]{},
		event:   event,
		watcher: watcher,
	}
	// 服务发现部分
	watcher.WatchServices(key, serverNames, func(infos []*goredis.RegistryInfo) {
		if tb.event != nil {
			confs := tb.event.GoRedisFilter(infos)
			tb.updateServices(confs)
		}
	})
	return tb, nil
}

// 创建HttpBackend，使用Redis做服务器发现
func NewHttpBackendWithConsul[T any](consulAddr, tag string, event HttpEvent[T]) (*HttpBackend[T], error) {
	watcher, err := consul.CreateClient(consulAddr, "http")
	if err != nil {
		return nil, err
	}
	hb := &HttpBackend[T]{
		group:   map[string]*HttpGroup[T]{},
		event:   event,
		watcher: watcher,
	}
	// 服务发现部分
	watcher.WatchServices(tag, func(infos []*consul.RegistryInfo) {
		if hb.event != nil {
			confs := hb.event.ConsulFilter(infos)
			hb.updateServices(confs)
		}
	})
	return hb, nil
}

// 创建HttpBackend，使用Redis做服务器发现
// key 表示服务器发现的key
// serverName 表示监听哪些服务器 为空表示监听全部的服务器
func NewHttpBackendWithRedis[T any](cfg *redis.Config, key string, serverNames []string, event HttpEvent[T]) (*HttpBackend[T], error) {
	watcher, err := redis.NewRedis(cfg)
	if err != nil {
		return nil, err
	}
	hb := &HttpBackend[T]{
		group:   map[string]*HttpGroup[T]{},
		event:   event,
		watcher: watcher,
	}
	// 服务发现部分
	watcher.WatchServices(key, serverNames, func(infos []*redis.RegistryInfo) {
		if hb.event != nil {
			confs := hb.event.RedisFilter(infos)
			hb.updateServices(confs)
		}
	})
	return hb, nil
}
func NewHttpBackendWithGoRedis[T any](cfg *goredis.Config, key string, serverNames []string, event HttpEvent[T]) (*HttpBackend[T], error) {
	watcher, err := goredis.NewRedis(cfg)
	if err != nil {
		return nil, err
	}
	hb := &HttpBackend[T]{
		group:   map[string]*HttpGroup[T]{},
		event:   event,
		watcher: watcher,
	}
	// 服务发现部分
	watcher.WatchServices(key, serverNames, func(infos []*goredis.RegistryInfo) {
		if hb.event != nil {
			confs := hb.event.GoRedisFilter(infos)
			hb.updateServices(confs)
		}
	})
	return hb, nil
}
