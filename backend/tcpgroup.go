package backend

// https://github.com/yuwf/gobase

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"
	"github.com/yuwf/gobase/utils"
	"stathat.com/c/consistent"
)

// 功能相同的一组服务器
// T是和业务相关的客户端信息结构 透传给TcpService
type TcpGroup[T any] struct {
	sync.RWMutex
	tb          *TcpBackend[T]            // 上层对象
	serviceName string                    // 不可修改
	services    map[string]*TcpService[T] // 锁保护
	// 线程安全
	hashring    *consistent.Consistent // 哈希环，填充serviceId, 协程安全，交给TcpService来填充，他有连接和熔断策略
	tagHashring *sync.Map              // 按tag分组的哈希环 [tag:*consistent.Consistent]
}

func NewTcpGroup[T any](serviceName string, tb *TcpBackend[T]) *TcpGroup[T] {
	g := &TcpGroup[T]{
		tb:          tb,
		serviceName: serviceName,
		services:    map[string]*TcpService[T]{},
		hashring:    consistent.New(),
		tagHashring: new(sync.Map),
	}
	return g
}

func (g *TcpGroup[T]) GetService(serviceId string) *TcpService[T] {
	serviceId = strings.ToLower(serviceId)
	serviceId = strings.TrimSpace(serviceId)
	g.RLock()
	defer g.RUnlock()
	service, ok := g.services[serviceId]
	if ok {
		return service
	}
	return nil
}

func (g *TcpGroup[T]) GetServices() map[string]*TcpService[T] {
	g.RLock()
	defer g.RUnlock()
	ss := map[string]*TcpService[T]{}
	for serviceId, service := range g.services {
		ss[serviceId] = service
	}
	return ss
}

// 根据哈希环获取对象 hash可以用用户id或者其他稳定的数据
func (g *TcpGroup[T]) GetServiceByHash(hash string) *TcpService[T] {
	serviceId, err := g.hashring.Get(hash)
	if err != nil {
		return nil
	}
	g.RLock()
	defer g.RUnlock()
	service, ok := g.services[serviceId]
	if ok {
		return service
	}
	return nil
}

// 根据tag和哈希环获取对象 hash可以用用户id或者其他稳定的数据
func (g *TcpGroup[T]) GetServiceByTagAndHash(tag, hash string) *TcpService[T] {
	tag = strings.TrimSpace(strings.ToLower(tag))
	hasrhing, ok := g.tagHashring.Load(tag)
	if !ok {
		return nil
	}
	serviceId, err := hasrhing.(*consistent.Consistent).Get(hash)
	if err != nil {
		return nil
	}
	g.RLock()
	defer g.RUnlock()
	service, ok := g.services[serviceId]
	if ok {
		return service
	}
	return nil
}

// 广播消息
func (g *TcpGroup[T]) Broad(ctx context.Context, buf []byte) {
	ss := g.GetServices()
	for _, service := range ss {
		service.Send(ctx, buf)
	}
}

func (g *TcpGroup[T]) BroadMsg(ctx context.Context, msg utils.SendMsger) {
	ss := g.GetServices()
	for _, service := range ss {
		service.SendMsg(ctx, msg)
	}
}

// 更新组，返回剩余个数
func (g *TcpGroup[T]) update(serviceConfs ServiceIdConfMap) int {
	var remove []*TcpService[T]
	var add []*TcpService[T]

	g.Lock()
	// 先删除不存在的
	for serviceId, service := range g.services {
		_, ok := serviceConfs[serviceId]
		if !ok {
			l := log.Info().Str("ServiceName", service.conf.ServiceName).
				Str("ServiceId", service.conf.ServiceId).
				Str("RegistryAddr", service.conf.ServiceAddr).
				Int("RegistryPort", service.conf.ServicePort).
				Str("RoutingTag", service.conf.RoutingTag)
			if TcpParamConf.Get().Immediately {
				l.Msg("TcpBackend Update Lost And Del")
				delete(g.services, serviceId) // 先删
				service.close()               // 关闭
				remove = append(remove, service)
			} else {
				l.Msg("TcpBackend Update Lost")
				atomic.StoreInt32(&service.confDestroy, 1)
			}
		}
	}
	// 新增的和修改的
	for serviceId, conf := range serviceConfs {
		service, ok := g.services[serviceId]
		if ok {
			// 判断是否修改了，如果需改了关闭之前的重新创建
			if service.conf.ServiceAddr != conf.ServiceAddr || service.conf.ServicePort != conf.ServicePort {
				log.Info().Str("ServiceName", service.conf.ServiceName).
					Str("ServiceId", service.conf.ServiceId).
					Str("RegistryAddr", service.conf.ServiceAddr).
					Int("RegistryPort", service.conf.ServicePort).
					Str("RoutingTag", service.conf.RoutingTag).
					Msg("TcpBackend Update Change")

				service.close() // 先关闭
				// 创建
				var err error
				service, err = NewTcpService(conf, g)
				if err != nil {
					continue
				}
				g.services[serviceId] = service
			} else if atomic.CompareAndSwapInt32(&service.confDestroy, 1, 0) {
				log.Info().Str("ServiceName", service.conf.ServiceName).
					Str("ServiceId", service.conf.ServiceId).
					Str("RegistryAddr", service.conf.ServiceAddr).
					Int("RegistryPort", service.conf.ServicePort).
					Str("RoutingTag", service.conf.RoutingTag).
					Msg("TcpBackend Update Recover")
			}
		} else {
			// 新增
			log.Info().Str("ServiceName", conf.ServiceName).
				Str("ServiceId", conf.ServiceId).
				Str("RegistryAddr", conf.ServiceAddr).
				Int("RegistryPort", conf.ServicePort).
				Str("RoutingTag", conf.RoutingTag).
				Msg("TcpBackend Update Add")
			service, err := NewTcpService(conf, g)
			if err != nil {
				continue
			}
			g.services[serviceId] = service
			add = append(add, service)
		}
	}
	len := len(g.services)
	g.Unlock()

	// 回调
	for _, service := range remove {
		for _, h := range g.tb.hook {
			h.OnRemove(service)
		}
	}
	for _, service := range add {
		for _, h := range g.tb.hook {
			h.OnAdd(service)
		}
	}
	return len
}

// 内部使用
// 添加到哈希环
func (g *TcpGroup[T]) addHashring(serviceId, routingTag string) {
	g.hashring.Add(serviceId)
	if len(routingTag) > 0 {
		hashring, ok := g.tagHashring.Load(routingTag)
		if ok {
			hashring.(*consistent.Consistent).Add(serviceId)
		} else {
			hashring := consistent.New()
			hashring.Add(serviceId)
			g.tagHashring.Store(routingTag, hashring)
		}
	}
}

// 从哈希环中移除
func (g *TcpGroup[T]) removeHashring(serviceId, routingTag string) {
	g.hashring.Remove(serviceId)
	hashring, ok := g.tagHashring.Load(routingTag)
	if ok {
		hashring.(*consistent.Consistent).Remove(serviceId)
		if len(hashring.(*consistent.Consistent).Members()) == 0 {
			g.tagHashring.Delete(routingTag)
		}
	}
}

// 删除Service
func (g *TcpGroup[T]) removeSevice(serviceId string) {
	g.Lock()
	service, ok := g.services[serviceId]
	if ok {
		log.Info().Str("ServiceName", service.conf.ServiceName).
			Str("ServiceId", service.conf.ServiceId).
			Str("RegistryAddr", service.conf.ServiceAddr).
			Int("RegistryPort", service.conf.ServicePort).
			Str("RoutingTag", service.conf.RoutingTag).
			Msg("TcpBackend Del")
		delete(g.services, serviceId)
	}
	len := len(g.services)
	g.Unlock()

	// 回调
	for _, h := range g.tb.hook {
		h.OnRemove(service)
	}

	// 如果空了
	if len == 0 {
		g.tb.Lock()
		delete(g.tb.group, g.serviceName)
		g.tb.Unlock()
	}
}

// 内部使用
