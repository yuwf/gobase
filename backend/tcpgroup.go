package backend

// https://github.com/yuwf/gobase

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"
	"stathat.com/c/consistent"
)

// 功能相同的一组服务器
// T是和业务相关的客户端信息结构 透传给TcpService
type TcpGroup[T any] struct {
	sync.RWMutex
	tb          *TcpBackend[T]            // 上层对象
	serviceName string                    // 不可修改
	services    map[string]*TcpService[T] // 锁保护
	hashring    *consistent.Consistent    // 哈希环，填充serviceId, 协程安全，交给TcpService来填充，他有连接和熔断策略
}

func NewTcpGroup[T any](serviceName string, tb *TcpBackend[T]) *TcpGroup[T] {
	g := &TcpGroup[T]{
		tb:          tb,
		serviceName: serviceName,
		services:    map[string]*TcpService[T]{},
		hashring:    consistent.New(),
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

// 根据哈希环获取对象 hash可以用用户id或者其他稳定的数据
func (g *TcpGroup[T]) GetServiceByHash(hash string) *TcpService[T] {
	g.RLock()
	defer g.RUnlock()
	serviceId, err := g.hashring.Get(hash)
	if err != nil {
		return nil
	}
	service, ok := g.services[serviceId]
	if ok {
		return service
	}
	return nil
}

// 广播消息
func (g *TcpGroup[T]) Broad(buf []byte) {
	g.RLock()
	defer g.RUnlock()
	for _, service := range g.services {
		service.Send(buf)
	}
}

func (g *TcpGroup[T]) BroadMsg(msg interface{}) {
	g.RLock()
	defer g.RUnlock()
	for _, service := range g.services {
		service.SendMsg(msg)
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
				Int("RegistryPort", service.conf.ServicePort)
			if TcpParamConf.Get().Immediately {
				l.Msg("TcpBackend Lost And Del")
				delete(g.services, serviceId) // 先删
				service.close()               // 关闭
				remove = append(remove, service)
			} else {
				l.Msg("TcpBackend Lost")
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
					Msg("TcpBackend Change")

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
					Msg("TcpBackend Recover")
			}
		} else {
			// 新增
			log.Info().Str("ServiceName", conf.ServiceName).
				Str("ServiceId", conf.ServiceId).
				Str("RegistryAddr", conf.ServiceAddr).
				Int("RegistryPort", conf.ServicePort).
				Msg("TcpBackend Add")
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

// 内部使用，删除Service
func (g *TcpGroup[T]) removeSevice(serviceId string) {
	g.Lock()
	service, ok := g.services[serviceId]
	if ok {
		log.Info().Str("ServiceName", service.conf.ServiceName).
			Str("ServiceId", service.conf.ServiceId).
			Str("RegistryAddr", service.conf.ServiceAddr).
			Int("RegistryPort", service.conf.ServicePort).
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
