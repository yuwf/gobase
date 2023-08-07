package backend

// https://github.com/yuwf

import (
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
	"stathat.com/c/consistent"
)

// 功能相同的一组服务器
// T是和业务相关的客户端信息结构 透传给HttpService
type HttpGroup[T any] struct {
	sync.RWMutex
	hb       *HttpBackend[T]            // 上层对象
	services map[string]*HttpService[T] // 锁保护
	hashring *consistent.Consistent     // 哈希环，填充serviceId, 交给HttpService来填充，他有健康检查和熔断机制
}

func NewHttpGroup[T any](hb *HttpBackend[T]) *HttpGroup[T] {
	g := &HttpGroup[T]{
		hb:       hb,
		services: map[string]*HttpService[T]{},
		hashring: consistent.New(),
	}
	return g
}

func (g *HttpGroup[T]) GetService(serviceId string) *HttpService[T] {
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
func (g *HttpGroup[T]) GetServiceByHash(hash string) *HttpService[T] {
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

// 更新组，返回剩余个数
func (g *HttpGroup[T]) update(confs ServiceIdConfMap, handler HttpEvent[T]) int {
	var remove []*HttpService[T]
	var add []*HttpService[T]

	g.Lock()
	// 先删除不存在的
	for serviceId, service := range g.services {
		_, ok := confs[serviceId]
		if !ok {
			log.Info().Str("ServiceName", service.conf.ServiceName).
				Str("ServiceId", service.conf.ServiceId).
				Str("RegistryAddr", service.conf.ServiceAddr).
				Int("RegistryPort", service.conf.ServicePort).
				Msg("HttpBackend Lost And Del")
			service.close() // 关闭
			delete(g.services, serviceId)
			remove = append(remove, service)
		}
	}
	// 新增的和修改的
	for serviceId, conf := range confs {
		service, ok := g.services[serviceId]
		if ok {
			// 判断是否修改了，如果需改了关闭之前的重新创建
			if service.conf.ServiceAddr != conf.ServiceAddr || service.conf.ServicePort != conf.ServicePort {
				log.Info().Str("ServiceName", service.conf.ServiceName).
					Str("ServiceId", service.conf.ServiceId).
					Str("RegistryAddr", service.conf.ServiceAddr).
					Int("RegistryPort", service.conf.ServicePort).
					Msg("HttpBackend Change")

				service.close() // 先关闭
				// 创建
				var err error
				service, err = NewHttpService(conf, g)
				if err != nil {
					continue
				}
				g.services[serviceId] = service
			}
		} else {
			// 新增
			log.Info().Str("ServiceName", conf.ServiceName).
				Str("ServiceId", conf.ServiceId).
				Str("RegistryAddr", conf.ServiceAddr).
				Int("RegistryPort", conf.ServicePort).
				Msg("HttpBackend Add")
			service, err := NewHttpService(conf, g)
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
		for _, h := range g.hb.hook {
			h.OnRemove(service)
		}
	}
	for _, service := range add {
		for _, h := range g.hb.hook {
			h.OnAdd(service)
		}
	}
	return len
}
