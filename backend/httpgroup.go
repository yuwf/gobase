package backend

// https://github.com/yuwf/gobase

import (
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
	"stathat.com/c/consistent"
)

// 功能相同的一组服务器
// ServiceInfo是和业务相关的客户端信息结构 透传给HttpService
type HttpGroup[ServiceInfo any] struct {
	sync.RWMutex
	hb       *HttpBackend[ServiceInfo]            // 上层对象
	services map[string]*HttpService[ServiceInfo] // 锁保护
	// 线程安全，哈希环 只填充连接状态和配置都OK的
	hashring    *consistent.Consistent //
	tagHashring *sync.Map              // 按tag分组的哈希环 [tag:*consistent.Consistent]
}

func NewHttpGroup[ServiceInfo any](hb *HttpBackend[ServiceInfo]) *HttpGroup[ServiceInfo] {
	g := &HttpGroup[ServiceInfo]{
		hb:          hb,
		services:    map[string]*HttpService[ServiceInfo]{},
		hashring:    consistent.New(),
		tagHashring: new(sync.Map),
	}
	return g
}

func (g *HttpGroup[ServiceInfo]) GetService(serviceId string) *HttpService[ServiceInfo] {
	serviceId = strings.TrimSpace(strings.ToLower(serviceId))
	g.RLock()
	defer g.RUnlock()
	service, ok := g.services[serviceId]
	if ok {
		return service
	}
	return nil
}

func (g *HttpGroup[ServiceInfo]) GetServices() map[string]*HttpService[ServiceInfo] {
	g.RLock()
	defer g.RUnlock()
	ss := map[string]*HttpService[ServiceInfo]{}
	for serviceId, service := range g.services {
		ss[serviceId] = service
	}
	return ss
}

// 根据哈希环获取对象 hash可以用用户id或者其他稳定的数据
// 只返回连接状态和发现配置都正常的服务对象
func (g *HttpGroup[ServiceInfo]) GetServiceByHash(hash string) *HttpService[ServiceInfo] {
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
// 只返回连接状态和发现配置都正常的服务对象
func (g *HttpGroup[ServiceInfo]) GetServiceByTagAndHash(tag, hash string) *HttpService[ServiceInfo] {
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

// 更新组，返回剩余个数
func (g *HttpGroup[ServiceInfo]) update(confs ServiceIdConfMap, handler HttpEvent[ServiceInfo]) int {
	var remove []*HttpService[ServiceInfo]
	var add []*HttpService[ServiceInfo]

	g.Lock()
	// 先删除不存在的
	for serviceId, service := range g.services {
		_, ok := confs[serviceId]
		if !ok {
			log.Info().Str("ServiceName", service.conf.ServiceName).
				Str("ServiceId", service.conf.ServiceId).
				Str("RegistryAddr", service.conf.ServiceAddr).
				Int("RegistryPort", service.conf.ServicePort).
				Str("RoutingTag", service.conf.RoutingTag).
				Msg("HttpBackend Update Lost And Del")

			// 从哈希环中移除
			g.removeHashring(service.conf.ServiceId, service.conf.RoutingTag)
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
					Str("RoutingTag", service.conf.RoutingTag).
					Msg("HttpBackend Update Change")

				// 从哈希环中移除
				g.removeHashring(service.conf.ServiceId, service.conf.RoutingTag)
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
				Str("RoutingTag", conf.RoutingTag).
				Msg("HttpBackend Update Add")
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

// 添加到哈希环
func (g *HttpGroup[ServiceInfo]) addHashring(serviceId, routingTag string) {
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
func (g *HttpGroup[ServiceInfo]) removeHashring(serviceId, routingTag string) {
	g.hashring.Remove(serviceId)
	hashring, ok := g.tagHashring.Load(routingTag)
	if ok {
		hashring.(*consistent.Consistent).Remove(serviceId)
		if len(hashring.(*consistent.Consistent).Members()) == 0 {
			g.tagHashring.Delete(routingTag)
		}
	}
}
