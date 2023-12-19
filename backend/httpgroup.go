package backend

// https://github.com/yuwf/gobase

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
	// 线程安全
	hashring    *consistent.Consistent // 哈希环，填充serviceId, 交给HttpService来填充，他有健康检查和熔断机制
	tagHashring *sync.Map              // 按tag分组的哈希环 [tag:*consistent.Consistent]
}

func NewHttpGroup[T any](hb *HttpBackend[T]) *HttpGroup[T] {
	g := &HttpGroup[T]{
		hb:          hb,
		services:    map[string]*HttpService[T]{},
		hashring:    consistent.New(),
		tagHashring: new(sync.Map),
	}
	return g
}

func (g *HttpGroup[T]) GetService(serviceId string) *HttpService[T] {
	serviceId = strings.TrimSpace(strings.ToLower(serviceId))
	g.RLock()
	defer g.RUnlock()
	service, ok := g.services[serviceId]
	if ok {
		return service
	}
	return nil
}

func (g *HttpGroup[T]) GetServices() map[string]*HttpService[T] {
	g.RLock()
	defer g.RUnlock()
	ss := map[string]*HttpService[T]{}
	for serviceId, service := range g.services {
		ss[serviceId] = service
	}
	return ss
}

// 根据哈希环获取对象 hash可以用用户id或者其他稳定的数据
func (g *HttpGroup[T]) GetServiceByHash(hash string) *HttpService[T] {
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
func (g *HttpGroup[T]) GetServiceByTagAndHash(tag, hash string) *HttpService[T] {
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
				Str("RoutingTag", service.conf.RoutingTag).
				Msg("HttpBackend Update Lost And Del")
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
func (g *HttpGroup[T]) addHashring(serviceId, routingTag string) {
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
func (g *HttpGroup[T]) removeHashring(serviceId, routingTag string) {
	g.hashring.Remove(serviceId)
	hashring, ok := g.tagHashring.Load(routingTag)
	if ok {
		hashring.(*consistent.Consistent).Remove(serviceId)
		if len(hashring.(*consistent.Consistent).Members()) == 0 {
			g.tagHashring.Delete(routingTag)
		}
	}
}
