package backend

// https://github.com/yuwf/gobase

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
	"stathat.com/c/consistent"
)

// 功能相同的一组服务器
// T是和业务相关的客户端信息结构 透传给TcpService
type TcpGroup[ServiceInfo any] struct {
	sync.RWMutex
	tb          *TcpBackend[ServiceInfo]            // 上层对象
	serviceName string                              // 不可修改
	services    map[string]*TcpService[ServiceInfo] // 锁保护
	// 线程安全，哈希环 只填充连接状态和配置都OK的
	hashring    *consistent.Consistent //
	tagHashring *sync.Map              // 按tag分组的哈希环 [tag:*consistent.Consistent]
}

func NewTcpGroup[ServiceInfo any](serviceName string, tb *TcpBackend[ServiceInfo]) *TcpGroup[ServiceInfo] {
	g := &TcpGroup[ServiceInfo]{
		tb:          tb,
		serviceName: serviceName,
		services:    map[string]*TcpService[ServiceInfo]{},
		hashring:    consistent.New(),
		tagHashring: new(sync.Map),
	}
	return g
}

func (g *TcpGroup[ServiceInfo]) GetService(serviceId string) *TcpService[ServiceInfo] {
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

func (g *TcpGroup[ServiceInfo]) GetServices() []*TcpService[ServiceInfo] {
	g.RLock()
	defer g.RUnlock()
	ss := make([]*TcpService[ServiceInfo], 0, len(g.services))
	for _, service := range g.services {
		ss = append(ss, service)
	}
	return ss
}

// 根据哈希环获取对象 hash可以用用户id或者其他稳定的数据
// 只返回连接状态和发现配置都正常的服务对象
func (g *TcpGroup[ServiceInfo]) GetServiceByHash(hash string) *TcpService[ServiceInfo] {
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
func (g *TcpGroup[ServiceInfo]) GetServiceByTagAndHash(tag, hash string) *TcpService[ServiceInfo] {
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
func (g *TcpGroup[ServiceInfo]) update(serviceConfs ServiceIdConfMap) int {
	var remove []*TcpService[ServiceInfo]
	var add []*TcpService[ServiceInfo]

	g.Lock()
	// 先删除不存在的
	for serviceId, service := range g.services {
		_, ok := serviceConfs[serviceId]
		if !ok {
			l := log.Info().Str("ServiceName", service.conf.ServiceName).
				Str("ServiceId", service.conf.ServiceId).
				Str("RegistryAddr", service.conf.ServiceAddr).
				Int("RegistryPort", service.conf.ServicePort).
				Strs("RoutingTag", service.conf.RoutingTag)

			// 从哈希环中移除
			g.removeHashring(service.conf.ServiceId)
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
					Strs("RoutingTag", service.conf.RoutingTag).
					Msg("TcpBackend Update Change")

				// 从哈希环中移除
				g.removeHashring(service.conf.ServiceId)
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
					Strs("RoutingTag", service.conf.RoutingTag).
					Msg("TcpBackend Update Recover")
				// 添加到哈希环中
				if service.HealthState() == 0 {
					service.g.addHashring(service.conf.ServiceId, service.conf.RoutingTag)
				}
			}
		} else {
			// 新增
			log.Info().Str("ServiceName", conf.ServiceName).
				Str("ServiceId", conf.ServiceId).
				Str("RegistryAddr", conf.ServiceAddr).
				Int("RegistryPort", conf.ServicePort).
				Strs("RoutingTag", conf.RoutingTag).
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
	func() {
		defer utils.HandlePanic()
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
	}()
	return len
}

// 内部使用
// 添加到哈希环
func (g *TcpGroup[ServiceInfo]) addHashring(serviceId string, routingTag []string) {
	g.hashring.Add(serviceId)
	if len(routingTag) > 0 {
		for _, tag := range routingTag {
			hashring, ok := g.tagHashring.Load(tag)
			if ok {
				hashring.(*consistent.Consistent).Add(serviceId)
			} else {
				hashring := consistent.New()
				hashring.Add(serviceId)
				g.tagHashring.Store(tag, hashring)
			}
		}
	}
}

// 从哈希环中移除
func (g *TcpGroup[ServiceInfo]) removeHashring(serviceId string) {
	g.hashring.Remove(serviceId)
	g.tagHashring.Range(func(key, value any) bool {
		value.(*consistent.Consistent).Remove(serviceId)

		if len(value.(*consistent.Consistent).Members()) == 0 {
			g.tagHashring.Delete(key)
		}
		return true
	})
}

// 删除Service
func (g *TcpGroup[ServiceInfo]) removeSevice(serviceId string) {
	g.Lock()
	service, ok := g.services[serviceId]
	if ok {
		log.Info().Str("ServiceName", service.conf.ServiceName).
			Str("ServiceId", service.conf.ServiceId).
			Str("RegistryAddr", service.conf.ServiceAddr).
			Int("RegistryPort", service.conf.ServicePort).
			Strs("RoutingTag", service.conf.RoutingTag).
			Msg("TcpBackend Del")
		delete(g.services, serviceId)
	}
	len := len(g.services)
	g.Unlock()

	// 回调
	func() {
		defer utils.HandlePanic()
		for _, h := range g.tb.hook {
			h.OnRemove(service)
		}
	}()

	// 如果空了
	if len == 0 {
		g.tb.Lock()
		delete(g.tb.group, g.serviceName)
		g.tb.Unlock()
	}
}

// 内部使用
