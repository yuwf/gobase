package backend

// https://github.com/yuwf/gobase

import (
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
	"stathat.com/c/consistent"
)

// 功能相同的一组服务器
// ServiceInfo是和业务相关的连接端自定义信息结构
type HttpGroup[ServiceInfo any] struct {
	// 不可修改，协程安全
	serviceName string
	hb          *HttpBackend[ServiceInfo] // 上层对象

	sync.RWMutex
	services map[string]*HttpService[ServiceInfo] // 锁保护

	// 哈希环，协程安全，根据版本号生成
	hashringServicesVersion int64
	hashringConnVersion     int64
	hashringAll             *consistent.Consistent //
	tagHashringAll          *sync.Map              // 按tag分组的哈希环 [tag:*consistent.Consistent]
	hashringConn            *consistent.Consistent //
	tagHashringConn         *sync.Map              // 按tag分组的哈希环 [tag:*consistent.Consistent]
}

func NewHttpGroup[ServiceInfo any](serviceName string, hb *HttpBackend[ServiceInfo]) *HttpGroup[ServiceInfo] {
	g := &HttpGroup[ServiceInfo]{
		serviceName: serviceName,
		hb:          hb,
		services:    map[string]*HttpService[ServiceInfo]{},
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
// hash可以用用户id或者其他稳定的数据
// status有效值 HttpStatus_All、HttpStatus_Conned
func (g *HttpGroup[ServiceInfo]) GetServiceByHash(hash string, status int) *HttpService[ServiceInfo] {
	g.updateHashring()
	var hashring *consistent.Consistent
	if status == HttpStatus_All {
		hashring = g.hashringAll
	} else if status == HttpStatus_Conned {
		hashring = g.hashringConn
	}
	if hashring == nil {
		return nil
	}

	serviceId, err := hashring.Get(hash)
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
// hash可以用用户id或者其他稳定的数据
// status有效值 HttpStatus_All、HttpStatus_Conned
func (g *HttpGroup[ServiceInfo]) GetServiceByTagAndHash(tag, hash string, status int) *HttpService[ServiceInfo] {
	tag = strings.TrimSpace(strings.ToLower(tag))

	g.updateHashring()
	var tagHashring *sync.Map
	if status == HttpStatus_All {
		tagHashring = g.tagHashringAll
	} else if status == HttpStatus_Conned {
		tagHashring = g.tagHashringConn
	}
	if tagHashring == nil {
		return nil
	}

	hasrhing, ok := tagHashring.Load(tag)
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

// 更新组，返回剩余个数、新增个数、修改个数、删除个数
func (g *HttpGroup[ServiceInfo]) update(confs ServiceIdConfMap, handler HttpEvent[ServiceInfo]) (int, int, int, int) {
	var remove []*HttpService[ServiceInfo]
	var add []*HttpService[ServiceInfo]
	var modify []*HttpService[ServiceInfo]

	g.Lock()
	// 先删除不存在的
	for serviceId, service := range g.services {
		_, ok := confs[serviceId]
		if !ok {
			log.Info().Str("ServiceName", service.conf.ServiceName).
				Str("ServiceId", service.conf.ServiceId).
				Str("RegistryAddr", service.conf.ServiceAddr).
				Int("RegistryPort", service.conf.ServicePort).
				Strs("RoutingTag", service.conf.RoutingTag).
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
					Strs("RoutingTag", service.conf.RoutingTag).
					Msg("HttpBackend Update Change")

				service.close() // 先关闭
				// 创建
				var err error
				service, err = NewHttpService(conf, g)
				if err != nil {
					continue
				}
				g.services[serviceId] = service
				modify = append(modify, service)
			} else {
				if !reflect.DeepEqual(service.conf, conf) {
					service.conf = conf
					modify = append(modify, service)
				}
			}
		} else {
			// 新增
			log.Info().Str("ServiceName", conf.ServiceName).
				Str("ServiceId", conf.ServiceId).
				Str("RegistryAddr", conf.ServiceAddr).
				Int("RegistryPort", conf.ServicePort).
				Strs("RoutingTag", conf.RoutingTag).
				Msg("HttpBackend Update Add")
			service, err := NewHttpService(conf, g)
			if err != nil {
				continue
			}
			g.services[serviceId] = service
			add = append(add, service)
		}
	}
	count := len(g.services)
	g.Unlock()

	// 回调
	func() {
		defer utils.HandlePanic()
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
	}()
	return count, len(add), len(modify), len(remove)
}

func (g *HttpGroup[ServiceInfo]) updateHashring() {
	var all, conn bool
	hashringServicesVersion := atomic.LoadInt64(&g.hashringServicesVersion)
	servicesVersion := g.hb.GetServiceVersion(g.serviceName)
	if hashringServicesVersion != servicesVersion && atomic.CompareAndSwapInt64(&g.hashringServicesVersion, hashringServicesVersion, servicesVersion) {
		all = true
	}
	hashringConnVersion := atomic.LoadInt64(&g.hashringConnVersion)
	connVersion := g.hb.GetConnVersion(g.serviceName)
	if hashringConnVersion != connVersion && atomic.CompareAndSwapInt64(&g.hashringConnVersion, hashringConnVersion, connVersion) {
		conn = true
	}
	if !all && !conn {
		return
	}

	g.Lock()
	defer g.Unlock()

	for serviceId, service := range g.services {
		if all {
			g.hashringAll = consistent.New()
			g.tagHashringAll = new(sync.Map)
			g.addHashring(g.hashringAll, g.tagHashringAll, serviceId, service.conf.RoutingTag)
		}
		status := service.HealthStatus()
		if all || conn {
			g.hashringConn = consistent.New()
			g.tagHashringConn = new(sync.Map)
			if status == HttpStatus_Conned {
				g.addHashring(g.hashringConn, g.tagHashringConn, serviceId, service.conf.RoutingTag)
			}
		}
	}
}

// 添加到哈希环
func (g *HttpGroup[ServiceInfo]) addHashring(hashring *consistent.Consistent, tagHashring *sync.Map, serviceId string, routingTag []string) {
	hashring.Add(serviceId)
	if len(routingTag) > 0 {
		for _, tag := range routingTag {
			hashring, ok := tagHashring.Load(tag)
			if ok {
				hashring.(*consistent.Consistent).Add(serviceId)
			} else {
				hashring := consistent.New()
				hashring.Add(serviceId)
				tagHashring.Store(tag, hashring)
			}
		}
	}
}
