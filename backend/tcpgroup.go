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
type TcpGroup[ServiceInfo any] struct {
	// 不可修改，协程安全
	serviceName string
	tb          *TcpBackend[ServiceInfo] // 上层对象

	sync.RWMutex
	services map[string]*TcpService[ServiceInfo] // 锁保护

	// services中所有的对象的哈希环

	// 哈希环，协程安全，根据版本号生成
	hashringServicesVersion int64
	hashringConnVersion     int64
	hashringLoginVersion    int64
	hashringAll             *consistent.Consistent //
	tagHashringAll          *sync.Map              // 按tag分组的哈希环 [tag:*consistent.Consistent]
	hashringConn            *consistent.Consistent //
	tagHashringConn         *sync.Map              // 按tag分组的哈希环 [tag:*consistent.Consistent]
	hashringLogin           *consistent.Consistent //
	tagHashringLogin        *sync.Map              //
}

func NewTcpGroup[ServiceInfo any](serviceName string, tb *TcpBackend[ServiceInfo]) *TcpGroup[ServiceInfo] {
	g := &TcpGroup[ServiceInfo]{
		serviceName: serviceName,
		tb:          tb,
		services:    map[string]*TcpService[ServiceInfo]{},
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

// 根据哈希环获取对象
// hash可以用用户id或者其他稳定的数据
// status有效值 TcpStatus_All、TcpStatus_Conned、TcpStatus_Logined
func (g *TcpGroup[ServiceInfo]) GetServiceByHash(hash string, status int) *TcpService[ServiceInfo] {
	g.updateHashring()
	var hashring *consistent.Consistent
	if status == TcpStatus_All {
		hashring = g.hashringAll
	} else if status == TcpStatus_Conned {
		hashring = g.hashringConn
	} else if status == TcpStatus_Logined {
		hashring = g.hashringLogin
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

// 根据tag和哈希环获取对象
// hash可以用用户id或者其他稳定的数据
// status有效值 TcpStatus_All、TcpStatus_Conned、TcpStatus_Logined
func (g *TcpGroup[ServiceInfo]) GetServiceByTagAndHash(tag, hash string, status int) *TcpService[ServiceInfo] {
	tag = strings.TrimSpace(strings.ToLower(tag))

	g.updateHashring()
	var tagHashring *sync.Map
	if status == TcpStatus_All {
		tagHashring = g.tagHashringAll
	} else if status == TcpStatus_Conned {
		tagHashring = g.tagHashringConn
	} else if status == TcpStatus_Logined {
		tagHashring = g.tagHashringLogin
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
func (g *TcpGroup[ServiceInfo]) update(serviceConfs ServiceIdConfMap) (int, int, int, int) {
	var remove []*TcpService[ServiceInfo]
	var add []*TcpService[ServiceInfo]
	var modify []*TcpService[ServiceInfo]

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

			if TcpParamConf.Get().Immediately || !service.conn.Connected() {
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

				service.close() // 先关闭
				// 创建
				var err error
				service, err = NewTcpService(conf, g)
				if err != nil {
					continue
				}
				g.services[serviceId] = service
				modify = append(modify, service)
			} else {
				if atomic.CompareAndSwapInt32(&service.confDestroy, 1, 0) {
					log.Info().Str("ServiceName", service.conf.ServiceName).
						Str("ServiceId", service.conf.ServiceId).
						Str("RegistryAddr", service.conf.ServiceAddr).
						Int("RegistryPort", service.conf.ServicePort).
						Strs("RoutingTag", service.conf.RoutingTag).
						Msg("TcpBackend Update Recover")
				}

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
				Msg("TcpBackend Update Add")
			service, err := NewTcpService(conf, g)
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
	return count, len(add), len(modify), len(remove)
}

func (g *TcpGroup[ServiceInfo]) updateHashring() {
	var all, conn, login bool
	hashringServicesVersion := atomic.LoadInt64(&g.hashringServicesVersion)
	servicesVersion := g.tb.getServiceVersion(g.serviceName)
	if hashringServicesVersion != servicesVersion && atomic.CompareAndSwapInt64(&g.hashringServicesVersion, hashringServicesVersion, servicesVersion) {
		all = true
	}
	hashringConnVersion := atomic.LoadInt64(&g.hashringConnVersion)
	connVersion := g.tb.getConnVersion(g.serviceName)
	if hashringConnVersion != connVersion && atomic.CompareAndSwapInt64(&g.hashringConnVersion, hashringConnVersion, connVersion) {
		conn = true
	}
	hashringLoginVersion := atomic.LoadInt64(&g.hashringLoginVersion)
	connLoginVersion := g.tb.getLoginVersion(g.serviceName)
	if hashringLoginVersion != connLoginVersion && atomic.CompareAndSwapInt64(&g.hashringLoginVersion, hashringLoginVersion, connLoginVersion) {
		login = true
	}
	if !all && !conn && !login {
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
		status, _ := service.HealthStatus()
		if all || conn {
			g.hashringConn = consistent.New()
			g.tagHashringConn = new(sync.Map)
			if status == TcpStatus_Conned {
				g.addHashring(g.hashringConn, g.tagHashringConn, serviceId, service.conf.RoutingTag)
			}
		}
		if all || login {
			g.hashringLogin = consistent.New()
			g.tagHashringLogin = new(sync.Map)
			if status == TcpStatus_Logined {
				g.addHashring(g.hashringLogin, g.tagHashringLogin, serviceId, service.conf.RoutingTag)
			}
		}
	}
}

// 添加到哈希环
func (g *TcpGroup[ServiceInfo]) addHashring(hashring *consistent.Consistent, tagHashring *sync.Map, serviceId string, routingTag []string) {
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

// service层删除Service使用
func (g *TcpGroup[ServiceInfo]) removeSevice(serviceId string) {
	g.Lock()
	service, ok := g.services[serviceId]
	if !ok {
		g.Unlock()
		return
	}

	log.Info().Str("ServiceName", service.conf.ServiceName).
		Str("ServiceId", service.conf.ServiceId).
		Str("RegistryAddr", service.conf.ServiceAddr).
		Int("RegistryPort", service.conf.ServicePort).
		Strs("RoutingTag", service.conf.RoutingTag).
		Msg("TcpBackend Del")

	delete(g.services, serviceId)
	g.tb.addServiceVersion(g.serviceName)

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
		g.tb.groupMutex.Lock()
		delete(g.tb.group, g.serviceName)
		g.tb.groupMutex.Unlock()
	}
}

// 内部使用
