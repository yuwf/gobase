package backend

// https://github.com/yuwf

import (
	"strings"
	"sync"
)

// T是和业务相关的客户端信息结构 透传给HttpService
type HttpBackend[T any] struct {
	sync.RWMutex
	group   map[string]*HttpGroup[T] // 服务器组map对象
	event   HttpEvent[T]             // 事件处理
	watcher interface{}              // 服务器发现相关的对象 consul或者redis对象
	// 请求处理完后回调 不使用锁，默认要求提前注册好
	hook []HttpHook[T]
}

// 注册hook
func (hb *HttpBackend[T]) RegHook(h HttpHook[T]) {
	hb.hook = append(hb.hook, h)
}

func (hb *HttpBackend[T]) GetGroup(serviceName string) *HttpGroup[T] {
	serviceName = strings.ToLower(serviceName)
	serviceName = strings.TrimSpace(serviceName)
	hb.RLock()
	defer hb.RUnlock()
	group, ok := hb.group[serviceName]
	if ok {
		return group
	}
	return nil
}

func (hb *HttpBackend[T]) GetService(serviceName, serviceId string) *HttpService[T] {
	group := hb.GetGroup(serviceName)
	if group != nil {
		return group.GetService(serviceId)
	}
	return nil
}

// 根据哈希环获取,哈希环行记录的都是状态测试健康的
func (hb *HttpBackend[T]) GetServiceByHash(serviceName, hash string) *HttpService[T] {
	group := hb.GetGroup(serviceName)
	if group != nil {
		return group.GetServiceByHash(hash)
	}
	return nil
}

// 服务器发现 更新逻辑
func (hb *HttpBackend[T]) updateServices(confs []*ServiceConfig) {
	// 组织成Map结构
	confsMap := ServiceNameConfMap{}
	for _, conf := range confs {
		if confsMap[conf.ServiceName] == nil {
			confsMap[conf.ServiceName] = ServiceIdConfMap{}
		}
		confsMap[conf.ServiceName][conf.ServiceId] = conf
	}

	// 如果confsMap中不存在group已经存在的组 填充一个空的
	{
		hb.RLock()
		for serviceName := range hb.group {
			_, ok := confsMap[serviceName]
			if !ok {
				confsMap[serviceName] = ServiceIdConfMap{}
			}
		}
		hb.RUnlock()
	}

	for serviceName, confs := range confsMap {
		group := hb.GetGroup(serviceName)
		if group == nil {
			if len(confs) == 0 {
				continue
			}
			// 如果之前不存在 创建一个
			group = NewHttpGroup(hb)
			hb.Lock()
			hb.group[serviceName] = group
			hb.Unlock()
		}
		// 更新组
		count := group.update(confs, hb.event)
		// 如果存在，配置为空了，删除
		if count == 0 {
			hb.Lock()
			delete(hb.group, serviceName)
			hb.Unlock()
		}
	}
}
