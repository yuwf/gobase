package backend

// https://github.com/yuwf/gobase

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/yuwf/gobase/httprequest"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

// ServiceInfo是和业务相关的客户端信息结构 透传给HttpService
type HttpBackend[ServiceInfo any] struct {
	sync.RWMutex                                    // 注意只保护group的变化 不要保护group内的操作
	group        map[string]*HttpGroup[ServiceInfo] // 服务器组map对象
	event        HttpEvent[ServiceInfo]             // 事件处理
	watcher      interface{}                        // 服务器发现相关的对象 consul或者redis对象
	// 请求处理完后回调 不使用锁，默认要求提前注册好
	hook []HttpHook[ServiceInfo]
}

// 注册hook
func (hb *HttpBackend[ServiceInfo]) RegHook(h HttpHook[ServiceInfo]) {
	hb.hook = append(hb.hook, h)
}

func (hb *HttpBackend[ServiceInfo]) GetGroup(serviceName string) *HttpGroup[ServiceInfo] {
	serviceName = strings.TrimSpace(strings.ToLower(serviceName))
	hb.RLock()
	defer hb.RUnlock()
	group, ok := hb.group[serviceName]
	if ok {
		return group
	}
	return nil
}

func (hb *HttpBackend[ServiceInfo]) GetGroups() map[string]*HttpGroup[ServiceInfo] {
	hb.RLock()
	defer hb.RUnlock()
	gs := map[string]*HttpGroup[ServiceInfo]{}
	for serviceName, group := range hb.group {
		gs[serviceName] = group
	}
	return gs
}

func (hb *HttpBackend[ServiceInfo]) GetService(serviceName, serviceId string) *HttpService[ServiceInfo] {
	group := hb.GetGroup(serviceName)
	if group != nil {
		return group.GetService(serviceId)
	}
	return nil
}

// 根据哈希环获取,哈希环行记录的都是状态测试健康的
func (hb *HttpBackend[ServiceInfo]) GetServiceByHash(serviceName, hash string) *HttpService[ServiceInfo] {
	group := hb.GetGroup(serviceName)
	if group != nil {
		return group.GetServiceByHash(hash)
	}
	return nil
}

// 根据哈希环获取,哈希环行记录的都是状态测试健康的
func (hb *HttpBackend[ServiceInfo]) GetServiceByTagAndHash(serviceName, tag, hash string) *HttpService[ServiceInfo] {
	group := hb.GetGroup(serviceName)
	if group != nil {
		return group.GetServiceByTagAndHash(tag, hash)
	}
	return nil
}

func (hb *HttpBackend[ServiceInfo]) Get(ctx context.Context, serviceName, serviceId, path string, body []byte, headers map[string]string) (int, []byte, error) {
	service := hb.GetService(serviceName, serviceId)
	if service != nil {
		return httprequest.Request(ctx, "get", service.Address()+path, body, headers)
	}
	err := fmt.Errorf("not find HttpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("path", path).Msg("HttpServiceBackend Get error")
	return http.StatusNotFound, nil, err
}

func (hb *HttpBackend[ServiceInfo]) Post(ctx context.Context, serviceName, serviceId, path string, body []byte, headers map[string]string) (int, []byte, error) {
	service := hb.GetService(serviceName, serviceId)
	if service != nil {
		return httprequest.Request(ctx, "post", service.Address()+path, body, headers)
	}
	err := fmt.Errorf("not find HttpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("path", path).Msg("HttpServiceBackend Post error")
	return http.StatusNotFound, nil, err
}

// 目前go不支持泛型方法，这里曲线救国下
func GetJson[ServiceInfo any, T any](hb *HttpBackend[ServiceInfo], ctx context.Context, serviceName, serviceId, path string, body interface{}, headers map[string]string) (int, *T, error) {
	service := hb.GetService(serviceName, serviceId)
	if service != nil {
		return httprequest.JsonRequest[T](ctx, "get", service.Address()+path, body, headers)
	}
	err := fmt.Errorf("not find HttpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("path", path).Msg("HttpServiceBackend GetJson error")
	return http.StatusNotFound, nil, err
}
func PostJson[ServiceInfo any, T any](hb *HttpBackend[ServiceInfo], ctx context.Context, serviceName, serviceId, path string, body interface{}, headers map[string]string) (int, *T, error) {
	service := hb.GetService(serviceName, serviceId)
	if service != nil {
		return httprequest.JsonRequest[T](ctx, "post", service.Address()+path, body, headers)
	}
	err := fmt.Errorf("not find HttpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("path", path).Msg("HttpServiceBackend GetJson error")
	return http.StatusNotFound, nil, err
}

// 服务器发现 更新逻辑
func (hb *HttpBackend[ServiceInfo]) updateServices(confs []*ServiceConfig) {
	// 转化成去空格的小写
	for _, conf := range confs {
		conf.ServiceName = strings.TrimSpace(strings.ToLower(conf.ServiceName))
		conf.ServiceId = strings.TrimSpace(strings.ToLower(conf.ServiceId))
		for i := 0; i < len(conf.RoutingTag); i++ {
			conf.RoutingTag[i] = strings.TrimSpace(strings.ToLower(conf.RoutingTag[i]))
		}
	}

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
