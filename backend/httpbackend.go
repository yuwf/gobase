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
	groupMutex sync.RWMutex                       // 注意只保护group的变化 不要保护group内的操作
	group      map[string]*HttpGroup[ServiceInfo] // 服务器组map对象

	event   HttpEvent[ServiceInfo] // 事件处理
	watcher interface{}            // 服务器发现相关的对象 consul或者redis对象
	// 请求处理完后回调 不使用锁，默认要求提前注册好
	hook []HttpHook[ServiceInfo]

	// 记录每个组的版本号，版本号包括配置版本号，连接版本号
	// 记录在TcpBackend是保证group消失了版本号还要存在
	versionMutex         sync.RWMutex
	groupServicesVersion map[string]int64
	groupConnVersion     map[string]int64
}

// 注册hook
func (hb *HttpBackend[ServiceInfo]) RegHook(h HttpHook[ServiceInfo]) {
	hb.hook = append(hb.hook, h)
}

func (hb *HttpBackend[ServiceInfo]) GetGroup(serviceName string) *HttpGroup[ServiceInfo] {
	serviceName = strings.TrimSpace(strings.ToLower(serviceName))
	hb.groupMutex.RLock()
	defer hb.groupMutex.RUnlock()
	group, ok := hb.group[serviceName]
	if ok {
		return group
	}
	return nil
}

func (hb *HttpBackend[ServiceInfo]) GetGroups() map[string]*HttpGroup[ServiceInfo] {
	hb.groupMutex.RLock()
	defer hb.groupMutex.RUnlock()
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

// 根据哈hash获取，获取的指定状态的服务
// hash可以用用户id或者其他稳定的数据
// status有效值 HttpStatus_All、HttpStatus_Conned
func (hb *HttpBackend[ServiceInfo]) GetServiceByHash(serviceName, hash string, status int) *HttpService[ServiceInfo] {
	group := hb.GetGroup(serviceName)
	if group != nil {
		return group.GetServiceByHash(hash, status)
	}
	return nil
}

// 根据hash和tag环获取，获取的指定状态的服务
// hash可以用用户id或者其他稳定的数据
// status有效值 HttpStatus_All、HttpStatus_Conned
func (hb *HttpBackend[ServiceInfo]) GetServiceByTagAndHash(serviceName, tag, hash string, status int) *HttpService[ServiceInfo] {
	group := hb.GetGroup(serviceName)
	if group != nil {
		return group.GetServiceByTagAndHash(tag, hash, status)
	}
	return nil
}

func (hb *HttpBackend[ServiceInfo]) Get(ctx context.Context, serviceName, serviceId, path string, body []byte, headers map[string]string) (int, []byte, error) {
	service := hb.GetService(serviceName, serviceId)
	if service != nil {
		return httprequest.Request(ctx, http.MethodGet, service.Address()+path, body, headers)
	}
	err := fmt.Errorf("not find HttpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("path", path).Msg("HttpServiceBackend Get error")
	return http.StatusNotFound, nil, err
}

func (hb *HttpBackend[ServiceInfo]) Post(ctx context.Context, serviceName, serviceId, path string, body []byte, headers map[string]string) (int, []byte, error) {
	service := hb.GetService(serviceName, serviceId)
	if service != nil {
		return httprequest.Request(ctx, http.MethodPost, service.Address()+path, body, headers)
	}
	err := fmt.Errorf("not find HttpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("path", path).Msg("HttpServiceBackend Post error")
	return http.StatusNotFound, nil, err
}

// 目前go不支持泛型方法，这里曲线救国下
func GetJson[ServiceInfo any, T any](hb *HttpBackend[ServiceInfo], ctx context.Context, serviceName, serviceId, path string, body interface{}, headers map[string]string) (int, *T, error) {
	service := hb.GetService(serviceName, serviceId)
	if service != nil {
		return httprequest.JsonRequest[T](ctx, http.MethodGet, service.Address()+path, body, headers)
	}
	err := fmt.Errorf("not find HttpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("path", path).Msg("HttpServiceBackend GetJson error")
	return http.StatusNotFound, nil, err
}
func PostJson[ServiceInfo any, T any](hb *HttpBackend[ServiceInfo], ctx context.Context, serviceName, serviceId, path string, body interface{}, headers map[string]string) (int, *T, error) {
	service := hb.GetService(serviceName, serviceId)
	if service != nil {
		return httprequest.JsonRequest[T](ctx, http.MethodPost, service.Address()+path, body, headers)
	}
	err := fmt.Errorf("not find HttpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("path", path).Msg("HttpServiceBackend PostJson error")
	return http.StatusNotFound, nil, err
}

// 服务器发现 更新逻辑
func (hb *HttpBackend[ServiceInfo]) updateServices(confs []*ServiceConfig) {
	// 标准化配置
	for _, conf := range confs {
		conf.normalize()
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
		hb.groupMutex.RLock()
		for serviceName := range hb.group {
			_, ok := confsMap[serviceName]
			if !ok {
				confsMap[serviceName] = ServiceIdConfMap{}
			}
		}
		hb.groupMutex.RUnlock()
	}

	for serviceName, confs := range confsMap {
		group := hb.GetGroup(serviceName)
		if group == nil {
			if len(confs) == 0 {
				continue
			}
			// 如果之前不存在 创建一个
			group = NewHttpGroup(serviceName, hb)
			hb.groupMutex.Lock()
			hb.group[serviceName] = group
			hb.groupMutex.Unlock()
		}
		// 更新组
		count, addCount, modifyCount, removeCount := group.update(confs, hb.event)
		// 更新版本号
		if addCount > 0 || modifyCount > 0 || removeCount > 0 {
			hb.addServiceVersion(serviceName)
		}
		// 如果存在，配置为空了，删除
		if count == 0 {
			hb.groupMutex.Lock()
			delete(hb.group, serviceName)
			hb.groupMutex.Unlock()
		}
	}
}

// 版本号相关接口
func (hb *HttpBackend[ServiceInfo]) GetServiceVersion(serviceName string) int64 {
	serviceName = strings.TrimSpace(strings.ToLower(serviceName))
	return hb.getServiceVersion(serviceName)
}

func (hb *HttpBackend[ServiceInfo]) GetConnVersion(serviceName string) int64 {
	serviceName = strings.TrimSpace(strings.ToLower(serviceName))
	return hb.getConnVersion(serviceName)
}

func (hb *HttpBackend[ServiceInfo]) getServiceVersion(serviceName string) int64 {
	hb.versionMutex.RLock()
	defer hb.versionMutex.RUnlock()
	if version, ok := hb.groupServicesVersion[serviceName]; ok {
		return version
	}
	return -1
}

func (hb *HttpBackend[ServiceInfo]) addServiceVersion(serviceName string) {
	hb.versionMutex.Lock()
	defer hb.versionMutex.Unlock()
	hb.groupServicesVersion[serviceName]++
}

func (hb *HttpBackend[ServiceInfo]) getConnVersion(serviceName string) int64 {
	hb.versionMutex.RLock()
	defer hb.versionMutex.RUnlock()
	if version, ok := hb.groupConnVersion[serviceName]; ok {
		return version
	}
	return -1
}
func (hb *HttpBackend[ServiceInfo]) addConnVersion(serviceName string) {
	hb.versionMutex.Lock()
	defer hb.versionMutex.Unlock()
	hb.groupConnVersion[serviceName]++
}
