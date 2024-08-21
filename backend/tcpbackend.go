package backend

// https://github.com/yuwf/gobase

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

// ServiceInfo是和业务相关的客户端信息结构 透传给TcpService
type TcpBackend[ServiceInfo any] struct {
	sync.RWMutex                                   // 注意只保护group的变化 不要保护group内的操作
	group        map[string]*TcpGroup[ServiceInfo] // 所有的服务器组 锁保护
	event        TcpEvent[ServiceInfo]             // 事件处理
	watcher      interface{}                       // 服务器发现相关的对象 consul或者redis对象
	// 请求处理完后回调 不使用锁，默认要求提前注册好
	hook []TCPHook[ServiceInfo]
}

// 注册hook
func (tb *TcpBackend[ServiceInfo]) RegHook(h TCPHook[ServiceInfo]) {
	tb.hook = append(tb.hook, h)
}

func (tb *TcpBackend[ServiceInfo]) GetGroup(serviceName string) *TcpGroup[ServiceInfo] {
	serviceName = strings.TrimSpace(strings.ToLower(serviceName))
	tb.RLock()
	defer tb.RUnlock()
	group, ok := tb.group[serviceName]
	if ok {
		return group
	}
	return nil
}

func (tb *TcpBackend[ServiceInfo]) GetGroups() map[string]*TcpGroup[ServiceInfo] {
	tb.RLock()
	defer tb.RUnlock()
	gs := map[string]*TcpGroup[ServiceInfo]{}
	for serviceName, group := range tb.group {
		gs[serviceName] = group
	}
	return gs
}

func (tb *TcpBackend[ServiceInfo]) GetService(serviceName, serviceId string) *TcpService[ServiceInfo] {
	group := tb.GetGroup(serviceName)
	if group != nil {
		return group.GetService(serviceId)
	}
	return nil
}

// 根据哈希环获取,哈希环行记录的都是连接成功的
func (tb *TcpBackend[ServiceInfo]) GetServiceByHash(serviceName, hash string) *TcpService[ServiceInfo] {
	group := tb.GetGroup(serviceName)
	if group != nil {
		return group.GetServiceByHash(hash)
	}
	return nil
}

// 根据哈希环获取,哈希环行记录的都是连接成功的
func (tb *TcpBackend[ServiceInfo]) GetServiceByTagAndHash(serviceName, tag, hash string) *TcpService[ServiceInfo] {
	group := tb.GetGroup(serviceName)
	if group != nil {
		return group.GetServiceByTagAndHash(tag, hash)
	}
	return nil
}

// 向TcpService发消息，指定serviceId的
func (tb *TcpBackend[ServiceInfo]) Send(ctx context.Context, serviceName, serviceId string, buf []byte) error {
	service := tb.GetService(serviceName, serviceId)
	if service != nil {
		return service.Send(ctx, buf)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	utils.LogCtx(log.Error(), ctx).Err(err).Int("size", len(buf)).Msg("TcpServiceBackend Send error")
	return err
}

func (tb *TcpBackend[ServiceInfo]) SendMsg(ctx context.Context, serviceName, serviceId string, msg utils.SendMsger) error {
	service := tb.GetService(serviceName, serviceId)
	if service != nil {
		return service.SendMsg(ctx, msg)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("Msg", msg).Msg("TcpServiceBackend SendMsg error")
	return err
}

// 向TcpGroup发消息，使用哈希获取service
func (tb *TcpBackend[ServiceInfo]) SendByHash(ctx context.Context, serviceName, hash string, buf []byte) error {
	service := tb.GetServiceByHash(serviceName, hash)
	if service != nil {
		return service.Send(ctx, buf)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s hash=%s", serviceName, hash)
	utils.LogCtx(log.Error(), ctx).Err(err).Int("size", len(buf)).Msg("TcpServiceBackend SendByHash error")
	return err
}

func (tb *TcpBackend[ServiceInfo]) SendMsgByHash(ctx context.Context, serviceName, hash string, msg utils.SendMsger) error {
	service := tb.GetServiceByHash(serviceName, hash)
	if service != nil {
		return service.SendMsg(ctx, msg)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s hash=%s", serviceName, hash)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("Msg", msg).Msg("TcpServiceBackend SendMsgByHash error")
	return err
}

// 向TcpGroup发消息，使用哈希获取service
func (tb *TcpBackend[ServiceInfo]) SendByTagAndHash(ctx context.Context, serviceName, tag, hash string, buf []byte) error {
	service := tb.GetServiceByTagAndHash(serviceName, tag, hash)
	if service != nil {
		return service.Send(ctx, buf)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s tag=%s hash=%s", serviceName, tag, hash)
	utils.LogCtx(log.Error(), ctx).Err(err).Int("size", len(buf)).Msg("TcpServiceBackend SendByTagAndHash error")
	return err
}

func (tb *TcpBackend[ServiceInfo]) SendMsgByTagAndHash(ctx context.Context, serviceName, tag, hash string, msg utils.SendMsger) error {
	service := tb.GetServiceByTagAndHash(serviceName, tag, hash)
	if service != nil {
		return service.SendMsg(ctx, msg)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s tag=%s hash=%s", serviceName, tag, hash)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("Msg", msg).Msg("TcpServiceBackend SendMsgByTagAndHash error")
	return err
}

// 向所有的TcpService发消息发送消息
func (tb *TcpBackend[ServiceInfo]) Broad(ctx context.Context, buf []byte) {
	gs := tb.GetGroups()
	for _, group := range gs {
		group.Broad(ctx, buf)
	}
}

func (tb *TcpBackend[ServiceInfo]) BroadMsg(ctx context.Context, msg utils.SendMsger) {
	gs := tb.GetGroups()
	for _, group := range gs {
		group.BroadMsg(ctx, msg)
	}
}

// 向Group中的所有TcpService发送消息
func (tb *TcpBackend[ServiceInfo]) BroadGroup(ctx context.Context, serviceName string, buf []byte) {
	group := tb.GetGroup(serviceName)
	if group != nil {
		group.Broad(ctx, buf)
	}
}

func (tb *TcpBackend[ServiceInfo]) BroadGroupMsg(ctx context.Context, serviceName string, msg utils.SendMsger) {
	group := tb.GetGroup(serviceName)
	if group != nil {
		group.BroadMsg(ctx, msg)
	}
}

// 向每个组中的其中一个TcpService发消息，使用哈希获取service
func (tb *TcpBackend[ServiceInfo]) BroadMsgByHash(ctx context.Context, hash string, msg utils.SendMsger) {
	gs := tb.GetGroups()
	for _, group := range gs {
		service := group.GetServiceByHash(hash)
		if service != nil {
			service.SendMsg(ctx, msg)
		}
	}
}

// 向每个组中的指定的tag组中的其中一个TcpService发消息，使用哈希获取service
func (tb *TcpBackend[ServiceInfo]) BroadMsgByTagAndHash(ctx context.Context, tag, hash string, msg utils.SendMsger) {
	gs := tb.GetGroups()
	for _, group := range gs {
		service := group.GetServiceByTagAndHash(tag, hash)
		if service != nil {
			service.SendMsg(ctx, msg)
		}
	}
}

// 服务器发现 更新逻辑
func (tb *TcpBackend[ServiceInfo]) updateServices(confs []*ServiceConfig) {
	// 转化成去空格的小写
	for _, conf := range confs {
		conf.ServiceName = strings.TrimSpace(strings.ToLower(conf.ServiceName))
		conf.ServiceId = strings.TrimSpace(strings.ToLower(conf.ServiceId))
		conf.RoutingTag = strings.TrimSpace(strings.ToLower(conf.RoutingTag))
	}

	// 组织成Map结构
	confsMap := ServiceNameConfMap{}
	for _, conf := range confs {
		if confsMap[conf.ServiceName] == nil {
			confsMap[conf.ServiceName] = ServiceIdConfMap{}
		}
		confsMap[conf.ServiceName][conf.ServiceId] = conf
	}

	// 如果serviceConfsMap中不存在group已经存在的组 填充一个空的 方便后面空判断时做删除
	{
		tb.RLock()
		for serviceName := range tb.group {
			_, ok := confsMap[serviceName]
			if !ok {
				confsMap[serviceName] = ServiceIdConfMap{}
			}
		}
		tb.RUnlock()
	}

	for serviceName, confs := range confsMap {
		group := tb.GetGroup(serviceName)
		if group == nil {
			if len(confs) == 0 {
				continue
			}
			// 如果之前不存在 创建一个
			group = NewTcpGroup(serviceName, tb)
			tb.Lock()
			tb.group[serviceName] = group
			tb.Unlock()
		}
		// 更新组
		count := group.update(confs)
		// 如果服务器为空了
		if count == 0 {
			tb.Lock()
			delete(tb.group, serviceName)
			tb.Unlock()
		}
	}
}
