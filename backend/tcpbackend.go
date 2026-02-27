package backend

// https://github.com/yuwf/gobase

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/yuwf/gobase/msger"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

// ServiceInfo是和业务相关的连接端自定义信息结构
type TcpBackend[ServiceInfo any] struct {
	// 消息分发
	*msger.MsgDispatch

	groupMutex sync.RWMutex                      // 注意只保护group的变化 不要保护group内的操作
	group      map[string]*TcpGroup[ServiceInfo] // 所有的服务器组 锁保护

	event   TcpEvent[ServiceInfo] // 事件处理
	watcher interface{}           // 服务器发现相关的对象 consul或者redis对象
	// 请求处理完后回调 不使用锁，默认要求提前注册好
	hook []TCPHook[ServiceInfo]

	// 记录每个组的版本号，版本号包括配置版本号，连接版本号，登录版本号
	// 记录在TcpBackend是保证group消失了版本号还要存在
	versionMutex         sync.RWMutex
	groupServicesVersion map[string]int64
	groupConnVersion     map[string]int64
	groupLoginVersion    map[string]int64
}

// 注册hook
func (tb *TcpBackend[ServiceInfo]) RegHook(h TCPHook[ServiceInfo]) {
	tb.hook = append(tb.hook, h)
}

func (tb *TcpBackend[ServiceInfo]) GetGroup(serviceName string) *TcpGroup[ServiceInfo] {
	serviceName = strings.TrimSpace(strings.ToLower(serviceName))
	tb.groupMutex.RLock()
	defer tb.groupMutex.RUnlock()
	group, ok := tb.group[serviceName]
	if ok {
		return group
	}
	return nil
}

func (tb *TcpBackend[ServiceInfo]) GetGroups() []*TcpGroup[ServiceInfo] {
	tb.groupMutex.RLock()
	defer tb.groupMutex.RUnlock()
	gs := make([]*TcpGroup[ServiceInfo], 0, len(tb.group))
	for _, group := range tb.group {
		gs = append(gs, group)
	}
	return gs
}

func (tb *TcpBackend[ServiceInfo]) GetServices() []*TcpService[ServiceInfo] {
	gs := tb.GetGroups()
	// 防止ss拷贝，弄的一个中间变量sss
	sss := make([][]*TcpService[ServiceInfo], len(gs))
	l := 0
	for i, group := range gs {
		sss[i] = group.GetServices()
		l += len(sss[i])
	}
	ss := make([]*TcpService[ServiceInfo], 0, l)
	for _, s := range sss {
		ss = append(ss, s...)
	}
	return ss
}

func (tb *TcpBackend[ServiceInfo]) GetService(serviceName, serviceId string) *TcpService[ServiceInfo] {
	group := tb.GetGroup(serviceName)
	if group != nil {
		return group.GetService(serviceId)
	}
	return nil
}

// 根据哈hash获取，获取的指定状态的服务
// hash可以用用户id或者其他稳定的数据
// status有效值 TcpStatus_All、TcpStatus_Conned、TcpStatus_Logined
func (tb *TcpBackend[ServiceInfo]) GetServiceByHash(serviceName, hash string, status int) *TcpService[ServiceInfo] {
	group := tb.GetGroup(serviceName)
	if group != nil {
		return group.GetServiceByHash(hash, status)
	}
	return nil
}

// 根据hash和tag环获取，获取的指定状态的服务
// hash可以用用户id或者其他稳定的数据
// status有效值 TcpStatus_All、TcpStatus_Conned、TcpStatus_Logined
func (tb *TcpBackend[ServiceInfo]) GetServiceByTagAndHash(serviceName, tag, hash string, status int) *TcpService[ServiceInfo] {
	group := tb.GetGroup(serviceName)
	if group != nil {
		return group.GetServiceByTagAndHash(tag, hash, status)
	}
	return nil
}

// 发消息，指定serviceId的
func (tb *TcpBackend[ServiceInfo]) Send(ctx context.Context, serviceName, serviceId string, buf []byte) error {
	service := tb.GetService(serviceName, serviceId)
	if service != nil {
		return service.Send(ctx, buf)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	utils.LogCtx(log.Error(), ctx).Err(err).Int("size", len(buf)).Msg("TcpServiceBackend Send error")
	return err
}

// 发消息，指定serviceId的
func (tb *TcpBackend[ServiceInfo]) SendMsg(ctx context.Context, serviceName, serviceId string, msg msger.Msger) error {
	service := tb.GetService(serviceName, serviceId)
	if service != nil {
		return service.SendMsg(ctx, msg)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("msger", msg).Msg("TcpServiceBackend SendMsg error")
	return err
}

// 通过hash发消息，只发送给指定状态的服务
// hash可以用用户id或者其他稳定的数据
// status有效值 TcpStatus_All、TcpStatus_Conned、TcpStatus_Logined
func (tb *TcpBackend[ServiceInfo]) SendByHash(ctx context.Context, serviceName, hash string, buf []byte, status int) error {
	service := tb.GetServiceByHash(serviceName, hash, status)
	if service != nil {
		return service.Send(ctx, buf)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s hash=%s", serviceName, hash)
	utils.LogCtx(log.Error(), ctx).Err(err).Int("size", len(buf)).Msg("TcpServiceBackend SendByHash error")
	return err
}

// 通过hash发消息，只发送给指定状态的服务
// hash可以用用户id或者其他稳定的数据
// status有效值 TcpStatus_All、TcpStatus_Conned、TcpStatus_Logined
func (tb *TcpBackend[ServiceInfo]) SendMsgByHash(ctx context.Context, serviceName, hash string, msg msger.Msger, status int) error {
	service := tb.GetServiceByHash(serviceName, hash, status)
	if service != nil {
		return service.SendMsg(ctx, msg)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s hash=%s", serviceName, hash)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("msger", msg).Msg("TcpServiceBackend SendMsgByHash error")
	return err
}

// 通过hash和tag发消息，只发送给指定状态的服务
// hash可以用用户id或者其他稳定的数据
// status有效值 TcpStatus_All、TcpStatus_Conned、TcpStatus_Logined
func (tb *TcpBackend[ServiceInfo]) SendByTagAndHash(ctx context.Context, serviceName, tag, hash string, buf []byte, status int) error {
	service := tb.GetServiceByTagAndHash(serviceName, tag, hash, status)
	if service != nil {
		return service.Send(ctx, buf)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s tag=%s hash=%s", serviceName, tag, hash)
	utils.LogCtx(log.Error(), ctx).Err(err).Int("size", len(buf)).Msg("TcpServiceBackend SendByTagAndHash error")
	return err
}

// 通过hash和tag发消息，只发送给指定状态的服务
// hash可以用用户id或者其他稳定的数据
// status有效值 TcpStatus_Conned、TcpStatus_Logined
func (tb *TcpBackend[ServiceInfo]) SendMsgByTagAndHash(ctx context.Context, serviceName, tag, hash string, msg msger.Msger, status int) error {
	service := tb.GetServiceByTagAndHash(serviceName, tag, hash, status)
	if service != nil {
		return service.SendMsg(ctx, msg)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s tag=%s hash=%s", serviceName, tag, hash)
	utils.LogCtx(log.Error(), ctx).Err(err).Interface("msger", msg).Msg("TcpServiceBackend SendMsgByTagAndHash error")
	return err
}

// 向所有的TcpService发消息发送消息
// status有效值 TcpStatus_All、TcpStatus_Conned、TcpStatus_Logined
func (tb *TcpBackend[ServiceInfo]) Broad(ctx context.Context, buf []byte, status int) {
	gs := tb.GetGroups()
	for _, group := range gs {
		ss := group.GetServices()
		for _, service := range ss {
			s, _ := service.HealthStatus()
			if status == s {
				service.Send(ctx, buf)
			}
		}
	}
}

// 向所有的TcpService发消息发送消息
// status有效值 TcpStatus_Conned、TcpStatus_Logined
func (tb *TcpBackend[ServiceInfo]) BroadMsg(ctx context.Context, msg msger.Msger, status int) {
	gs := tb.GetGroups()
	for _, group := range gs {
		ss := group.GetServices()
		for _, service := range ss {
			s, _ := service.HealthStatus()
			if status == s {
				service.SendMsg(ctx, msg)
			}
		}
	}
}

// 向Group中的所有TcpService发送消息
// status有效值 TcpStatus_Conned、TcpStatus_Logined
func (tb *TcpBackend[ServiceInfo]) BroadGroup(ctx context.Context, serviceName string, buf []byte, status int) {
	group := tb.GetGroup(serviceName)
	if group != nil {
		ss := group.GetServices()
		for _, service := range ss {
			s, _ := service.HealthStatus()
			if status == s {
				service.Send(ctx, buf)
			}
		}
	}
}

// 向Group中的所有TcpService发送消息
// status有效值 TcpStatus_Conned、TcpStatus_Logined
func (tb *TcpBackend[ServiceInfo]) BroadGroupMsg(ctx context.Context, serviceName string, msg msger.Msger, status int) {
	group := tb.GetGroup(serviceName)
	if group != nil {
		ss := group.GetServices()
		for _, service := range ss {
			s, _ := service.HealthStatus()
			if status == s {
				service.SendMsg(ctx, msg)
			}
		}
	}
}

// 向每个组中的其中一个TcpService发消息
// status有效值 TcpStatus_Conned、TcpStatus_Logined
func (tb *TcpBackend[ServiceInfo]) BroadMsgByHash(ctx context.Context, hash string, msg msger.Msger, status int) {
	gs := tb.GetGroups()
	for _, group := range gs {
		service := group.GetServiceByHash(hash, status)
		if service != nil {
			service.SendMsg(ctx, msg)
		}
	}
}

// 向每个组中的指定的tag组中的其中一个TcpService发消息
// status有效值 TcpStatus_Conned、TcpStatus_Logined
func (tb *TcpBackend[ServiceInfo]) BroadMsgByTagAndHash(ctx context.Context, tag, hash string, msg msger.Msger, status int) {
	gs := tb.GetGroups()
	for _, group := range gs {
		service := group.GetServiceByTagAndHash(tag, hash, status)
		if service != nil {
			service.SendMsg(ctx, msg)
		}
	}
}

// 服务器发现 更新逻辑
func (tb *TcpBackend[ServiceInfo]) UpdateServices(confs []*ServiceConfig) {
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

	// 如果serviceConfsMap中不存在group已经存在的组 填充一个空的 方便后面空判断时做删除
	{
		tb.groupMutex.RLock()
		for serviceName := range tb.group {
			_, ok := confsMap[serviceName]
			if !ok {
				confsMap[serviceName] = ServiceIdConfMap{}
			}
		}
		tb.groupMutex.RUnlock()
	}

	for serviceName, confs := range confsMap {
		group := tb.GetGroup(serviceName)
		if group == nil {
			if len(confs) == 0 {
				continue
			}
			// 如果之前不存在 创建一个
			group = NewTcpGroup(serviceName, tb)
			tb.groupMutex.Lock()
			tb.group[serviceName] = group
			tb.groupMutex.Unlock()
		}
		// 更新组
		count, addCount, modifyCount, removeCount := group.update(confs)
		// 更新版本号
		if addCount > 0 || modifyCount > 0 || removeCount > 0 {
			tb.addServiceVersion(serviceName)
		}
		// 如果服务器为空了
		if count == 0 {
			tb.groupMutex.Lock()
			delete(tb.group, serviceName)
			tb.groupMutex.Unlock()
		}
	}
}

// 版本号相关接口
func (tb *TcpBackend[ServiceInfo]) GetServiceVersion(serviceName string) int64 {
	serviceName = strings.TrimSpace(strings.ToLower(serviceName))
	return tb.getServiceVersion(serviceName)
}

func (tb *TcpBackend[ServiceInfo]) GetConnVersion(serviceName string) int64 {
	serviceName = strings.TrimSpace(strings.ToLower(serviceName))
	return tb.getConnVersion(serviceName)
}

func (tb *TcpBackend[ServiceInfo]) GetLoginVersion(serviceName string) int64 {
	serviceName = strings.TrimSpace(strings.ToLower(serviceName))
	return tb.getLoginVersion(serviceName)
}

func (tb *TcpBackend[ServiceInfo]) getServiceVersion(serviceName string) int64 {
	tb.versionMutex.RLock()
	defer tb.versionMutex.RUnlock()
	if version, ok := tb.groupServicesVersion[serviceName]; ok {
		return version
	}
	return -1
}

func (tb *TcpBackend[ServiceInfo]) addServiceVersion(serviceName string) {
	tb.versionMutex.Lock()
	defer tb.versionMutex.Unlock()
	tb.groupServicesVersion[serviceName]++
}

func (tb *TcpBackend[ServiceInfo]) getConnVersion(serviceName string) int64 {
	tb.versionMutex.RLock()
	defer tb.versionMutex.RUnlock()
	if version, ok := tb.groupConnVersion[serviceName]; ok {
		return version
	}
	return -1
}
func (tb *TcpBackend[ServiceInfo]) addConnVersion(serviceName string) {
	tb.versionMutex.Lock()
	defer tb.versionMutex.Unlock()
	tb.groupConnVersion[serviceName]++
}

func (tb *TcpBackend[ServiceInfo]) getLoginVersion(serviceName string) int64 {
	tb.versionMutex.RLock()
	defer tb.versionMutex.RUnlock()
	if version, ok := tb.groupLoginVersion[serviceName]; ok {
		return version
	}
	return -1
}
func (tb *TcpBackend[ServiceInfo]) addLoginVersion(serviceName string) {
	tb.versionMutex.Lock()
	defer tb.versionMutex.Unlock()
	tb.groupLoginVersion[serviceName]++
}
