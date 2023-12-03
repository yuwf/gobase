package backend

// https://github.com/yuwf/gobase

import (
	"fmt"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
)

// T是和业务相关的客户端信息结构 透传给TcpService
type TcpBackend[T any] struct {
	sync.RWMutex                         // 注意只保护group的变化 不要保护group内的操作
	group        map[string]*TcpGroup[T] // 所有的服务器组 锁保护
	event        TcpEvent[T]             // 事件处理
	watcher      interface{}             // 服务器发现相关的对象 consul或者redis对象
	// 请求处理完后回调 不使用锁，默认要求提前注册好
	hook []TCPHook[T]
}

// 注册hook
func (tb *TcpBackend[T]) RegHook(h TCPHook[T]) {
	tb.hook = append(tb.hook, h)
}

func (tb *TcpBackend[T]) GetGroup(serviceName string) *TcpGroup[T] {
	serviceName = strings.TrimSpace(strings.ToLower(serviceName))
	tb.RLock()
	defer tb.RUnlock()
	group, ok := tb.group[serviceName]
	if ok {
		return group
	}
	return nil
}

func (tb *TcpBackend[T]) GetGroups() map[string]*TcpGroup[T] {
	tb.RLock()
	defer tb.RUnlock()
	gs := map[string]*TcpGroup[T]{}
	for serviceName, group := range tb.group {
		gs[serviceName] = group
	}
	return gs
}

func (tb *TcpBackend[T]) GetService(serviceName, serviceId string) *TcpService[T] {
	group := tb.GetGroup(serviceName)
	if group != nil {
		return group.GetService(serviceId)
	}
	return nil
}

// 根据哈希环获取,哈希环行记录的都是连接成功的
func (tb *TcpBackend[T]) GetServiceByHash(serviceName, hash string) *TcpService[T] {
	group := tb.GetGroup(serviceName)
	if group != nil {
		return group.GetServiceByHash(hash)
	}
	return nil
}

// 根据哈希环获取,哈希环行记录的都是连接成功的
func (tb *TcpBackend[T]) GetServiceByTagAndHash(serviceName, tag, hash string) *TcpService[T] {
	group := tb.GetGroup(serviceName)
	if group != nil {
		return group.GetServiceByTagAndHash(tag, hash)
	}
	return nil
}

// 向TcpService发消息，指定serviceId的
func (tb *TcpBackend[T]) Send(serviceName string, serviceId string, buf []byte) error {
	service := tb.GetService(serviceName, serviceId)
	if service != nil {
		return service.Send(buf)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	log.Error().Err(err).Int("size", len(buf)).Msg("TcpServiceBackend Send error")
	return err
}

func (tb *TcpBackend[T]) SendMsg(serviceName string, serviceId string, msg interface{}) error {
	service := tb.GetService(serviceName, serviceId)
	if service != nil {
		return service.SendMsg(msg)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s serviceId=%s", serviceName, serviceId)
	log.Error().Err(err).Interface("desc", msg).Msg("TcpServiceBackend SendMsg error")
	return err
}

// 向TcpGroup发消息，使用哈希获取service
func (tb *TcpBackend[T]) SendByHash(serviceName, hash string, buf []byte) error {
	service := tb.GetServiceByHash(serviceName, hash)
	if service != nil {
		return service.Send(buf)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s hash=%s", serviceName, hash)
	log.Error().Err(err).Int("size", len(buf)).Msg("TcpServiceBackend SendByHash error")
	return err
}

func (tb *TcpBackend[T]) SendMsgByHash(serviceName string, hash string, msg interface{}) error {
	service := tb.GetServiceByHash(serviceName, hash)
	if service != nil {
		return service.SendMsg(msg)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s hash=%s", serviceName, hash)
	log.Error().Err(err).Interface("desc", msg).Msg("TcpServiceBackend SendMsgByHash error")
	return err
}

// 向TcpGroup发消息，使用哈希获取service
func (tb *TcpBackend[T]) SendByTagAndHash(serviceName, tag, hash string, buf []byte) error {
	service := tb.GetServiceByTagAndHash(serviceName, tag, hash)
	if service != nil {
		return service.Send(buf)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s tag=%s hash=%s", serviceName, tag, hash)
	log.Error().Err(err).Int("size", len(buf)).Msg("TcpServiceBackend SendByTagAndHash error")
	return err
}

func (tb *TcpBackend[T]) SendMsgByTagAndHash(serviceName, tag, hash string, msg interface{}) error {
	service := tb.GetServiceByTagAndHash(serviceName, tag, hash)
	if service != nil {
		return service.SendMsg(msg)
	}
	err := fmt.Errorf("not find TcpService, serviceName=%s tag=%s hash=%s", serviceName, tag, hash)
	log.Error().Err(err).Interface("desc", msg).Msg("TcpServiceBackend SendMsgByTagAndHash error")
	return err
}

// 向所有的TcpService发消息发送消息
func (tb *TcpBackend[T]) Broad(buf []byte) {
	gs := tb.GetGroups()
	for _, group := range gs {
		group.BroadMsg(buf)
	}
}

func (tb *TcpBackend[T]) BroadMsg(msg interface{}) {
	gs := tb.GetGroups()
	for _, group := range gs {
		group.BroadMsg(msg)
	}
}

// 向Group中的所有TcpService发送消息
func (tb *TcpBackend[T]) BroadGroup(serviceName string, buf []byte) {
	group := tb.GetGroup(serviceName)
	if group != nil {
		group.Broad(buf)
	}
}

func (tb *TcpBackend[T]) BroadGroupMsg(serviceName string, msg interface{}) {
	group := tb.GetGroup(serviceName)
	if group != nil {
		group.BroadMsg(msg)
	}
}

// 向每个组中的其中一个TcpService发消息，使用哈希获取service
func (tb *TcpBackend[T]) BroadMsgByHash(hash string, msg interface{}) {
	gs := tb.GetGroups()
	for _, group := range gs {
		service := group.GetServiceByHash(hash)
		if service != nil {
			service.SendMsg(msg)
		}
	}
}

// 向每个组中的指定的tag组中的其中一个TcpService发消息，使用哈希获取service
func (tb *TcpBackend[T]) BroadMsgByTagAndHash(tag, hash string, msg interface{}) {
	gs := tb.GetGroups()
	for _, group := range gs {
		service := group.GetServiceByTagAndHash(tag, hash)
		if service != nil {
			service.SendMsg(msg)
		}
	}
}

// 服务器发现 更新逻辑
func (tb *TcpBackend[T]) updateServices(confs []*ServiceConfig) {
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
