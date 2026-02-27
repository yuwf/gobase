package backend

// https://github.com/yuwf/gobase

import (
	"sort"
	"strings"

	"github.com/yuwf/gobase/consul"
	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/msger"
	"github.com/yuwf/gobase/nacos"
)

// 服务配置
type ServiceConfig struct {
	// 要求字符串类型的字段小写且去掉前后空格
	ServiceName string            `json:"servicename,omitempty"` // 服务器类型名，用来分组【内部会转化成去空格的小写】
	ServiceId   string            `json:"serviceid,omitempty"`   // 服务器唯一ID【内部会转化成去空格的小写】
	ServiceAddr string            `json:"serviceaddr,omitempty"` // 服务器对外暴露的地址
	ServicePort int               `json:"serviceport,omitempty"` // 服务器对外暴露的端口
	Metadata    map[string]string `json:"metadata,omitempty"`    // 配置的元数据
	RoutingTag  []string          `json:"routertag,omitempty"`   // 支持路由tag 可以将服务器分到多个组中【内部会转化成去空格的小写并去重排序】
}

func (sc *ServiceConfig) normalize() {
	sc.ServiceName = strings.TrimSpace(strings.ToLower(sc.ServiceName))
	sc.ServiceId = strings.TrimSpace(strings.ToLower(sc.ServiceId))
	if len(sc.RoutingTag) > 0 {
		m := make(map[string]struct{}, len(sc.RoutingTag))
		for _, v := range sc.RoutingTag {
			tag := strings.ToLower(strings.TrimSpace(v))
			if tag == "" {
				continue
			}
			m[tag] = struct{}{}
		}
		sc.RoutingTag = sc.RoutingTag[:0]
		for k := range m {
			sc.RoutingTag = append(sc.RoutingTag, k)
		}
		sort.Strings(sc.RoutingTag)
	}
}

type ServiceIdConfMap = map[string]*ServiceConfig
type ServiceNameConfMap = map[string]ServiceIdConfMap

// ServiceInfo中可选择实现的函数
type ServiceCreater interface {
	ServiceCreate(conf *ServiceConfig)
}

// 创建TcpBackend，使用外层自己调用UpdateServices做服务器发现
// Msg表示消息类型，必须实现util.Msger接口，否则消息无法分发
func NewTcpBackend[ServiceInfo any, Msg any](event TcpEvent[ServiceInfo]) (*TcpBackend[ServiceInfo], error) {
	md, err := msger.NewMsgDispatch[Msg, TcpService[ServiceInfo]]()
	if err != nil {
		return nil, err
	}
	tb := &TcpBackend[ServiceInfo]{
		MsgDispatch:          md,
		group:                map[string]*TcpGroup[ServiceInfo]{},
		event:                event,
		groupServicesVersion: map[string]int64{},
		groupConnVersion:     map[string]int64{},
		groupLoginVersion:    map[string]int64{},
	}

	// 先让外层注册消息
	if event != nil {
		event.OnMsgReg(md)
	}
	return tb, nil
}

// 创建TcpBackend，使用Consul做服务器发现
// Msg表示消息类型，必须实现util.Msger接口，否则消息无法分发
func NewTcpBackendWithConsul[ServiceInfo any, Msg any](consulAddr, tag string, event TcpEvent[ServiceInfo]) (*TcpBackend[ServiceInfo], error) {
	watcher, err := consul.CreateClient(consulAddr, "http")
	if err != nil {
		return nil, err
	}

	tb, err := NewTcpBackend[ServiceInfo, Msg](event)
	if err != nil {
		return nil, err
	}
	tb.watcher = watcher

	// 服务发现部分
	watcher.WatchServices(tag, func(infos []*consul.RegistryInfo) {
		if tb.event != nil {
			confs := tb.event.ConsulFilter(infos)
			tb.UpdateServices(confs)
		}
	})
	return tb, nil
}

// 创建TcpBackend，使用Nacos做服务器发现
// Msg表示消息类型，必须实现util.Msger接口，否则消息无法分发
func NewTcpBackendWithNacos[ServiceInfo any, Msg any](nacosCli *nacos.Client, serviceNames []string, groupName string, clusters []string, event TcpEvent[ServiceInfo]) (*TcpBackend[ServiceInfo], error) {
	tb, err := NewTcpBackend[ServiceInfo, Msg](event)
	if err != nil {
		return nil, err
	}
	tb.watcher = nacosCli

	// 服务发现部分
	nacosCli.ListenServices(serviceNames, groupName, clusters, func(infos []*nacos.RegistryInfo) {
		if tb.event != nil {
			confs := tb.event.NacosFilter(infos)
			tb.UpdateServices(confs)
		}
	})
	return tb, nil
}

// 创建TcpBackend，使用Redis做服务器发现
// Msg表示消息类型，必须实现util.Msger接口，否则消息无法分发
func NewTcpBackendWithGoRedis[ServiceInfo any, Msg any](cfg *goredis.Config, key string, serverNames []string, event TcpEvent[ServiceInfo]) (*TcpBackend[ServiceInfo], error) {
	watcher, err := goredis.NewRedis(cfg)
	if err != nil {
		return nil, err
	}
	tb, err := NewTcpBackend[ServiceInfo, Msg](event)
	if err != nil {
		return nil, err
	}
	tb.watcher = watcher

	// 服务发现部分
	watcher.WatchServices(key, serverNames, func(infos []*goredis.RegistryInfo) {
		if tb.event != nil {
			confs := tb.event.GoRedisFilter(infos)
			tb.UpdateServices(confs)
		}
	})
	return tb, nil
}

func NewHttpBackend[ServiceInfo any](event HttpEvent[ServiceInfo], watcher interface{}) *HttpBackend[ServiceInfo] {
	return &HttpBackend[ServiceInfo]{
		group:                map[string]*HttpGroup[ServiceInfo]{},
		event:                event,
		watcher:              watcher,
		groupServicesVersion: map[string]int64{},
		groupConnVersion:     map[string]int64{},
	}
}

// 创建HttpBackend，使用Redis做服务器发现
func NewHttpBackendWithConsul[ServiceInfo any](consulAddr, tag string, event HttpEvent[ServiceInfo]) (*HttpBackend[ServiceInfo], error) {
	watcher, err := consul.CreateClient(consulAddr, "http")
	if err != nil {
		return nil, err
	}
	hb := NewHttpBackend[ServiceInfo](event, watcher)
	// 服务发现部分
	watcher.WatchServices(tag, func(infos []*consul.RegistryInfo) {
		if hb.event != nil {
			confs := hb.event.ConsulFilter(infos)
			hb.updateServices(confs)
		}
	})
	return hb, nil
}

func NewHttpBackendWithGoRedis[ServiceInfo any](cfg *goredis.Config, key string, serverNames []string, event HttpEvent[ServiceInfo]) (*HttpBackend[ServiceInfo], error) {
	watcher, err := goredis.NewRedis(cfg)
	if err != nil {
		return nil, err
	}
	hb := NewHttpBackend[ServiceInfo](event, watcher)
	// 服务发现部分
	watcher.WatchServices(key, serverNames, func(infos []*goredis.RegistryInfo) {
		if hb.event != nil {
			confs := hb.event.GoRedisFilter(infos)
			hb.updateServices(confs)
		}
	})
	return hb, nil
}

// 创建HttpBackend，使用Nacos做服务器发现
func NewHttpBackendWithNacos[ServiceInfo any](nacosCli *nacos.Client, serviceName, groupName string, clusters []string, event HttpEvent[ServiceInfo]) (*HttpBackend[ServiceInfo], error) {
	hb := NewHttpBackend[ServiceInfo](event, nacosCli)
	// 服务发现部分
	nacosCli.ListenService(serviceName, groupName, clusters, func(infos []*nacos.RegistryInfo) {
		if hb.event != nil {
			confs := hb.event.NacosFilter(infos)
			hb.updateServices(confs)
		}
	})
	return hb, nil
}
func NewHttpBackendWithNacos2[ServiceInfo any](nacosCli *nacos.Client, serviceNames []string, groupName string, clusters []string, event HttpEvent[ServiceInfo]) (*HttpBackend[ServiceInfo], error) {
	hb := NewHttpBackend[ServiceInfo](event, nacosCli)
	// 服务发现部分
	nacosCli.ListenServices(serviceNames, groupName, clusters, func(infos []*nacos.RegistryInfo) {
		if hb.event != nil {
			confs := hb.event.NacosFilter(infos)
			hb.updateServices(confs)
		}
	})
	return hb, nil
}
