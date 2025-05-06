package backend

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yuwf/gobase/consul"
	"github.com/yuwf/gobase/dispatch"
	"github.com/yuwf/gobase/goredis"
	_ "github.com/yuwf/gobase/log"
	"github.com/yuwf/gobase/nacos"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

// 服务器端信息
type TcpServiceInfo struct {
	sync.RWMutex

	// 最近一次心跳的时间
	lastHeart time.Time
}

func (c *TcpServiceInfo) SetLastHeart(t time.Time) {
	c.Lock()
	defer c.Unlock()
	c.lastHeart = t
}

func (c *TcpServiceInfo) LastHeart() time.Time {
	c.RLock()
	defer c.RUnlock()
	return c.lastHeart
}

type TcpHandler struct {
	dispatch.MsgDispatch[*utils.TestMsg, TcpService[TcpServiceInfo]]
}

func NewTcpHandler() *TcpHandler {
	h := &TcpHandler{}
	h.RegMsgID = utils.TestRegMsgID
	h.RegMsg(h.onHeatBeatResp)
	return h
}

func (h *TcpHandler) ConsulFilter(confs []*consul.RegistryInfo) []*ServiceConfig {
	// 过滤出tcp的配置
	// 【目前根据业务 ServiceId是存储在meta中serviceId】
	// 【目前根据业务 目前ServiceName是存储在meta中的serviceName】
	tcp := []*ServiceConfig{}
	for _, conf := range confs {
		serviceName, ok := conf.RegistryMeta["serviceName"]
		serviceName = strings.ToLower(serviceName)
		serviceName = strings.TrimSpace(serviceName)
		if !ok || len(serviceName) == 0 {
			log.Error().Str("RegistryName", conf.RegistryName).Str("RegistryID", conf.RegistryID).Msg("TcpService Filter serviceName is nil")
			continue
		}
		serviceId, ok := conf.RegistryMeta["serviceId"]
		serviceId = strings.TrimSpace(strings.ToLower(serviceId))
		if !ok || len(serviceId) == 0 {
			log.Error().Str("RegistryName", conf.RegistryName).Str("ID", conf.RegistryID).Msg("TcpService Filter serviceId error is nil")
			continue
		}
		// 根据协议过滤
		scheme := strings.ToLower(conf.RegistryMeta["scheme"])
		if scheme == "tcp" {
			c := &ServiceConfig{
				ServiceName: serviceName,
				ServiceId:   serviceId,
				ServiceAddr: conf.RegistryAddr,
				ServicePort: conf.RegistryPort,
				Metadata:    conf.RegistryMeta,
			}
			tcp = append(tcp, c)
		}
	}
	return tcp
}

func (h *TcpHandler) NacosFilter(confs []*nacos.RegistryInfo) []*ServiceConfig {
	// 过滤出tcp的配置
	// 【目前根据业务 ServiceId是存储在meta中serviceId】
	// 【目前根据业务 目前ServiceName是存储在meta中的serviceName】
	tcp := []*ServiceConfig{}
	for _, conf := range confs {
		serviceName, ok := conf.Metadata["serviceName"]
		serviceName = strings.ToLower(serviceName)
		serviceName = strings.TrimSpace(serviceName)
		if !ok || len(serviceName) == 0 {
			log.Error().Str("InstanceId", conf.InstanceId).Msg("TcpService Filter serviceName is nil")
			continue
		}
		serviceId, ok := conf.Metadata["serviceId"]
		serviceId = strings.TrimSpace(strings.ToLower(serviceId))
		if !ok || len(serviceId) == 0 {
			log.Error().Str("InstanceId", conf.InstanceId).Msg("TcpService Filter serviceId error is nil")
			continue
		}
		// 根据协议过滤
		scheme := strings.ToLower(conf.Metadata["scheme"])
		if scheme == "tcp" {
			c := &ServiceConfig{
				ServiceName: serviceName,
				ServiceId:   serviceId,
				ServiceAddr: conf.Ip,
				ServicePort: conf.Port,
				Metadata:    conf.Metadata,
			}
			tcp = append(tcp, c)
		}
	}
	return tcp
}

func (h *TcpHandler) GoRedisFilter(confs []*goredis.RegistryInfo) []*ServiceConfig {
	// 过滤出tcp的配置
	// 【目前根据业务 ServiceId是存储在meta中nodeId】
	// 【目前根据业务 目前ServiceName是存储在meta中的serviceName】
	tcp := []*ServiceConfig{}
	for _, conf := range confs {
		if conf.RegistryScheme == "tcp" {
			c := &ServiceConfig{
				ServiceName: conf.RegistryName,
				ServiceId:   conf.RegistryID,
				ServiceAddr: conf.RegistryAddr,
				ServicePort: conf.RegistryPort,
				// Metadata:    conf.Metadata, redis没有Metadata
			}
			tcp = append(tcp, c)
		}
	}
	return tcp
}

func (h *TcpHandler) OnConnected(ctx context.Context, ts *TcpService[TcpServiceInfo]) {
	// 连接成功 发送心跳
	ts.OnLogin() // 直接标记登录成功
}

func (h *TcpHandler) OnDisConnect(ctx context.Context, ts *TcpService[TcpServiceInfo]) {

}

func (h *TcpHandler) DecodeMsg(ctx context.Context, data []byte, ts *TcpService[TcpServiceInfo]) (utils.RecvMsger, int, interface{}, error) {
	m, l, err := utils.TestDecodeMsg(data)
	if err != nil {
		return nil, len(data), nil, err
	}
	if m.Msgid == utils.TestHeatBeatRespMsg.Msgid {
		return m, l, utils.TestHeatBeatReqMsg.Msgid, err
	}
	return m, l, nil, err
}

func (h *TcpHandler) OnMsg(ctx context.Context, msg utils.RecvMsger, ts *TcpService[TcpServiceInfo]) {
	m, _ := msg.(*utils.TestMsg)
	if handle, _ := h.Dispatch(ctx, m, ts); handle {
		return
	}
	log.Error().Str("Name", ts.ConnName()).Interface("Msg", msg).Msg("msg not handle")
}

func (h *TcpHandler) OnTick(ctx context.Context, ts *TcpService[TcpServiceInfo]) {
	if ts.Conn().Connected() {
		now := time.Now()
		if now.Sub(ts.Info().LastHeart()) > time.Second {
			ts.Info().SetLastHeart(time.Now())
			// 连接成功 发送心跳
			//正常发送
			//ts.SendMsg(utils.TestHeatBeatReqMsg)
			//rpc方式发送，需要修改DecodeMsg来支持
			_, err := ts.SendRPCMsg(ctx, utils.TestHeatBeatReqMsg.Msgid, utils.TestHeatBeatReqMsg, time.Second*5)
			if err == nil {
				//m, _ := resp.(*utils.TestMsg)
				//utils.LogCtx(log.Debug(), ctx).Str("Name", ts.ConnName()).Interface("Msg", m).Msg("RecvRPCMsg")
			}
		}
	}
}

func (h *TcpHandler) onHeatBeatResp(ctx context.Context, msg *utils.TestHeatBeatResp, ts *TcpService[TcpServiceInfo]) {

}

func BenchmarkTCPBackendConsul(b *testing.B) {
	_, err := NewTcpBackendWithConsul[TcpServiceInfo]("127.0.0.1:8500", "gobase-test", NewTcpHandler())
	if err != nil {
		return
	}

	utils.ExitWait()
}

func BenchmarkTCPBackendNacos(b *testing.B) {
	cfg := nacos.Config{
		NamespaceId: "",
		Username:    "nacos",
		Password:    "nacos",
		Addrs:       []string{"localhost:8848"},
	}
	nacosCli, err := nacos.CreateClient(&cfg)
	if err != nil {
		return
	}

	_, err = NewTcpBackendWithNacos[TcpServiceInfo](nacosCli, "serviceN", "groupN", []string{}, NewTcpHandler())
	if err != nil {
		return
	}

	utils.ExitWait()
}

func BenchmarkTCPBackendGoRedis(b *testing.B) {
	//BackendParamConf.Get().Immediately = true
	var cfg = &goredis.Config{
		Addrs: []string{"127.0.0.1:6379"},
	}
	_, err := NewTcpBackendWithGoRedis[TcpServiceInfo](cfg, "test-service", nil, NewTcpHandler())
	if err != nil {
		return
	}

	utils.ExitWait()
}
