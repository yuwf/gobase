package backend

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"gobase/consul"
	"gobase/goredis"
	_ "gobase/log"
	"gobase/redis"
	"gobase/utils"

	"github.com/rs/zerolog"
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
	utils.TestMsgRegister[TcpService[TcpServiceInfo]]
}

func NewTcpHandler() *TcpHandler {
	h := &TcpHandler{}
	h.RegMsgHandler(utils.TestHeatBeatRespMsg.Msgid, h.onHeatBeatResp)
	return h
}

func (h *TcpHandler) ConsulFilter(confs []*consul.RegistryInfo) []*ServiceConfig {
	// 过滤出tcp的配置
	// 【目前根据业务 ServiceId是存储在meta中nodeId】
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
		nodeId, ok := conf.RegistryMeta["nodeId"]
		nodeId = strings.ToLower(nodeId)
		nodeId = strings.TrimSpace(nodeId)
		if !ok || len(nodeId) == 0 {
			log.Error().Str("RegistryName", conf.RegistryName).Str("ID", conf.RegistryID).Msg("TcpService Filter nodeId error is nil")
			continue
		}
		// 根据协议过滤
		scheme := strings.ToLower(conf.RegistryMeta["scheme"])
		if scheme == "tcp" {
			c := &ServiceConfig{
				ServiceName: serviceName,
				ServiceId:   nodeId,
				ServiceAddr: conf.RegistryAddr,
				ServicePort: conf.RegistryPort,
			}
			tcp = append(tcp, c)
		}
	}
	return tcp
}

func (h *TcpHandler) RedisFilter(confs []*redis.RegistryInfo) []*ServiceConfig {
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
			}
			tcp = append(tcp, c)
		}
	}
	return tcp
}

func (h *TcpHandler) OnConnected(ctx context.Context, ts *TcpService[TcpServiceInfo]) {
	// 连接成功 发送心跳
}

func (h *TcpHandler) OnDisConnect(ctx context.Context, ts *TcpService[TcpServiceInfo]) {

}

func (h *TcpHandler) Encode(data []byte, ts *TcpService[TcpServiceInfo], msgLog *zerolog.Event) ([]byte, error) {
	msgLog.Int("Size", len(data))
	return data, nil
}

func (h *TcpHandler) EncodeMsg(msg interface{}, ts *TcpService[TcpServiceInfo], msgLog *zerolog.Event) ([]byte, error) {
	m, _ := msg.(*utils.TestMsg)
	msgLog.Uint32("msgid", m.Msgid).Uint32("size", m.Len).Str("data", string(m.Data))
	return utils.TestEncodeMsg(msg)
}

func (h *TcpHandler) DecodeMsg(ctx context.Context, data []byte, ts *TcpService[TcpServiceInfo]) (interface{}, int, error) {
	return utils.TestDecodeMsg(data)
}

func (h *TcpHandler) CheckRPCResp(msg interface{}) interface{} {
	m, _ := msg.(*utils.TestMsg)
	if m.Msgid == utils.TestHeatBeatRespMsg.Msgid {
		return utils.TestHeatBeatReqMsg.Msgid
	}
	return nil
}

func (h *TcpHandler) OnMsg(ctx context.Context, msg interface{}, ts *TcpService[TcpServiceInfo]) {
	if h.TestMsgRegister.OnMsg(ctx, msg, ts) {
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
			//rpc方式发送，需要修改CheckRPCResp支持
			resp, err := ts.SendRPCMsg(utils.TestHeatBeatReqMsg.Msgid, utils.TestHeatBeatReqMsg, time.Second*5)
			if err == nil {
				m, _ := resp.(*utils.TestMsg)
				log.Debug().Str("Name", ts.ConnName()).Interface("Msg", m).Msg("RecvRPCMsg")
			}
		}
	}
}

func (h *TcpHandler) onHeatBeatResp(ctx context.Context, msg *utils.TestMsg, ts *TcpService[TcpServiceInfo]) {

}

func BenchmarkTCPBackend(b *testing.B) {
	_, err := NewTcpBackendWithConsul[TcpServiceInfo]("127.0.0.1:8500", "gobase-test", NewTcpHandler())
	if err != nil {
		return
	}

	utils.ExitWait()
}

func BenchmarkTCPBackendRedis(b *testing.B) {
	//BackendParamConf.Get().Immediately = true
	var cfg = &redis.Config{
		Addr: "127.0.0.1:6379",
	}
	_, err := NewTcpBackendWithRedis[TcpServiceInfo](cfg, "test-service", nil, NewTcpHandler())
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
