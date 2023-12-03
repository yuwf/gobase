package gnetserver

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"gobase/consul"
	"gobase/dispatch"
	_ "gobase/log"
	"gobase/utils"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// 客户端信息
type ClientInfo struct {
	sync.RWMutex
	// 最近一次心跳的时间
	lastHeart time.Time
}

func (c *ClientInfo) SetLastHeart(t time.Time) {
	c.Lock()
	defer c.Unlock()
	c.lastHeart = t
}

func (c *ClientInfo) LastHeart() time.Time {
	c.RLock()
	defer c.RUnlock()
	return c.lastHeart
}

type Handler struct {
	dispatch.MsgDispatch[utils.TestMsg, GNetClient[ClientInfo]]
}

func NewHandler() *Handler {
	h := &Handler{}
	h.RegMsgID = utils.TestRegMsgID
	h.RegMsg(h.onHeatBeatReq)
	return h
}

func (h *Handler) OnConnected(ctx context.Context, gc *GNetClient[ClientInfo]) {
}

func (h *Handler) OnDisConnect(ctx context.Context, gc *GNetClient[ClientInfo]) {
}

func (h *Handler) Encode(data []byte, gc *GNetClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error) {
	msgLog.Int("Size", len(data))
	return data, nil
}

func (h *Handler) EncodeMsg(msg interface{}, gc *GNetClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error) {
	m, _ := msg.(*utils.TestMsg)
	msgLog.Uint32("msgid", m.Msgid).Uint32("size", m.Len).Interface("data", m.BodyMsg)
	return utils.TestEncodeMsg(msg)
}

func (h *Handler) EncodeText(data []byte, gc *GNetClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error) {
	msgLog.Int("Size", len(data))
	return data, nil
}

func (b *Handler) DecodeMsg(ctx context.Context, data []byte, gc *GNetClient[ClientInfo]) (interface{}, int, error) {
	texttype := ctx.Value(CtxKey_Text)
	if texttype != nil {
		return data, len(data), nil
	}
	return utils.TestDecodeMsg(data)
}

func (h *Handler) OnMsg(ctx context.Context, msg interface{}, gc *GNetClient[ClientInfo]) {
	texttype := ctx.Value(CtxKey_Text)
	if texttype != nil {
		gc.SendText(msg.([]byte)) // 原路返回
		return
	}
	m, _ := msg.(*utils.TestMsg)
	if h.Dispatch(ctx, m, gc) {
		return
	}
	log.Error().Str("Name", gc.ConnName()).Interface("Msg", msg).Msg("msg not handle")
}

func (h *Handler) OnTick(ctx context.Context, gc *GNetClient[ClientInfo]) {

}

func (h *Handler) onHeatBeatReq(ctx context.Context, msg *utils.TestHeatBeatReq, gc *GNetClient[ClientInfo]) {
	gc.SendMsg(utils.TestHeatBeatRespMsg)
	gc.info.SetLastHeart(time.Now())
}

func BenchmarkGNetServer(b *testing.B) {
	server := NewGNetServer[int, ClientInfo](1236, NewHandler(), "")
	server.Start()
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})

	utils.ExitWait()
}

func BenchmarkGNetServerConsul(b *testing.B) {
	server := NewGNetServer[int, ClientInfo](1236, NewHandler(), "")
	server.Start()
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})

	// 服务器注册下
	_, err := consul.InitDefaultClient("127.0.0.1:8500", "http")
	if err != nil {
		return
	}
	hostname, _ := os.Hostname()
	conf := &consul.RegistryConfig{
		RegistryName: "tcpserver",
		RegistryID:   "serverId-serverName-nodeId",
		RegistryAddr: "localhost",
		RegistryPort: 1236,
		RegistryTag:  []string{"gobase-test"},
		RegistryMeta: map[string]string{
			"hostname":    hostname,
			"metricsPath": "/metrics",
			"metricsPort": "9100",
			"serviceName": "my-service-name",
			"nodeId":      "110",
			"scheme":      "tcp",
		},
		HealthPort:     9502, //consul内部自己开启监听
		HealthPath:     "/health",
		HealthInterval: 4,
		HealthTimeout:  4,
		DeregisterTime: 4,
	}
	reg, err := consul.DefaultClient().CreateRegister(conf)
	if err == nil {
		err = reg.Reg()
		if err != nil {
			utils.RegExit(func(s os.Signal) {
				reg.DeReg()
			})
		}
	}

	utils.ExitWait()
}

func BenchmarkGNetServerWS(b *testing.B) {
	server := NewGNetServer[int, ClientInfo](1236, NewHandler(), "ws")
	server.Start()
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})

	utils.ExitWait()
}
