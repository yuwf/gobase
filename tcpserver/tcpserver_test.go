package tcpserver

import (
	"context"
	"os"
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
	dispatch.MsgDispatch[*utils.TestMsg, TCPClient[ClientInfo]]
}

func NewHandler() *Handler {
	h := &Handler{}
	h.RegMsgID = utils.TestRegMsgID
	h.RegMsg(h.onHeatBeatReq)
	return h
}

func (h *Handler) OnConnected(ctx context.Context, tc *TCPClient[ClientInfo]) {
}

func (h *Handler) OnDisConnect(ctx context.Context, tc *TCPClient[ClientInfo]) {
}

func (b *Handler) DecodeMsg(ctx context.Context, data []byte, tc *TCPClient[ClientInfo]) (utils.RecvMsger, int, interface{}, error) {
	texttype := ctx.Value(CtxKey_Text)
	if texttype != nil {
		return &utils.TestMsg{
			TestMsgHead: utils.TestMsgHead{
				Msgid: 0,
				Len:   uint32(len(data)),
			},
			RecvData: data,
		}, len(data), nil, nil
	}
	m, l, err := utils.TestDecodeMsg(data)
	return m, l, nil, err
}

func (h *Handler) OnMsg(ctx context.Context, msg utils.RecvMsger, tc *TCPClient[ClientInfo]) {
	m, _ := msg.(*utils.TestMsg)
	texttype := ctx.Value(CtxKey_Text)
	if texttype != nil {
		tc.SendText(ctx, m.RecvData) // 原路返回
		return
	}
	if handle, _ := h.Dispatch(ctx, m, tc); handle {
		return
	}
	log.Error().Str("Name", tc.ConnName()).Interface("Msg", msg).Msg("msg not handle")
}

func (h *Handler) OnTick(ctx context.Context, tc *TCPClient[ClientInfo]) {
}

func (h *Handler) onHeatBeatReq(ctx context.Context, msg *utils.TestHeatBeatReq, tc *TCPClient[ClientInfo]) {
	//time.Sleep(time.Second * 2)
	tc.SendMsg(ctx, utils.TestHeatBeatRespMsg)
	tc.info.SetLastHeart(time.Now())
}

func BenchmarkTCPServer(b *testing.B) {
	server, _ := NewTCPServer[int, ClientInfo](1236, NewHandler())
	server.Start(false)
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})

	utils.ExitWait()
}

func BenchmarkTCPServerConsul(b *testing.B) {
	server, _ := NewTCPServer[int, ClientInfo](1236, NewHandler())
	server.Start(false)
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
			"serviceId":   "110",
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

func BenchmarkTCPServerNacos(b *testing.B) {
	server, _ := NewTCPServer[int, ClientInfo](1236, NewHandler())
	server.Start(false)
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})

	// 服务器注册下
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
	conf := &nacos.RegistryConfig{
		ServiceName: "serviceN",
		GroupName:   "groupN",
		Ip:          "localhost",
		Port:        1236,
		Metadata: map[string]string{
			"serviceName": "my-service-name",
			"serviceId":   "111",
			"scheme":      "tcp",
		},
	}
	reg, err := nacosCli.CreateRegister(conf)
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

func BenchmarkTCPServerRegGoRedis(b *testing.B) {
	server, _ := NewTCPServer[int, ClientInfo](1237, NewHandler())
	server.Start(false)
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})

	// 服务器注册下
	var cfg = &goredis.Config{
		Addrs: []string{"127.0.0.1:6379"},
	}
	r, err := goredis.NewRedis(cfg)
	if err != nil {
		return
	}
	var info = &goredis.RegistryInfo{
		RegistryName:   "name",
		RegistryID:     "id",
		RegistryAddr:   "127.0.0.1",
		RegistryPort:   1237,
		RegistryScheme: "tcp",
	}
	reg := r.CreateRegister("test-service", info)
	reg.Reg()
	utils.RegExit(func(s os.Signal) {
		reg.DeReg()
	})

	utils.ExitWait()
}

func BenchmarkTCPServerWS(b *testing.B) {
	server, _ := NewTCPServerWithWS[int, ClientInfo](1237, NewHandler(), "ca.crt", "ca.key")
	server.Start(false)
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})
	utils.ExitWait()
}
