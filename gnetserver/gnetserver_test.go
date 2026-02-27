package gnetserver

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/yuwf/gobase/consul"
	"github.com/yuwf/gobase/goredis"
	_ "github.com/yuwf/gobase/log"
	"github.com/yuwf/gobase/msger"
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
}

func NewHandler() *Handler {
	h := &Handler{}
	return h
}

func (h *Handler) OnMsgReg(md *msger.MsgDispatch) {
	md.RegMsg(utils.TestHeatBeatReqMsg.MsgID(), h.onHeatBeatReq)
}

func (h *Handler) OnConnected(ctx context.Context, gc *GNetClient[ClientInfo]) {
}

func (h *Handler) OnDisConnect(ctx context.Context, gc *GNetClient[ClientInfo]) {
}

func (b *Handler) DecodeMsg(ctx context.Context, data []byte, gc *GNetClient[ClientInfo]) (msger.RecvMsger, int, error) {
	texttype := ctx.Value(CtxKey_Text)
	if texttype != nil {
		return &utils.TestMsg{
			TestMsgHead: utils.TestMsgHead{
				Msgid: 0,
				Len:   uint32(len(data)),
			},
			RecvData: data,
		}, len(data), nil
	}
	return utils.TestDecodeMsg(data)
}
func (h *Handler) OnMsg(ctx context.Context, mr msger.RecvMsger, gc *GNetClient[ClientInfo]) {
	utils.LogCtx(log.Warn(), ctx).Interface("msger", mr).Msgf("Msg Not Handle %s", gc.ConnName())
}

func (h *Handler) OnTick(ctx context.Context, gc *GNetClient[ClientInfo]) {

}

func (h *Handler) onHeatBeatReq(ctx context.Context, msg *utils.TestHeatBeatReq, gc *GNetClient[ClientInfo]) {
	gc.SendMsg(ctx, utils.TestHeatBeatRespMsg)
	gc.info.SetLastHeart(time.Now())
}

func BenchmarkGNetServer(b *testing.B) {
	h := NewHandler()
	server, _ := NewGNetServer[int, ClientInfo, utils.TestMsg](1236, h)
	server.Start()
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})

	utils.ExitWait()
}

func BenchmarkGNetServerConsul(b *testing.B) {
	h := NewHandler()
	server, _ := NewGNetServer[int, ClientInfo, utils.TestMsg](1236, h)
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

func BenchmarkGNetServerNacos(b *testing.B) {
	h := NewHandler()
	server, _ := NewGNetServer[int, ClientInfo, utils.TestMsg](1237, h)
	server.Start()
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
		Port:        1237,
		Metadata: map[string]string{
			"serviceName": "my-service-name",
			"serviceId":   "110",
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

func BenchmarkGNetServerRegGoRedis(b *testing.B) {
	h := NewHandler()
	server, _ := NewGNetServer[int, ClientInfo, utils.TestMsg](1237, h)
	server.Start()
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

func BenchmarkGNetServerWS(b *testing.B) {
	h := NewHandler()
	server, _ := NewGNetServerWS[int, ClientInfo, utils.TestMsg](1236, h)
	server.Start()
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})

	utils.ExitWait()
}
