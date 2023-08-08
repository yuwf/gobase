package tcpserver

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/yuwf/gobase/consul"
	"github.com/yuwf/gobase/goredis"
	_ "github.com/yuwf/gobase/log"
	"github.com/yuwf/gobase/redis"
	"github.com/yuwf/gobase/utils"
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
	utils.TestMsgRegister[TCPClient[ClientInfo]]
}

func NewHandler() *Handler {
	h := &Handler{}
	h.RegMsgHandler(utils.TestHeatBeatReqMsg.Msgid, h.onHeatBeatReq)
	return h
}

func (h *Handler) OnConnected(ctx context.Context, tc *TCPClient[ClientInfo]) {
}

func (h *Handler) OnDisConnect(ctx context.Context, tc *TCPClient[ClientInfo]) {
}

func (h *Handler) Encode(data []byte, tc *TCPClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error) {
	msgLog.Int("Size", len(data))
	return data, nil
}

func (h *Handler) EncodeMsg(msg interface{}, tc *TCPClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error) {
	m, _ := msg.(*utils.TestMsg)
	msgLog.Uint32("msgid", m.Msgid).Uint32("size", m.Len).Str("data", string(m.Data))
	return utils.TestEncodeMsg(msg)
}

func (h *Handler) EncodeText(data []byte, gc *TCPClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error) {
	msgLog.Int("Size", len(data))
	return data, nil
}

func (b *Handler) DecodeMsg(ctx context.Context, data []byte, tc *TCPClient[ClientInfo]) (interface{}, int, error) {
	texttype := ctx.Value(CtxKey_Text)
	if texttype != nil {
		return data, len(data), nil
	}
	return utils.TestDecodeMsg(data)
}

func (h *Handler) CheckRPCResp(msg interface{}) interface{} {
	return nil
}

func (h *Handler) OnMsg(ctx context.Context, msg interface{}, tc *TCPClient[ClientInfo]) {
	texttype := ctx.Value(CtxKey_Text)
	if texttype != nil {
		tc.SendText(msg.([]byte)) // 原路返回
		return
	}
	if h.TestMsgRegister.OnMsg(ctx, msg, tc) {
		return
	}
	log.Error().Str("Name", tc.ConnName()).Interface("Msg", msg).Msg("msg not handle")
}

func (h *Handler) OnTick(ctx context.Context, tc *TCPClient[ClientInfo]) {
}

func (h *Handler) onHeatBeatReq(ctx context.Context, msg *utils.TestMsg, tc *TCPClient[ClientInfo]) {
	//time.Sleep(time.Second * 2)
	tc.SendMsg(utils.TestHeatBeatRespMsg)
	tc.info.SetLastHeart(time.Now())
}

func BenchmarkTCPServer(b *testing.B) {
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

func BenchmarkTCPServerRegRedis(b *testing.B) {
	server, _ := NewTCPServer[int, ClientInfo](1237, NewHandler())
	server.Start(false)
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})

	// 服务器注册下
	var cfg = &redis.Config{
		Addr: "127.0.0.1:6379",
	}
	r, err := redis.NewRedis(cfg)
	if err != nil {
		return
	}
	var info = &redis.RegistryInfo{
		RegistryName:   "name",
		RegistryID:     "id",
		RegistryAddr:   "127.0.0.1",
		RegistryPort:   1237,
		RegistryScheme: "tcp",
	}
	reg := r.CreateRegister("test-service", info)
	for i := 0; i < 1000; i++ {
		reg.Reg()
		time.Sleep(time.Second * 1)
		reg.DeReg()
		time.Sleep(time.Second * 1)
	}
	reg.Reg()
	utils.RegExit(func(s os.Signal) {
		reg.DeReg()
	})

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
