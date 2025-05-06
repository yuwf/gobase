package backend

// https://github.com/yuwf/gobase

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/yuwf/gobase/tcp"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

// T是和业务相关的客户端信息结构
type HttpService[ServiceInfo any] struct {
	// 不可修改
	g       *HttpGroup[ServiceInfo] // 上层对象
	conf    *ServiceConfig          // 服务器发现的配置
	address string                  // 地址
	info    *ServiceInfo            // 客户端信息 内容修改需要外层加锁控制
	state   int32                   // 状态检查 0：未连接 1：检查OK 原子操作

	// 外部要求退出
	quit     chan int // 退出chan 外部写 内部读
	quitFlag int32    // 标记是否退出，原子操作
}

func NewHttpService[ServiceInfo any](conf *ServiceConfig, g *HttpGroup[ServiceInfo]) (*HttpService[ServiceInfo], error) {
	hs := &HttpService[ServiceInfo]{
		g:        g,
		conf:     conf,
		address:  fmt.Sprintf("http://%s:%d", conf.ServiceAddr, conf.ServicePort),
		info:     new(ServiceInfo),
		quit:     make(chan int),
		quitFlag: 0,
	}
	// 开启tick协程
	go hs.loopTick()
	return hs, nil
}

// 获取配置 获取后外层要求只读
func (hs *HttpService[ServiceInfo]) Conf() *ServiceConfig {
	return hs.conf
}

func (hs *HttpService[ServiceInfo]) ServiceName() string {
	return hs.conf.ServiceName
}

func (hs *HttpService[ServiceInfo]) ServiceId() string {
	return hs.conf.ServiceId
}

func (hs *HttpService[ServiceInfo]) Address() string {
	return hs.address
}

func (hs *HttpService[ServiceInfo]) Info() *ServiceInfo {
	return hs.info
}

func (hs *HttpService[ServiceInfo]) InfoI() interface{} {
	return hs.info
}

// 0:健康 1:没连接成功
func (hs *HttpService[ServiceInfo]) HealthState() int {
	if atomic.LoadInt32(&hs.state) != 1 {
		return 1
	}
	return 0
}

func (hs *HttpService[ServiceInfo]) loopTick() {
	cheackAddr := fmt.Sprintf("%s:%d", hs.conf.ServiceAddr, hs.conf.ServicePort)
	add := false
	for {
		// 健康检查 只是检查端口能不能通
		err := tcp.TcpPortCheck(cheackAddr, time.Second*3)
		if err == nil {
			atomic.StoreInt32(&hs.state, 1)
			// 能连通
			if !add {
				add = true
				hs.onDialSuccess()
			}
		} else {
			atomic.StoreInt32(&hs.state, 0)
			if add {
				add = false
				hs.onDisConnect(err)
			}
		}
		// 每秒tick下 tick放下面，先上面检查下端口
		timer := time.NewTimer(time.Second)
		select {
		case <-hs.quit:
			if !timer.Stop() {
				select {
				case <-timer.C: // try to drain the channel
				default:
				}
			}
		case <-timer.C:
		}
		if atomic.LoadInt32(&hs.quitFlag) != 0 {
			if add {
				hs.onDisConnect(errors.New("quit"))
			}
			break
		}
	}
	hs.quit <- 1 // 反写让Close退出
}

func (hs *HttpService[ServiceInfo]) close() {
	// 退出循环
	if atomic.CompareAndSwapInt32(&hs.quitFlag, 0, 1) {
		hs.quit <- 1
		<-hs.quit
	}
}

func (hs *HttpService[ServiceInfo]) onDialSuccess() {
	log.Info().Str("ServiceName", hs.conf.ServiceName).
		Str("ServiceId", hs.conf.ServiceId).
		Strs("RoutingTag", hs.conf.RoutingTag).
		Str("Addr", fmt.Sprintf("%s:%d", hs.conf.ServiceAddr, hs.conf.ServicePort)).
		Msg("HttpService connect success")

	// 添加到哈希环中
	if hs.HealthState() == 0 {
		hs.g.addHashring(hs.conf.ServiceId, hs.conf.RoutingTag)
	}

	// 回调
	func() {
		defer utils.HandlePanic()
		for _, h := range hs.g.hb.hook {
			h.OnConnected(hs)
		}
	}()
}

func (hs *HttpService[ServiceInfo]) onDisConnect(err error) {
	log.Error().Str("ServiceName", hs.conf.ServiceName).
		Str("ServiceId", hs.conf.ServiceId).
		Strs("RoutingTag", hs.conf.RoutingTag).
		Err(err).
		Str("Addr", fmt.Sprintf("%s:%d", hs.conf.ServiceAddr, hs.conf.ServicePort)).
		Msg("HttpService connect fail")

	hs.g.removeHashring(hs.conf.ServiceId)

	// 回调
	func() {
		defer utils.HandlePanic()
		for _, h := range hs.g.hb.hook {
			h.OnDisConnect(hs)
		}
	}()
}
