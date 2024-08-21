package backend

// https://github.com/yuwf/gobase

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/yuwf/gobase/tcp"

	"github.com/rs/zerolog/log"
)

// T是和业务相关的客户端信息结构
type HttpService[ServiceInfo any] struct {
	// 不可修改
	g       *HttpGroup[ServiceInfo] // 上层对象
	conf    *ServiceConfig          // 服务器发现的配置
	address string                  // 地址
	info    *ServiceInfo            // 客户端信息 内容修改需要外层加锁控制

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

func (hs *HttpService[ServiceInfo]) loopTick() {
	cheackAddr := fmt.Sprintf("%s:%d", hs.conf.ServiceAddr, hs.conf.ServicePort)
	add := false
	for {
		// 每秒tick下
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
			break
		}
		// 健康检查 只是检查端口能不能通
		err := tcp.TcpPortCheck(cheackAddr, time.Second*3)
		if err == nil {
			// 能连通
			if !add {
				log.Info().Str("ServiceName", hs.conf.ServiceName).
					Str("ServiceId", hs.conf.ServiceId).
					Str("RoutingTag", hs.conf.RoutingTag).
					Err(err).
					Str("Addr", cheackAddr).
					Msg("HttpService connect success")
				add = true
				hs.g.addHashring(hs.conf.ServiceId, hs.conf.RoutingTag)

				// 回调
				for _, h := range hs.g.hb.hook {
					h.OnConnected(hs)
				}
			}
		} else {
			log.Error().Str("ServiceName", hs.conf.ServiceName).
				Str("ServiceId", hs.conf.ServiceId).
				Str("RoutingTag", hs.conf.RoutingTag).
				Err(err).
				Str("Addr", cheackAddr).
				Msg("HttpService connect fail")
			if add {
				add = false
				hs.g.removeHashring(hs.conf.ServiceId, hs.conf.RoutingTag)

				// 回调
				for _, h := range hs.g.hb.hook {
					h.OnDisConnect(hs)
				}
			}
		}
	}
	hs.g.removeHashring(hs.conf.ServiceId, hs.conf.RoutingTag) // 保证移除掉
	hs.quit <- 1                                               // 反写让Close退出
}

func (hs *HttpService[ServiceInfo]) close() {
	// 先从哈希环中移除
	hs.g.removeHashring(hs.conf.ServiceId, hs.conf.RoutingTag)
	// 异步关闭
	go func() {
		if atomic.CompareAndSwapInt32(&hs.quitFlag, 0, 1) {
			hs.quit <- 1
			<-hs.quit
		}
	}()
}
