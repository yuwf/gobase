package backend

// https://github.com/yuwf

import (
	"fmt"
	"sync/atomic"
	"time"

	"gobase/tcp"

	"github.com/rs/zerolog/log"
)

// T是和业务相关的客户端信息结构
type HttpService[T any] struct {
	// 不可修改
	g       *HttpGroup[T]  // 上层对象
	conf    *ServiceConfig // 服务器发现的配置
	address string         // 地址
	info    *T             // 客户端信息 内容修改需要外层加锁控制

	// 外部要求退出
	quit     chan int // 退出chan 外部写 内部读
	quitFlag int32    // 标记是否退出，原子操作
}

func NewHttpService[T any](conf *ServiceConfig, g *HttpGroup[T]) (*HttpService[T], error) {
	hs := &HttpService[T]{
		g:        g,
		conf:     conf,
		address:  fmt.Sprintf("http://%s:%d", conf.ServiceAddr, conf.ServicePort),
		info:     new(T),
		quit:     make(chan int),
		quitFlag: 0,
	}
	// 开启tick协程
	go hs.loopTick()
	return hs, nil
}

// 获取配置 获取后外层要求只读
func (hs *HttpService[T]) Conf() *ServiceConfig {
	return hs.conf
}

func (hs *HttpService[T]) ServiceName() string {
	return hs.conf.ServiceName
}

func (hs *HttpService[T]) ServiceId() string {
	return hs.conf.ServiceId
}

func (hs *HttpService[T]) Address() string {
	return hs.address
}

func (hs *HttpService[T]) Info() *T {
	return hs.info
}

func (hs *HttpService[T]) loopTick() {
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
				log.Info().Str("ServiceName", hs.conf.ServiceName).Str("ServiceId", hs.conf.ServiceId).Err(err).Str("Addr", cheackAddr).Msg("HttpService connect success")
				add = true
				hs.g.hashring.Add(hs.conf.ServiceId)

				// 回调
				for _, h := range hs.g.hb.hook {
					h.OnConnected(hs)
				}
			}
		} else {
			log.Error().Str("ServiceName", hs.conf.ServiceName).Str("ServiceId", hs.conf.ServiceId).Err(err).Str("Addr", cheackAddr).Msg("HttpService connect fail")
			if add {
				add = false
				hs.g.hashring.Remove(hs.conf.ServiceId)

				// 回调
				for _, h := range hs.g.hb.hook {
					h.OnDisConnect(hs)
				}
			}
		}
	}
	hs.g.hashring.Remove(hs.conf.ServiceId) // 保证移除掉
	hs.quit <- 1                            // 反写让Close退出
}

func (hs *HttpService[T]) close() {
	// 先从哈希环中移除
	hs.g.hashring.Remove(hs.conf.ServiceId)
	// 异步关闭
	go func() {
		if atomic.CompareAndSwapInt32(&hs.quitFlag, 0, 1) {
			hs.quit <- 1
			<-hs.quit
		}
	}()
}
