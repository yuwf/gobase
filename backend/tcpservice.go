package backend

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuwf/gobase/tcp"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type MsgIDer interface {
	MsgID() string
}

// 后端连接对象 协程安全对象
// T是和业务相关的客户端信息结构
type TcpService[T any] struct {
	// 不可修改
	g           *TcpGroup[T]   // 上层对象
	conf        *ServiceConfig // 服务器发现的配置
	confDestroy int32          // 表示将配置是否销毁了 原子操作
	address     string         // 地址
	seq         utils.Sequence // 消息顺序处理工具 协程安全
	info        *T             // 客户端信息，内容修改需要外层加锁控制
	conn        *tcp.TCPConn   // 连接对象，协程安全

	ctx context.Context // 本连接的上下文

	//RPC消息使用 [rpcid:chan respmsg]
	rpc *sync.Map

	// 外部要求退出
	quit     chan int // 退出chan 外部写 内部读
	quitFlag int32    // 标记是否退出，原子操作

	// 标记服务器发现逻辑配置已经不存在了

}

func NewTcpService[T any](conf *ServiceConfig, g *TcpGroup[T]) (*TcpService[T], error) {
	ts := &TcpService[T]{
		g:           g,
		conf:        conf,
		confDestroy: 0,
		address:     fmt.Sprintf("%s:%d", conf.ServiceAddr, conf.ServicePort),
		info:        new(T),
		ctx:         context.WithValue(context.TODO(), "scheme", "tcp"),
		rpc:         new(sync.Map),
		quit:        make(chan int),
		quitFlag:    0,
	}
	conn, err := tcp.NewTCPConn(ts.address, ts)
	if err != nil {
		log.Error().Str("ServiceName", conf.ServiceName).
			Str("ServiceId", conf.ServiceId).Err(err).
			Str("RoutingTag", conf.RoutingTag).
			Str("addr", ts.address).
			Msg("NewTcpService")
		return nil, err
	}
	ts.conn = conn
	// 开启tick协程
	go ts.loopTick()
	return ts, nil
}

// 重连或者关闭连接时调用
func (ts *TcpService[T]) clear() {
	// 清空下rpc
	ts.rpc.Range(func(key, value interface{}) bool {
		rpc, ok := ts.rpc.LoadAndDelete(key)
		if ok {
			ch := rpc.(chan interface{})
			ch <- nil // 写一个空的
			close(ch) // 删除的地方负责关闭
		}
		return true
	})
}

// 获取配置 获取后外层要求只读
func (ts *TcpService[T]) Conf() *ServiceConfig {
	return ts.conf
}

func (ts *TcpService[T]) ServiceName() string {
	return ts.conf.ServiceName
}

func (ts *TcpService[T]) ServiceId() string {
	return ts.conf.ServiceId
}

func (ts *TcpService[T]) Info() *T {
	return ts.info
}

// 获取连接对象
func (ts *TcpService[T]) Conn() *tcp.TCPConn {
	return ts.conn
}

func (ts *TcpService[T]) ConnName() string {
	return ts.conf.ServiceName + ":" + ts.conf.ServiceId
}

func (ts *TcpService[T]) Send(ctx context.Context, data []byte) error {
	var err error
	if len(data) == 0 {
		err = errors.New("data is empty")
		utils.LogCtx(log.Error(), ctx).Str("Name", ts.ConnName()).Msg("Send error")
		return err
	}
	err = ts.conn.Send(data)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", ts.ConnName()).Err(err).Int("Size", len(data)).Msg("Send error")
		return err
	}
	utils.LogCtx(log.Debug(), ctx).Str("Name", ts.ConnName()).Int("Size", len(data)).Msg("Send")
	return nil
}

func (ts *TcpService[T]) SendMsg(ctx context.Context, msg utils.SendMsger) error {
	if msg == nil {
		err := errors.New("msg is empty")
		utils.LogCtx(log.Error(), ctx).Str("Name", ts.ConnName()).Msg("SendMsg error")
		return err
	}
	data, err := msg.MsgMarshal()
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", ts.ConnName()).Err(err).Interface("Msg", msg).Msg("SendMsg error")
		return err
	}
	err = ts.conn.Send(data)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", ts.ConnName()).Err(err).Interface("Msg", msg).Msg("SendMsg error")
		return err
	}
	// 日志
	logLevel := TcpParamConf.Get().MsgLogLevel(msg.MsgID())
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("Name", ts.ConnName()).Interface("Msg", msg).Msg("SendMsg")
	}
	return nil
}

func (ts *TcpService[T]) SendRPCMsg(ctx context.Context, rpcId interface{}, msg utils.SendMsger, timeout time.Duration) (interface{}, error) {
	if msg == nil {
		err := errors.New("msg is empty")
		utils.LogCtx(log.Error(), ctx).Str("Name", ts.ConnName()).Msg("SendRPCMsg error")
		return nil, err
	}
	data, err := msg.MsgMarshal()
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", ts.ConnName()).Err(err).Interface("Msg", msg).Msg("SendRPCMsg error")
		return nil, err
	}
	_, ok := ts.rpc.Load(rpcId)
	if ok {
		err := errors.New("rpcId exist")
		utils.LogCtx(log.Error(), ctx).Str("Name", ts.ConnName()).Err(err).Interface("rpcID", rpcId).Interface("Msg", msg).Msg("SendRPCMsg error")
		return nil, err
	}
	// 先添加一个记录，防止Send还没出来就收到了回复
	ch := make(chan interface{})
	ts.rpc.Store(rpcId, ch)
	defer func() {
		_, ok = ts.rpc.LoadAndDelete(rpcId)
		if ok {
			close(ch) // 删除的地方负责关闭
		}
	}()
	// 发送
	err = ts.conn.Send(data)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", ts.ConnName()).Err(err).Interface("Msg", msg).Msg("SendRPCMsg error")
		return nil, err
	}
	// 日志
	logLevel := TcpParamConf.Get().MsgLogLevel(msg.MsgID())
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("Name", ts.ConnName()).Interface("Msg", msg).Msg("SendRPCMsg")
	}

	// 等待rpc回复
	timer := time.NewTimer(timeout)
	var resp interface{}
	select {
	case resp = <-ch:
		if !timer.Stop() {
			select {
			case <-timer.C: // try to drain the channel
			default:
			}
		}
	case <-timer.C:
		err = errors.New("timeout")
	}
	if resp == nil && err == nil { //clear函数的调用会触发此情况
		err = errors.New("close")
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", ts.ConnName()).Err(err).Msg("SendRPCMsg resp error")
	}
	// 日志
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("Name", ts.ConnName()).Interface("Resp", resp).Msg("RecvRPCMsg")
	}
	return resp, err
}

func (ts *TcpService[T]) loopTick() {
	for {
		// 每秒tick下
		timer := time.NewTimer(time.Second)
		select {
		case <-ts.quit:
			if !timer.Stop() {
				select {
				case <-timer.C: // try to drain the channel
				default:
				}
			}
		case <-timer.C:
		}
		if atomic.LoadInt32(&ts.quitFlag) != 0 {
			break
		}
		if ts.g.tb.event != nil {
			if TcpParamConf.Get().MsgSeq {
				ts.seq.Submit(func() {
					ts.g.tb.event.OnTick(ts.g.tb.event.Context(ts.ctx), ts)
				})
			} else {
				utils.Submit(func() {
					ts.g.tb.event.OnTick(ts.g.tb.event.Context(ts.ctx), ts)
				})
			}
		}
	}
	ts.quit <- 1 // 反写让Close退出
}

func (ts *TcpService[T]) close() {
	// 先从哈希环中移除
	ts.g.removeHashring(ts.conf.ServiceId, ts.conf.RoutingTag)
	// 关闭网络
	ts.conn.Close(true)
	// 退出循环
	if atomic.CompareAndSwapInt32(&ts.quitFlag, 0, 1) {
		ts.quit <- 1
		<-ts.quit
	}
}

func (ts *TcpService[T]) OnDialFail(err error, t *tcp.TCPConn) error {
	log.Error().Str("Name", ts.ConnName()).Err(err).Str("DialAddr", ts.address).Int32("ConfDestroy", atomic.LoadInt32(&ts.confDestroy)).Msg("Connect fail")
	if atomic.LoadInt32(&ts.confDestroy) == 1 {
		// 服务器发现配置已经不存在了，停止loop，直接从group中删除
		ts.g.removeSevice(ts.conf.ServiceId) // 先删
		if atomic.CompareAndSwapInt32(&ts.quitFlag, 0, 1) {
			ts.quit <- 1
			<-ts.quit
		}
		return errors.New("config destroy")
	}
	return nil
}

func (ts *TcpService[T]) OnDialSuccess(t *tcp.TCPConn) {
	log.Info().Str("Name", ts.ConnName()).Str("RemoteAddr", t.RemoteAddr().String()).Str("LocalAddr", t.LocalAddr().String()).Msg("Connect success")

	// 添加到哈希环中
	ts.g.addHashring(ts.conf.ServiceId, ts.conf.RoutingTag)

	if ts.g.tb.event != nil {
		ctx := context.TODO()
		ts.g.tb.event.OnConnected(ctx, ts)
	}

	// 回调
	for _, h := range ts.g.tb.hook {
		h.OnConnected(ts)
	}
}

func (ts *TcpService[T]) OnDisConnect(err error, t *tcp.TCPConn) error {
	log.Error().Str("Name", ts.ConnName()).
		Str("ServiceId", ts.conf.ServiceId).
		Err(err).
		Str("Addr", ts.address).
		Str("RoutingTag", ts.conf.RoutingTag).
		Int32("ConfDestroy", atomic.LoadInt32(&ts.confDestroy)).
		Msg("Disconnect")

	// 从哈希环中移除
	ts.g.removeHashring(ts.conf.ServiceId, ts.conf.RoutingTag)

	ts.clear()

	if atomic.LoadInt32(&ts.confDestroy) == 1 {
		// 服务器发现配置已经不存在了，停止loop，直接从group中删除
		ts.g.removeSevice(ts.conf.ServiceId) // 先删
		if atomic.CompareAndSwapInt32(&ts.quitFlag, 0, 1) {
			ts.quit <- 1
			<-ts.quit
		}
		return errors.New("config destroy")
	}
	if ts.g.tb.event != nil {
		ts.g.tb.event.OnDisConnect(ts.ctx, ts)
	}

	// 回调
	for _, h := range ts.g.tb.hook {
		h.OnDisConnect(ts)
	}
	return nil
}

func (ts *TcpService[T]) OnClose(tc *tcp.TCPConn) {
	// 调用close主动关闭的
	log.Info().Str("Name", ts.ConnName()).Str("addr", ts.address).Msg("Onlose")

	ts.clear()

	// 回调
	for _, h := range ts.g.tb.hook {
		h.OnDisConnect(ts)
	}
}

func (ts *TcpService[T]) OnRecv(data []byte, tc *tcp.TCPConn) (int, error) {
	// 回调
	for _, h := range ts.g.tb.hook {
		h.OnRecv(ts, len(data))
	}
	len, err := ts.recv(data)
	//if err != nil {
	//	tc.Close(err) // 不需要关，tcpconn会根据err关闭掉，并调用OnDisConnect
	//}
	return len, err
}

func (ts *TcpService[T]) OnSend(data []byte, tc *tcp.TCPConn) ([]byte, error) {
	// 回调
	for _, h := range ts.g.tb.hook {
		h.OnSend(ts, len(data))
	}
	return data, nil
}

func (ts *TcpService[T]) recv(data []byte) (int, error) {
	if ts.g.tb.event == nil {
		return len(data), nil
	}
	decodeLen := 0
	for {
		msg, l, err := ts.g.tb.event.DecodeMsg(ts.ctx, data[decodeLen:], ts)
		if err != nil {
			return 0, err
		}
		if l < 0 || l > len(data)-decodeLen {
			err = fmt.Errorf("decode return len is %d, readbuf len is %d", l, len(data)-decodeLen)
			return 0, err
		}
		decodeLen += l
		if msg != nil {
			// rpc消息检查
			rpcId := ts.g.tb.event.CheckRPCResp(msg)
			if rpcId != nil {
				// rpc
				rpc, ok := ts.rpc.LoadAndDelete(rpcId)
				if ok {
					ch := rpc.(chan interface{})
					ch <- msg
					close(ch) // 删除的地方负责关闭
				} else {
					// 没找到可能是超时了也可能是CheckRPCResp出错了 也交给OnMsg执行
					if TcpParamConf.Get().MsgSeq {
						ts.seq.Submit(func() {
							ts.g.tb.event.OnMsg(ts.g.tb.event.Context(ts.ctx), msg, ts)
						})
					} else {
						utils.Submit(func() {
							ts.g.tb.event.OnMsg(ts.g.tb.event.Context(ts.ctx), msg, ts)
						})
					}
				}
			} else {
				// 消息放入协程池中
				if TcpParamConf.Get().MsgSeq {
					ts.seq.Submit(func() {
						ts.g.tb.event.OnMsg(ts.g.tb.event.Context(ts.ctx), msg, ts)
					})
				} else {
					utils.Submit(func() {
						ts.g.tb.event.OnMsg(ts.g.tb.event.Context(ts.ctx), msg, ts)
					})
				}
			}
		}
		if decodeLen >= len(data) {
			break // 不需要继续读取了
		}
	}
	return decodeLen, nil
}
