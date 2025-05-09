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

const CtxKey_scheme = utils.CtxKey("scheme")

// 后端连接对象 协程安全对象
// T是和业务相关的客户端信息结构
type TcpService[ServiceInfo any] struct {
	// 不可修改
	g           *TcpGroup[ServiceInfo] // 上层对象
	conf        *ServiceConfig         // 服务器发现的配置
	confDestroy int32                  // 表示配置是否已经销毁了 原子操作
	address     string                 // 地址
	seq         utils.Sequence         // 消息顺序处理工具 协程安全
	info        *ServiceInfo           // 客户端信息，内容修改需要外层加锁控制
	conn        *tcp.TCPConn           // 连接对象，协程安全
	connLogin   int32                  // 表示连接是否登录成功了 原子操作

	ctx context.Context // 本连接的上下文

	//RPC消息使用 [rpcid:chan respmsg]
	rpc *sync.Map

	// 外部要求退出
	quit     chan int // 退出chan 外部写 内部读
	quitFlag int32    // 标记是否退出，原子操作

	// 标记服务器发现逻辑配置已经不存在了

}

func NewTcpService[ServiceInfo any](conf *ServiceConfig, g *TcpGroup[ServiceInfo]) (*TcpService[ServiceInfo], error) {
	ts := &TcpService[ServiceInfo]{
		g:           g,
		conf:        conf,
		confDestroy: 0,
		address:     fmt.Sprintf("%s:%d", conf.ServiceAddr, conf.ServicePort),
		info:        new(ServiceInfo),
		connLogin:   0,
		ctx:         context.WithValue(context.TODO(), CtxKey_scheme, "tcp"),
		rpc:         new(sync.Map),
		quit:        make(chan int),
		quitFlag:    0,
	}
	conn, err := tcp.NewTCPConn(ts.address, ts)
	if err != nil {
		log.Error().Str("ServiceName", conf.ServiceName).
			Str("ServiceId", conf.ServiceId).Err(err).
			Strs("RoutingTag", conf.RoutingTag).
			Str("addr", ts.address).
			Msg("NewTcpService")
		return nil, err
	}
	ts.conn = conn

	// 调用对象的ServiceCreate函数
	creater, ok := any(ts.info).(ServiceCreater)
	if ok {
		creater.ServiceCreate(ts.conf)
	}

	// 开启tick协程
	go ts.loopTick()
	return ts, nil
}

// 重连或者关闭连接时调用
func (ts *TcpService[ServiceInfo]) clear() {
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
func (ts *TcpService[ServiceInfo]) Conf() *ServiceConfig {
	return ts.conf
}

func (ts *TcpService[ServiceInfo]) ServiceName() string {
	return ts.conf.ServiceName
}

func (ts *TcpService[ServiceInfo]) ServiceId() string {
	return ts.conf.ServiceId
}

func (ts *TcpService[ServiceInfo]) Info() *ServiceInfo {
	return ts.info
}

func (ts *TcpService[ServiceInfo]) InfoI() interface{} {
	return ts.info
}

// 获取连接对象
func (ts *TcpService[ServiceInfo]) Conn() *tcp.TCPConn {
	return ts.conn
}

func (ts *TcpService[ServiceInfo]) ConnName() string {
	return ts.conf.ServiceName + ":" + ts.conf.ServiceId
}

func (ts *TcpService[ServiceInfo]) RecvSeqCount() int {
	return ts.seq.Len()
}

// 登录成功后调用,否则直接调用tcpbackend发不了消息
func (ts *TcpService[ServiceInfo]) OnLogin() {
	atomic.StoreInt32(&ts.connLogin, 1)

	// 添加到哈希环中
	if ts.HealthState() == 0 {
		ts.g.addHashring(ts.conf.ServiceId, ts.conf.RoutingTag)
	}
}

// 0:健康 1:没连接成功 2:发现配置已经不存在了，还保持连接 3：未登录
func (ts *TcpService[ServiceInfo]) HealthState() int {
	if !ts.conn.Connected() {
		return 1
	}
	if atomic.LoadInt32(&ts.confDestroy) != 0 {
		return 2
	}
	if atomic.LoadInt32(&ts.connLogin) != 1 {
		return 3
	}
	return 0
}

func (ts *TcpService[ServiceInfo]) Send(ctx context.Context, data []byte) error {
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

// SendMsg 发送消息对象，会调用消息对象的MsgMarshal来编码消息
// 消息对象可实现zerolog.LogObjectMarshaler接口，更好的输出日志，通过ParamConf.LogLevelMsg配置可控制日志级别
func (ts *TcpService[ServiceInfo]) SendMsg(ctx context.Context, msg utils.SendMsger) error {
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
	logLevel := TcpParamConf.Get().MsgLogLevel.SendLevel(msg)
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("Name", ts.ConnName()).Interface("Msg", msg).Msg("SendMsg")
	}
	// 回调
	func() {
		defer utils.HandlePanic()
		for _, h := range ts.g.tb.hook {
			h.OnSendMsg(ts, msg.MsgID(), len(data))
		}
	}()
	return nil
}

// SendRPCMsg 发送RPC消息并等待消息回复，需要依赖event.DecodeMsg返回的rpcId来判断是否rpc调用
// 成功返回解析后的消息
// 消息对象可实现zerolog.LogObjectMarshaler接口，更好的输出日志，通过ParamConf.LogLevelMsg配置可控制日志级别
func (ts *TcpService[ServiceInfo]) SendRPCMsg(ctx context.Context, rpcId interface{}, msg utils.SendMsger, timeout time.Duration) (interface{}, error) {
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
	// 回调
	func() {
		defer utils.HandlePanic()
		for _, h := range ts.g.tb.hook {
			h.OnSendMsg(ts, msg.MsgID(), len(data))
		}
	}()
	// 日志
	logLevel := TcpParamConf.Get().MsgLogLevel.SendLevel(msg)
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
	return resp, err
}

func (ts *TcpService[ServiceInfo]) loopTick() {
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
			ctx := context.WithValue(ts.ctx, utils.CtxKey_traceId, utils.GenTraceID())
			ctx = context.WithValue(ctx, utils.CtxKey_msgId, "_tick_")
			ts.seq.Submit(func() {
				ts.g.tb.event.OnTick(ctx, ts)
			})
		}
	}
	ts.quit <- 1 // 反写让Close退出
}

func (ts *TcpService[ServiceInfo]) close() {
	// 关闭网络
	ts.conn.Close(true)
	// 退出循环
	if atomic.CompareAndSwapInt32(&ts.quitFlag, 0, 1) {
		ts.quit <- 1
		<-ts.quit
	}
}

func (ts *TcpService[ServiceInfo]) OnDialFail(err error, t *tcp.TCPConn) error {
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

func (ts *TcpService[ServiceInfo]) OnDialSuccess(t *tcp.TCPConn) {
	log.Info().Str("Name", ts.ConnName()).Str("RemoteAddr", t.RemoteAddr().String()).Str("LocalAddr", t.LocalAddr().String()).Msg("Connect success")

	if ts.g.tb.event != nil {
		ts.seq.Submit(func() {
			ctx := context.WithValue(ts.ctx, utils.CtxKey_traceId, utils.GenTraceID())
			ts.g.tb.event.OnConnected(ctx, ts)
		})
	}

	// 回调
	func() {
		defer utils.HandlePanic()
		for _, h := range ts.g.tb.hook {
			h.OnConnected(ts)
		}
	}()
}

func (ts *TcpService[ServiceInfo]) OnDisConnect(err error, t *tcp.TCPConn) error {
	log.Error().Str("Name", ts.ConnName()).
		Str("ServiceId", ts.conf.ServiceId).
		Err(err).
		Str("Addr", ts.address).
		Strs("RoutingTag", ts.conf.RoutingTag).
		Int32("ConfDestroy", atomic.LoadInt32(&ts.confDestroy)).
		Msg("Disconnect")

	atomic.StoreInt32(&ts.connLogin, 0)
	// 从哈希环中移除
	ts.g.removeHashring(ts.conf.ServiceId)

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
		ts.seq.Submit(func() {
			ctx := context.WithValue(ts.ctx, utils.CtxKey_traceId, utils.GenTraceID())
			ts.g.tb.event.OnDisConnect(ctx, ts)
		})
	}

	// 回调
	func() {
		defer utils.HandlePanic()
		for _, h := range ts.g.tb.hook {
			h.OnDisConnect(ts)
		}
	}()
	return nil
}

func (ts *TcpService[ServiceInfo]) OnClose(tc *tcp.TCPConn) {
	// 调用close主动关闭的
	log.Info().Str("Name", ts.ConnName()).Str("addr", ts.address).Msg("Onlose")

	ts.clear()

	// 回调
	func() {
		defer utils.HandlePanic()
		for _, h := range ts.g.tb.hook {
			h.OnDisConnect(ts)
		}
	}()
}

func (ts *TcpService[ServiceInfo]) OnRecv(data []byte, tc *tcp.TCPConn) (int, error) {
	// 回调
	func() {
		defer utils.HandlePanic()
		for _, h := range ts.g.tb.hook {
			h.OnRecv(ts, len(data))
		}
	}()
	len, err := ts.recv(data)
	//if err != nil {
	//	tc.Close(err) // 不需要关，tcpconn会根据err关闭掉，并调用OnDisConnect
	//}
	return len, err
}

func (ts *TcpService[ServiceInfo]) OnSend(data []byte, tc *tcp.TCPConn) ([]byte, error) {
	// 回调
	func() {
		defer utils.HandlePanic()
		for _, h := range ts.g.tb.hook {
			h.OnSend(ts, len(data))
		}
	}()
	return data, nil
}

func (ts *TcpService[ServiceInfo]) recv(data []byte) (int, error) {
	if ts.g.tb.event == nil {
		return len(data), nil
	}
	decodeLen := 0
	for {
		msg, l, rpcId, err := ts.g.tb.event.DecodeMsg(ts.ctx, data[decodeLen:], ts)
		if err != nil {
			return 0, err
		}
		if l < 0 || l > len(data)-decodeLen {
			err = fmt.Errorf("decode return len is %d, readbuf len is %d", l, len(data)-decodeLen)
			return 0, err
		}
		if l > 0 {
			decodeLen += l
		}
		if l == 0 || msg == nil {
			break
		}
		if msg != nil {
			ctx := context.WithValue(ts.ctx, utils.CtxKey_traceId, utils.GenTraceID())
			ctx = context.WithValue(ctx, utils.CtxKey_msgId, msg.MsgID())
			// 日志
			logLevel := TcpParamConf.Get().MsgLogLevel.RecvLevel(msg)
			if logLevel >= int(log.Logger.GetLevel()) {
				if rpcId != nil {
					utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("Name", ts.ConnName()).Interface("Resp", msg).Msg("RecvRPCMsg")
				} else {
					utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("Name", ts.ConnName()).Interface("Msg", msg).Msg("RecvMsg")
				}
			}
			if rpcId != nil {
				// rpc
				rpc, ok := ts.rpc.LoadAndDelete(rpcId)
				if ok {
					ch := rpc.(chan interface{})
					ch <- msg
					close(ch) // 删除的地方负责关闭
				} else {
					// 没找到可能是超时了也可能是DecodeMsg没正确返回 也交给OnMsg执行
					if TcpParamConf.Get().MsgSeq {
						ts.seq.Submit(func() {
							ts.g.tb.event.OnMsg(ctx, msg, ts)
						})
					} else {
						utils.Submit(func() {
							ts.g.tb.event.OnMsg(ctx, msg, ts)
						})
					}
				}
			} else {
				// 消息放入协程池中
				if TcpParamConf.Get().MsgSeq {
					ts.seq.Submit(func() {
						ts.g.tb.event.OnMsg(ctx, msg, ts)
					})
				} else {
					utils.Submit(func() {
						ts.g.tb.event.OnMsg(ctx, msg, ts)
					})
				}
			}
			// 回调
			func() {
				defer utils.HandlePanic()
				for _, h := range ts.g.tb.hook {
					h.OnRecvMsg(ts, msg.MsgID(), l)
				}
			}()
		}
		if decodeLen >= len(data) {
			break // 不需要继续读取了
		}
	}
	return decodeLen, nil
}
