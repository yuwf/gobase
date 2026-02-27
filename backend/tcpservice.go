package backend

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuwf/gobase/msger"
	"github.com/yuwf/gobase/tcp"
	"github.com/yuwf/gobase/utils"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const CtxKey_scheme = utils.CtxKey("scheme")

const (
	TcpStatus_All     int = -1 // 不存在该状态 用于一些接口的获取参数
	TcpStatus_Conning int = 0  // 发现配置 链接中 此状态目前没有哈希环
	TcpStatus_Conned  int = 1  // 连接成功
	TcpStatus_Logined int = 2  // 登录成功 外层控制登录，成功后显示调用OnConnLogined
)

// 后端连接对象 协程安全对象
// ServiceInfo是和业务相关的连接端自定义信息结构
type TcpService[ServiceInfo any] struct {
	// 不可修改，协程安全
	g        *TcpGroup[ServiceInfo] // 上层对象
	conf     *ServiceConfig         // 服务器发现的配置
	address  string                 // 地址
	seq      utils.Sequence         // 消息顺序处理工具 协程安全
	groupSeq utils.GroupSequence    // 分组执行的消息, 消息设置为非顺序处理的才会分组
	info     *ServiceInfo           // 客户端信息，内容修改需要外层加锁控制
	conn     *tcp.TCPConn           // 连接对象，协程安全

	confDestroy int32 // 表示配置是否已经销毁了 原子操作，如果conn正在连接中，直接销毁该对象
	connLogined int32 // 表示连接是否登录成功了 原子操作

	ctx context.Context // 本连接的上下文

	//RPC消息使用 [rpcid:chan respmsg]
	rpc *sync.Map

	// 外部要求退出
	quit      chan struct{} // 退出chan 外部写 内部读
	quitState int32         // 标记是否退出，原子操作
	closed    chan struct{} // 关闭chan 内部写 外部读
}

func NewTcpService[ServiceInfo any](conf *ServiceConfig, g *TcpGroup[ServiceInfo]) (*TcpService[ServiceInfo], error) {
	ts := &TcpService[ServiceInfo]{
		g:           g,
		conf:        conf,
		confDestroy: 0,
		address:     fmt.Sprintf("%s:%d", conf.ServiceAddr, conf.ServicePort),
		info:        new(ServiceInfo),
		connLogined: 0,
		ctx:         context.WithValue(context.TODO(), CtxKey_scheme, "tcp"),
		rpc:         new(sync.Map),
		quit:        make(chan struct{}),
		quitState:   0,
		closed:      make(chan struct{}),
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
			ch := rpc.(chan msger.RecvMsger)
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

// 消息堆积数量，顺序处理和分组处理的才能计算
func (ts *TcpService[ServiceInfo]) RecvSeqCount() int {
	return ts.seq.Len() + ts.groupSeq.Len()
}

// 外部调用，登录成功后调用
func (ts *TcpService[ServiceInfo]) OnConnLogined(ctx context.Context) {
	if !ts.conn.Connected() {
		// 未连接成功
		utils.LogCtx(log.Error(), ctx).Msgf("OnConnLogined %s error", ts.ConnName())
		return
	}

	atomic.StoreInt32(&ts.connLogined, 1)
	// 修改登录版本
	ts.g.tb.addLoginVersion(ts.g.serviceName)
}

// 状态，发现配置中是否还存在
func (ts *TcpService[ServiceInfo]) HealthStatus() (int, bool) {
	if !ts.conn.Connected() {
		// 未连接成功
		return TcpStatus_Conning, false
	}
	if atomic.LoadInt32(&ts.connLogined) == 0 {
		return TcpStatus_Conned, atomic.LoadInt32(&ts.confDestroy) == 0
	} else {
		return TcpStatus_Logined, atomic.LoadInt32(&ts.confDestroy) == 0
	}
}

func (ts *TcpService[ServiceInfo]) Send(ctx context.Context, data []byte) error {
	var err error
	if len(data) == 0 {
		err = errors.New("data is empty")
		utils.LogCtx(log.Error(), ctx).Msgf("Send %s error", ts.ConnName())
		return err
	}
	// 回调
	defer func() {
		defer utils.HandlePanic()
		for _, h := range ts.g.tb.hook {
			h.OnSend(ts, len(data))
		}
	}()
	// 发送
	err = ts.conn.Send(data)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Int("size", len(data)).Msgf("Send %s error", ts.ConnName())
		return err
	}
	utils.LogCtx(log.Debug(), ctx).Int("size", len(data)).Msgf("Send %s", ts.ConnName())
	return nil
}

// SendRPCMsg 发送RPC消息并等待消息回复，需要依赖event.DecodeMsg返回的rpcId来判断是否rpc调用
// 消息对象可实现zerolog.LogObjectMarshaler接口，更好的输出日志，通过ParamConf.LogLevelMsg配置可控制日志级别
func (ts *TcpService[ServiceInfo]) SendMsg(ctx context.Context, msg msger.Msger) error {
	if msg == nil {
		err := errors.New("msg is empty")
		utils.LogCtx(log.Error(), ctx).Msgf("SendMsg %s error", ts.ConnName())
		return err
	}
	data, err := msg.MsgMarshal()
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Interface("msger", msg).Msgf("SendMsg %s error", ts.ConnName())
		return err
	}
	// 回调
	defer func() {
		defer utils.HandlePanic()
		for _, h := range ts.g.tb.hook {
			h.OnSendMsg(ts, msg, len(data))
		}
	}()
	// 发送
	err = ts.conn.Send(data)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Interface("msger", msg).Msgf("SendMsg %s error", ts.ConnName())
		return err
	}
	// 日志
	logLevel := msger.ParamConf.Get().LogLevel.MsgLevel(msg)
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Interface("msger", msg).Msgf("SendMsg %s", ts.ConnName())
	}
	return nil
}

// SendRPCMsg 发送RPC消息并等待消息回复，需要依赖event.DecodeMsg返回的rpcId来判断是否rpc调用
// 消息对象可实现zerolog.LogObjectMarshaler接口，更好的输出日志，通过ParamConf.LogLevelMsg配置可控制日志级别
// respBody: 解析后的消息体 ，如果respBody不是nil，会调用RecvMsger的BodyUnMarshal解析消息体
// 返回值
// - resp: 回复的消息对象
func (ts *TcpService[ServiceInfo]) SendRPCMsg(ctx context.Context, rpcId interface{}, req msger.Msger, timeout time.Duration, respBody interface{}) (msger.RecvMsger, error) {
	rpcIdV := fmt.Sprintf("%v", rpcId)
	if req == nil {
		err := errors.New("msg is empty")
		utils.LogCtx(log.Error(), ctx).Str("rpcId", rpcIdV).Msgf("SendRPCMsg %s error", ts.ConnName())
		return nil, err
	}
	data, err := req.MsgMarshal()
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Str("rpcId", rpcIdV).Interface("msger", req).Msgf("SendRPCMsg %s error", ts.ConnName())
		return nil, err
	}

	// 先添加一个channel记录，防止Send还没出来就收到了回复，并且判断是否存在一样的
	ch := make(chan msger.RecvMsger, 1) // 使用缓冲channel
	if _, loaded := ts.rpc.LoadOrStore(rpcIdV, ch); loaded {
		close(ch) // 关闭新创建的channel
		err := errors.New("rpcId exist")
		utils.LogCtx(log.Error(), ctx).Str("rpcId", rpcIdV).Err(err).Interface("msger", req).Msgf("SendRPCMsg %s error", ts.ConnName())
		return nil, err
	}
	defer func() {
		if _, ok := ts.rpc.LoadAndDelete(rpcIdV); ok {
			close(ch) // 删除的地方负责关闭
		}
	}()
	// 回调
	entry := time.Now()
	defer func() {
		defer utils.HandlePanic()
		for _, h := range ts.g.tb.hook {
			h.OnSendRPCMsg(ts, rpcId, req, time.Since(entry), len(data))
		}
	}()
	// 发送
	err = ts.conn.Send(data)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Str("rpcId", rpcIdV).Interface("msger", req).Msgf("SendRPCMsg %s error", ts.ConnName())
		return nil, err
	}
	// 日志
	logLevel := msger.ParamConf.Get().LogLevel.MsgLevel(req)
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("rpcId", rpcIdV).Interface("msger", req).Msgf("SendRPCMsg %s", ts.ConnName())
	}

	// 等待rpc回复
	timer := time.NewTimer(timeout)
	var resp msger.RecvMsger
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
		utils.LogCtx(log.Error(), ctx).Str("rpcId", rpcIdV).Err(err).Msgf("SendRPCMsg %s resp error", ts.ConnName())
		return nil, err
	}

	// 解析消息体
	if respBody != nil {
		err = resp.BodyUnMarshal(respBody)
		if err != nil {
			utils.LogCtx(log.Error(), ctx).Str("rpcId", rpcIdV).Err(err).Msgf("SendRPCMsg %s resp error", ts.ConnName())
			return resp, err
		}
	}

	// 日志
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("rpcId", rpcIdV).Interface("msger", resp).Msgf("SendRPCMsg %s resp", ts.ConnName())
	}
	return resp, nil
}

// SendRPCMsgAsync 发送异步RPC消息，需要依赖event.DecodeMsg返回消息的RPCId()来判断是否rpc调用
// 消息对象可实现zerolog.LogObjectMarshaler接口，更好的输出日志，通过ParamConf.LogLevelMsg配置可控制日志级别
// 返回值 为nil时，才会调用callback
// callback 参考msger.AsyncRPCCallback说明
func (ts *TcpService[ServiceInfo]) SendAsyncRPCMsg(ctx context.Context, rpcId interface{}, req msger.Msger, timeout time.Duration, callback interface{}) error {
	rpcIdV := fmt.Sprintf("%v", rpcId)
	if req == nil {
		err := errors.New("msg is empty")
		utils.LogCtx(log.Error(), ctx).Str("rpcId", rpcIdV).Msgf("SendAsyncRPCMsg %s error", ts.ConnName())
		return err
	}
	data, err := req.MsgMarshal()
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("rpcId", rpcIdV).Err(err).Interface("msger", req).Msgf("SendAsyncRPCMsg %s error", ts.ConnName())
		return err
	}

	cb, err := msger.GetAsyncCallback(callback)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("rpcId", rpcIdV).Err(err).Interface("msger", req).Msgf("SendAsyncRPCMsg %s error", ts.ConnName())
		return err
	}

	// 先添加一个channel记录，防止Send还没出来就收到了回复，并且判断是否存在一样的
	ch := make(chan msger.RecvMsger, 1) // 使用缓冲channel
	if _, loaded := ts.rpc.LoadOrStore(rpcIdV, ch); loaded {
		close(ch) // 关闭新创建的channel
		err := errors.New("rpcId exist")
		utils.LogCtx(log.Error(), ctx).Str("rpcId", rpcIdV).Err(err).Interface("msger", req).Msgf("SendAsyncRPCMsg %s error", ts.ConnName())
		return err
	}
	// 回调
	entry := time.Now()
	defer func() {
		defer utils.HandlePanic()
		for _, h := range ts.g.tb.hook {
			h.OnSendRPCMsg(ts, rpcId, req, time.Since(entry), len(data))
		}
	}()
	// 发送
	err = ts.conn.Send(data)
	if err != nil {
		// 发送失败，先删除channel记录
		if _, ok := ts.rpc.LoadAndDelete(rpcIdV); ok {
			close(ch) // 删除的地方负责关闭
		}
		utils.LogCtx(log.Error(), ctx).Str("rpcId", rpcIdV).Err(err).Interface("msger", req).Msgf("SendAsyncRPCMsg %s error", ts.ConnName())
		return err
	}
	// 日志
	logLevel := msger.ParamConf.Get().LogLevel.MsgLevel(req)
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("rpcId", rpcIdV).Interface("msger", req).Msgf("SendAsyncRPCMsg %s", ts.ConnName())
	}

	// 异步等待回复
	utils.Submit(func() {
		defer func() {
			if _, ok := ts.rpc.LoadAndDelete(rpcIdV); ok {
				close(ch) // 删除的地方负责关闭
			}
		}()
		// 等待rpc回复
		timer := time.NewTimer(timeout)
		var resp msger.RecvMsger
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

		handle := func(resp msger.RecvMsger, body interface{}, err error) {
			if cb == nil {
				return
			}
			// 消息放入协程池中
			if TcpParamConf.Get().MsgSeq {
				ts.seq.Submit(func() {
					cb.Call(resp, body, err)
				})
			} else {
				if resp == nil {
					cb.Call(resp, body, err) // 已经在异步协程中了 直接调用
					return
				}
				groupId := resp.GroupId()
				if groupId != nil {
					ts.groupSeq.Submit(groupId, func() {
						cb.Call(resp, body, err)
					})
				} else {
					cb.Call(resp, body, err) // 已经在异步协程中了 直接调用
				}
			}
		}

		if err != nil {
			utils.LogCtx(log.Error(), ctx).Str("rpcId", rpcIdV).Err(err).Msgf("SendAsyncRPCMsg %s resp error", ts.ConnName())
			handle(nil, nil, err)
			return
		}

		// 解析消息体
		var respBody interface{}
		if cb != nil {
			if respBodyType := cb.RespBodyElemType(); respBodyType != nil {
				respBody = reflect.New(respBodyType).Interface()
				err = resp.BodyUnMarshal(respBody)
				if err != nil {
					utils.LogCtx(log.Error(), ctx).Str("rpcId", rpcIdV).Err(err).Msgf("SendAsyncRPCMsg %s resp error", ts.ConnName())
					handle(resp, nil, err)
					return
				}
			}
		}

		// 日志
		if logLevel >= int(log.Logger.GetLevel()) {
			utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("rpcId", rpcIdV).Interface("msger", resp).Msgf("SendAsyncRPCMsg %s resp", ts.ConnName())
		}
		handle(resp, respBody, nil)
	})

	return nil
}

func (ts *TcpService[ServiceInfo]) loopTick() {
	quit := false
	for {
		interval := TcpParamConf.Get().TickInterval
		if interval <= 0 {
			interval = 1.0
		}
		timer := time.NewTimer(time.Duration(float64(interval) * float64(time.Second)))

		select {
		case <-ts.quit:
			if !timer.Stop() {
				select {
				case <-timer.C: // try to drain the channel
				default:
				}
			}
			quit = true
		case <-timer.C:
		}
		if quit {
			break
		}
		if ts.g.tb.event != nil {
			ctx := utils.CtxSetTrace(ts.ctx, 0, "Tick")
			ts.seq.Submit(func() {
				ts.g.tb.event.OnTick(ctx, ts)
			})
		}
	}
	close(ts.closed)
}

// 此函数会等待网络彻底关闭，会调用OnDisConnect
func (ts *TcpService[ServiceInfo]) close() {
	// 关闭网络
	ts.conn.Close(true)
	// 退出循环
	if atomic.CompareAndSwapInt32(&ts.quitState, 0, 1) {
		close(ts.quit)
		<-ts.closed
	}
}

func (ts *TcpService[ServiceInfo]) OnDialFail(err error, t *tcp.TCPConn) error {
	log.Error().Err(err).Str("DialAddr", ts.address).Int32("ConfDestroy", atomic.LoadInt32(&ts.confDestroy)).Msgf("Connect %s fail", ts.ConnName())
	if atomic.LoadInt32(&ts.confDestroy) == 1 {
		// 服务器发现配置已经不存在了，停止loop，直接从group中删除
		utils.Submit(func() {
			// 使用协程，因为在group的update中调用功能close removeSevice会阻塞
			ts.g.removeSevice(ts.conf.ServiceId) // 先删
		})
		if atomic.CompareAndSwapInt32(&ts.quitState, 0, 1) {
			close(ts.quit)
			<-ts.closed
		}
		return errors.New("config destroy")
	}
	return nil
}

func (ts *TcpService[ServiceInfo]) OnDialSuccess(t *tcp.TCPConn) {
	log.Info().Str("RemoteAddr", t.RemoteAddr().String()).Str("LocalAddr", t.LocalAddr().String()).Msgf("Connect %s success", ts.ConnName())

	// 修改连接版本
	ts.g.tb.addConnVersion(ts.g.serviceName)

	if ts.g.tb.event != nil {
		ts.seq.Submit(func() {
			ctx := utils.CtxSetTrace(ts.ctx, 0, "Connected")
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
	log.Error().
		Str("ServiceId", ts.conf.ServiceId).
		Err(err).
		Str("Addr", ts.address).
		Strs("RoutingTag", ts.conf.RoutingTag).
		Int32("ConfDestroy", atomic.LoadInt32(&ts.confDestroy)).
		Msgf("Disconnect %s", ts.ConnName())

	// 登录状态还原
	atomic.StoreInt32(&ts.connLogined, 0)
	// 修改版本
	ts.g.tb.addConnVersion(ts.g.serviceName)
	ts.g.tb.addLoginVersion(ts.g.serviceName)

	ts.clear()

	if ts.g.tb.event != nil {
		ts.seq.Submit(func() {
			ctx := utils.CtxSetTrace(ts.ctx, 0, "DisConnected")
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

	if atomic.LoadInt32(&ts.confDestroy) == 1 {
		// 服务器发现配置已经不存在了，停止loop，直接从group中删除
		utils.Submit(func() {
			// 使用协程，因为在group的update中调用功能close removeSevice会阻塞
			ts.g.removeSevice(ts.conf.ServiceId) // 先删
		})
		if atomic.CompareAndSwapInt32(&ts.quitState, 0, 1) {
			close(ts.quit)
			<-ts.closed
		}
		return errors.New("config destroy")
	}
	return nil
}

func (ts *TcpService[ServiceInfo]) OnRecv(data []byte, tc *tcp.TCPConn) (int, error) {
	// 回调
	func() {
		defer utils.HandlePanic()
		for _, h := range ts.g.tb.hook {
			h.OnRecvData(ts, len(data))
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
			h.OnSendData(ts, len(data))
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
		mr, l, err := ts.decode(ts.ctx, data[decodeLen:])
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
		if l == 0 || mr == nil {
			break
		}
		if mr != nil {
			// 回调
			func() {
				defer utils.HandlePanic()
				for _, h := range ts.g.tb.hook {
					h.OnRecvMsg(ts, mr, l)
				}
			}()

			traceName := mr.MsgID()
			if mner, _ := any(mr).(msger.MsgerName); mner != nil {
				traceName = mner.MsgName()
			}
			ctx := utils.CtxSetTrace(ts.ctx, mr.TraceId(), traceName)

			rpcId := mr.RPCId()
			if rpcId != nil {
				// rpc
				rpcIdV := fmt.Sprintf("%v", rpcId)
				rpc, ok := ts.rpc.LoadAndDelete(rpcIdV)
				if ok {
					ch := rpc.(chan msger.RecvMsger)
					ch <- mr
					close(ch) // 删除的地方负责关闭
				} else {
					// 没找到可能是超时了也可能是DecodeMsg没正确返回 也交给OnMsg执行
					ts.handle(ctx, mr)
				}
			} else {
				ts.handle(ctx, mr)
			}
		}
		if decodeLen >= len(data) {
			break // 不需要继续读取了
		}
	}
	return decodeLen, nil
}

func (ts *TcpService[ServiceInfo]) decode(ctx context.Context, buf []byte) (msger.RecvMsger, int, error) {
	var mr msger.RecvMsger
	var l int
	var err error
	defer utils.HandlePanic2(func(r any) {
		err = fmt.Errorf("decode panic: %v", r)
	})

	mr, l, err = ts.g.tb.event.DecodeMsg(ctx, buf, ts)
	return mr, l, err
}

func (ts *TcpService[ServiceInfo]) handle(ctx context.Context, mr msger.RecvMsger) {
	// 熔断
	if name, ok := msger.ParamConf.Get().IsHystrixMsg(mr.MsgID()); ok {
		hystrix.DoC(ctx, name, func(ctx context.Context) error {
			// 消息放入协程池中
			if TcpParamConf.Get().MsgSeq {
				ts.seq.Submit(func() {
					ts.onMsg(ctx, mr)
				})
			} else {
				groupId := mr.GroupId()
				if groupId != nil {
					ts.groupSeq.Submit(groupId, func() {
						ts.onMsg(ctx, mr)
					})
				} else {
					utils.Submit(func() {
						ts.onMsg(ctx, mr)
					})
				}
			}
			return nil
		}, func(ctx context.Context, err error) error {
			utils.LogCtx(log.Error(), ctx).Err(err).Interface("msger", mr).Msg("RecvMsg Hystrix")
			return err
		})
	} else {
		// 消息放入协程池中
		if TcpParamConf.Get().MsgSeq {
			ts.seq.Submit(func() {
				ts.onMsg(ctx, mr)
			})
		} else {
			groupId := mr.GroupId()
			if groupId != nil {
				ts.groupSeq.Submit(groupId, func() {
					ts.onMsg(ctx, mr)
				})
			} else {
				utils.Submit(func() {
					ts.onMsg(ctx, mr)
				})
			}
		}
	}
}

func (ts *TcpService[ServiceInfo]) onMsg(ctx context.Context, mr msger.RecvMsger) {
	if handle, _ := ts.g.tb.Dispatch(ctx, mr, ts, fmt.Sprintf("RecvMsg %s Dispatch", ts.ConnName())); handle {
	} else {
		// 日志
		logLevel := msger.ParamConf.Get().LogLevel.MsgLevel(mr)
		if logLevel >= int(log.Logger.GetLevel()) {
			utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Interface("msger", mr).Msgf("RecvMsg %s", ts.ConnName())
		}
		ts.g.tb.event.OnMsg(ctx, mr, ts)
	}
}
