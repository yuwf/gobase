package tcpserver

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuwf/gobase/msger"
	"github.com/yuwf/gobase/tcp"
	"github.com/yuwf/gobase/utils"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/gobwas/ws/wsutil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type ClientNamer interface {
	ClientName() string
}
type ClientCreater interface {
	ClientCreate()
}

// TCPClient是TCPServer创建的连接对象
// ClientInfo是和业务相关的客户端信息结构
// 如果ClientInfo存在ClientCreate函数，创建链接时会调用
// 如果ClientInfo存在ClientName函数，输出日志是会调用
type TCPClient[ClientInfo any] struct {
	// 本身不可修改对象
	conn       *tcp.TCPConn         // 连接对象
	removeAddr net.TCPAddr          // 拷贝出来 防止conn关闭时发生变化
	localAddr  net.TCPAddr          //
	event      TCPEvent[ClientInfo] // 事件处理器
	md         *msger.MsgDispatch   // 消息分发
	hook       []TCPHook[ClientInfo]
	seq        utils.Sequence            // 消息顺序处理工具 协程安全
	groupSeq   utils.GroupSequence       // 分组执行的消息, 消息设置为非顺序处理的才会分组
	info       *ClientInfo               // 客户端信息 内容修改需要外层加锁控制
	connName   func() string             // // 日志调使用，输出连接名字，优先会调用ClientInfo.ClientName()函数
	wsh        *tcpWSHandler[ClientInfo] // websocket处理

	ctx          context.Context // 本连接的上下文
	lastRecvTime int64           // 最近一次接受数据的时间戳 微妙 原子访问
	lastSendTime int64           // 最近一次接受数据的时间戳 微妙 原子访问

	//RPC消息使用 [rpcid:chan interface{}]
	rpc *sync.Map

	closeReason error // 关闭原因
}

func newTCPClient[ClientInfo any](conn *tcp.TCPConn, event TCPEvent[ClientInfo], md *msger.MsgDispatch, hook []TCPHook[ClientInfo]) *TCPClient[ClientInfo] {
	tc := &TCPClient[ClientInfo]{
		conn:         conn,
		removeAddr:   *conn.RemoteAddr(),
		localAddr:    *conn.LocalAddr(),
		event:        event,
		md:           md,
		hook:         hook,
		info:         new(ClientInfo),
		ctx:          context.TODO(),
		lastRecvTime: time.Now().UnixMicro(),
		rpc:          new(sync.Map),
	}
	// 调用对象的ClientCreate函数
	creater, ok := any(tc.info).(ClientCreater)
	if ok {
		creater.ClientCreate()
	}
	return tc
}

// 销毁时调用
func (tc *TCPClient[ClientInfo]) clear() {
	// 清空下rpc
	tc.rpc.Range(func(key, value interface{}) bool {
		rpc, ok := tc.rpc.LoadAndDelete(key)
		if ok {
			ch := rpc.(chan msger.RecvMsger)
			close(ch) // 删除的地方负责关闭
		}
		return true
	})
}

func (tc *TCPClient[ClientInfo]) RemoteAddr() net.TCPAddr {
	return tc.removeAddr
}

func (tc *TCPClient[ClientInfo]) LocalAddr() net.TCPAddr {
	return tc.localAddr
}

func (tc *TCPClient[ClientInfo]) Info() *ClientInfo {
	return tc.info
}

func (tc *TCPClient[ClientInfo]) InfoI() interface{} {
	return tc.info
}

func (tc *TCPClient[ClientInfo]) ConnName() string {
	if tc.connName == nil {
		return tc.removeAddr.String()
	}
	return tc.connName()
}

// 消息堆积数量，顺序处理和分组处理的才能计算
func (tc *TCPClient[ClientInfo]) RecvSeqCount() int {
	return tc.seq.Len() + tc.groupSeq.Len()
}

func (tc *TCPClient[ClientInfo]) LastRecvTime() time.Time {
	return time.UnixMicro(atomic.LoadInt64(&tc.lastRecvTime))
}

func (tc *TCPClient[ClientInfo]) LastSendTime() time.Time {
	return time.UnixMicro(atomic.LoadInt64(&tc.lastSendTime))
}

func (tc *TCPClient[ClientInfo]) Send(ctx context.Context, data []byte) error {
	var err error
	if len(data) == 0 {
		err = errors.New("data is empty")
		utils.LogCtx(log.Error(), ctx).Msgf("Send %s error", tc.ConnName())
		return err
	}
	// 回调
	defer func() {
		defer utils.HandlePanic()
		for _, h := range tc.hook {
			h.OnSend(tc, len(data))
		}
	}()
	// 发送
	if tc.wsh != nil {
		err = wsutil.WriteServerBinary(tc.wsh, data)
	} else {
		err = tc.conn.Send(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Int("size", len(data)).Msgf("Send %s error", tc.ConnName())
		return err
	}
	atomic.StoreInt64(&tc.lastSendTime, time.Now().UnixMicro())
	// 日志
	utils.LogCtx(log.Debug(), ctx).Int("size", len(data)).Msgf("Send %s", tc.ConnName())
	return nil
}

// SendMsg 发送消息对象，会调用消息对象的MsgMarshal来编码消息
// 消息对象可实现zerolog.LogObjectMarshaler接口，更好的输出日志，通过ParamConf.LogLevelMsg配置可控制日志级别
func (tc *TCPClient[ClientInfo]) SendMsg(ctx context.Context, msg msger.Msger) error {
	if msg == nil {
		err := errors.New("msg is empty")
		utils.LogCtx(log.Error(), ctx).Msgf("SendMsg %s error", tc.ConnName())
		return err
	}
	data, err := msg.MsgMarshal()
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Interface("msger", msg).Msgf("SendMsg %s error", tc.ConnName())
		return err
	}
	// 回调
	defer func() {
		defer utils.HandlePanic()
		for _, h := range tc.hook {
			h.OnSendMsg(tc, msg, len(data))
		}
	}()
	// 发送
	if tc.wsh != nil {
		err = wsutil.WriteServerBinary(tc.wsh, data)
	} else {
		err = tc.conn.Send(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Interface("msger", msg).Msgf("SendMsg %s error", tc.ConnName())
		return err
	}
	atomic.StoreInt64(&tc.lastSendTime, time.Now().UnixMicro())
	// 日志
	logLevel := msger.ParamConf.Get().LogLevel.MsgLevel(msg)
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Interface("msger", msg).Msgf("SendMsg %s", tc.ConnName())
	}
	return nil
}

func (tc *TCPClient[ClientInfo]) SendText(ctx context.Context, data []byte) error {
	var err error
	if len(data) == 0 {
		err = errors.New("data is empty")
		utils.LogCtx(log.Error(), ctx).Msgf("SendText %s error", tc.ConnName())
		return err
	}
	// 回调
	defer func() {
		defer utils.HandlePanic()
		for _, h := range tc.hook {
			h.OnSendText(tc, len(data))
		}
	}()
	// 发送
	if tc.wsh != nil {
		err = wsutil.WriteServerText(tc.wsh, data)
	} else {
		err = tc.conn.Send(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Int("size", len(data)).Msgf("SendText %s error", tc.ConnName())
		return err
	}
	atomic.StoreInt64(&tc.lastSendTime, time.Now().UnixMicro())
	// 日志
	utils.LogCtx(log.Debug(), ctx).Int("size", len(data)).Msgf("SendText %s", tc.ConnName())
	return nil
}

// SendRPCMsg 发送RPC消息并等待消息回复，需要依赖event.DecodeMsg返回消息的RPCId()来判断是否rpc调用
// 消息对象可实现zerolog.LogObjectMarshaler接口，更好的输出日志，通过ParamConf.LogLevelMsg配置可控制日志级别
// respBody: 解析后的消息体 ，如果respBody不是nil，会调用RecvMsger的BodyUnMarshal解析消息体
// 返回值
// - resp: 回复的消息对象
func (tc *TCPClient[ClientInfo]) SendRPCMsg(ctx context.Context, rpcId interface{}, req msger.Msger, timeout time.Duration, respBody interface{}) (msger.RecvMsger, error) {
	rpcIdV := fmt.Sprintf("%v", rpcId)
	if req == nil {
		err := errors.New("msg is empty")
		utils.LogCtx(log.Error(), ctx).Str("rpcId", rpcIdV).Msgf("SendRPCMsg %s error", tc.ConnName())
		return nil, err
	}
	data, err := req.MsgMarshal()
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Str("rpcId", rpcIdV).Interface("msger", req).Msgf("SendRPCMsg %s error", tc.ConnName())
		return nil, err
	}

	// 先添加一个channel记录，防止Send还没出来就收到了回复，并且判断是否存在一样的
	ch := make(chan msger.RecvMsger, 1) // 使用缓冲channel
	if _, loaded := tc.rpc.LoadOrStore(rpcIdV, ch); loaded {
		close(ch) // 关闭新创建的channel
		err := errors.New("rpcId exist")
		utils.LogCtx(log.Error(), ctx).Err(err).Str("rpcId", rpcIdV).Interface("msger", req).Msgf("SendRPCMsg %s error", tc.ConnName())
		return nil, err
	}
	defer func() {
		if _, ok := tc.rpc.LoadAndDelete(rpcIdV); ok {
			close(ch) // 删除的地方负责关闭
		}
	}()
	// 回调
	entry := time.Now()
	defer func() {
		defer utils.HandlePanic()
		for _, h := range tc.hook {
			h.OnSendRPCMsg(tc, rpcId, req, time.Since(entry), len(data))
		}
	}()
	// 发送
	if tc.wsh != nil {
		err = wsutil.WriteServerBinary(tc.wsh, data)
	} else {
		err = tc.conn.Send(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Str("rpcId", rpcIdV).Interface("msger", req).Msgf("SendRPCMsg %s error", tc.ConnName())
		return nil, err
	}
	atomic.StoreInt64(&tc.lastSendTime, time.Now().UnixMicro())
	// 日志
	logLevel := msger.ParamConf.Get().LogLevel.MsgLevel(req)
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("rpcId", rpcIdV).Interface("msger", req).Msgf("SendRPCMsg %s", tc.ConnName())
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
		utils.LogCtx(log.Error(), ctx).Err(err).Str("rpcId", rpcIdV).Msgf("SendRPCMsg %s resp error", tc.ConnName())
		return nil, err
	}

	// 解析消息体
	if respBody != nil {
		err = resp.BodyUnMarshal(respBody)
		if err != nil {
			utils.LogCtx(log.Error(), ctx).Err(err).Str("rpcId", rpcIdV).Msgf("SendRPCMsg %s resp error", tc.ConnName())
			return resp, err
		}
	}

	// 日志
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("rpcId", rpcIdV).Interface("msger", resp).Msgf("SendRPCMsg %s resp", tc.ConnName())
	}
	return resp, nil
}

// SendRPCMsgAsync 发送异步RPC消息，需要依赖event.DecodeMsg返回消息的RPCId()来判断是否rpc调用
// 消息对象可实现zerolog.LogObjectMarshaler接口，更好的输出日志，通过ParamConf.LogLevelMsg配置可控制日志级别
// 返回值 为nil时，才会调用callback
// callback 参考msger.AsyncRPCCallback说明
func (tc *TCPClient[ClientInfo]) SendAsyncRPCMsg(ctx context.Context, rpcId interface{}, req msger.Msger, timeout time.Duration, callback interface{}) error {
	rpcIdV := fmt.Sprintf("%v", rpcId)
	if req == nil {
		err := errors.New("msg is empty")
		utils.LogCtx(log.Error(), ctx).Str("rpcId", rpcIdV).Msgf("SendAsyncRPCMsg %s error", tc.ConnName())
		return err
	}
	data, err := req.MsgMarshal()
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Str("rpcId", rpcIdV).Interface("msger", req).Msgf("SendAsyncRPCMsg %s error", tc.ConnName())
		return err
	}

	cb, err := msger.GetAsyncCallback(callback)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Str("rpcId", rpcIdV).Interface("msger", req).Msgf("SendAsyncRPCMsg %s error", tc.ConnName())
		return err
	}

	// 先添加一个channel记录，防止Send还没出来就收到了回复，并且判断是否存在一样的
	ch := make(chan msger.RecvMsger, 1) // 使用缓冲channel
	if _, loaded := tc.rpc.LoadOrStore(rpcIdV, ch); loaded {
		close(ch) // 关闭新创建的channel
		err := errors.New("rpcId exist")
		utils.LogCtx(log.Error(), ctx).Err(err).Str("rpcId", rpcIdV).Interface("msger", req).Msgf("SendAsyncRPCMsg %s error", tc.ConnName())
		return err
	}
	// 回调
	entry := time.Now()
	defer func() {
		defer utils.HandlePanic()
		for _, h := range tc.hook {
			h.OnSendRPCMsg(tc, rpcId, req, time.Since(entry), len(data))
		}
	}()
	// 发送
	if tc.wsh != nil {
		err = wsutil.WriteServerBinary(tc.wsh, data)
	} else {
		err = tc.conn.Send(data)
	}
	if err != nil {
		// 发送失败，先删除channel记录
		if _, ok := tc.rpc.LoadAndDelete(rpcIdV); ok {
			close(ch) // 删除的地方负责关闭
		}
		utils.LogCtx(log.Error(), ctx).Err(err).Str("rpcId", rpcIdV).Interface("msger", req).Msgf("SendAsyncRPCMsg %s error", tc.ConnName())
		return err
	}
	atomic.StoreInt64(&tc.lastSendTime, time.Now().UnixMicro())
	// 日志
	logLevel := msger.ParamConf.Get().LogLevel.MsgLevel(req)
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("rpcId", rpcIdV).Interface("msger", req).Msgf("SendAsyncRPCMsg %s", tc.ConnName())
	}

	// 异步等待回复
	utils.Submit(func() {
		defer func() {
			if _, ok := tc.rpc.LoadAndDelete(rpcIdV); ok {
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
			if ParamConf.Get().MsgSeq {
				tc.seq.Submit(func() {
					cb.Call(resp, body, err)
				})
			} else {
				if resp == nil {
					cb.Call(resp, body, err) // 已经在异步协程中了 直接调用
					return
				}
				groupId := resp.GroupId()
				if groupId != nil {
					tc.groupSeq.Submit(groupId, func() {
						cb.Call(resp, body, err)
					})
				} else {
					cb.Call(resp, body, err) // 已经在异步协程中了 直接调用
				}
			}
		}

		if err != nil {
			utils.LogCtx(log.Error(), ctx).Err(err).Str("rpcId", rpcIdV).Msgf("SendAsyncRPCMsg %s resp error", tc.ConnName())
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
					utils.LogCtx(log.Error(), ctx).Err(err).Str("rpcId", rpcIdV).Msgf("SendAsyncRPCMsg %s resp error", tc.ConnName())
					handle(resp, nil, err)
					return
				}
			}
		}

		// 日志
		if logLevel >= int(log.Logger.GetLevel()) {
			utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("rpcId", rpcIdV).Interface("msger", resp).Msgf("SendAsyncRPCMsg %s resp", tc.ConnName())
		}
		handle(resp, respBody, nil)
	})

	return nil
}

// 会回调event的OnClose
// 若想不回调使用 TCPServer.CloseClient
func (tc *TCPClient[ClientInfo]) Close(err error) {
	tc.closeReason = err
	tc.conn.Close(false)
}

// 会等待关闭完成后返回，TCPServer.OnClose调用完之后返回
func (tc *TCPClient[ClientInfo]) CloseWait(err error) {
	tc.closeReason = err
	tc.conn.Close(true)
}

// 收到数据时调用
func (tc *TCPClient[ClientInfo]) recv(ctx context.Context, buf []byte) (int, error) {
	if tc.event == nil {
		return len(buf), nil
	}

	readlen := 0
	for {
		mr, l, err := tc.decode(ctx, buf[readlen:])
		if err != nil {
			return 0, err
		}
		if l < 0 || l > len(buf[readlen:]) {
			err = fmt.Errorf("decode return len is %d, readbuf len is %d", l, buf[readlen:])
			return 0, err
		}
		if l > 0 {
			readlen += l
		}
		if l == 0 || mr == nil {
			break
		}
		if mr != nil {
			// 回调
			func() {
				defer utils.HandlePanic()
				for _, h := range tc.hook {
					h.OnRecvMsg(tc, mr, l)
				}
			}()

			traceName := mr.MsgID()
			if mner, _ := any(mr).(msger.MsgerName); mner != nil {
				traceName = mner.MsgName()
			}
			ctx2 := utils.CtxSetTrace(ctx, mr.TraceId(), traceName) // 拷贝出一个新的context，防止污染了其他消息

			rpcId := mr.RPCId()
			if rpcId != nil {
				// rpc
				rpcIdV := fmt.Sprintf("%v", rpcId)
				rpc, ok := tc.rpc.LoadAndDelete(rpcIdV)
				if ok {
					ch := rpc.(chan msger.RecvMsger)
					ch <- mr
					close(ch) // 删除的地方负责关闭
				} else {
					// 没找到可能是超时了也可能是DecodeMsg没正确返回 也交给OnMsg执行
					tc.handle(ctx2, mr)
				}
			} else {
				tc.handle(ctx2, mr)
			}
		}
		if len(buf)-readlen == 0 {
			break // 不需要继续读取了
		}
	}
	//websocket帧数据没有解析出完整的消息数据，暂时返回错误
	if tc.wsh != nil && readlen != len(buf) {
		return 0, errors.New("wsframe not decode msg")
	}
	return readlen, nil
}

func (tc *TCPClient[ClientInfo]) decode(ctx context.Context, buf []byte) (msger.RecvMsger, int, error) {
	var mr msger.RecvMsger
	var l int
	var err error
	defer utils.HandlePanic2(func(r any) {
		err = fmt.Errorf("decode panic: %v", r)
	})

	mr, l, err = tc.event.DecodeMsg(ctx, buf, tc)
	return mr, l, err
}

func (tc *TCPClient[ClientInfo]) handle(ctx context.Context, mr msger.RecvMsger) {
	// 熔断
	if name, ok := msger.ParamConf.Get().IsHystrixMsg(mr.MsgID()); ok {
		hystrix.DoC(ctx, name, func(ctx context.Context) error {
			// 消息放入协程池中
			if ParamConf.Get().MsgSeq {
				tc.seq.Submit(func() {
					tc.onMsg(ctx, mr)
				})
			} else {
				groupId := mr.GroupId()
				if groupId != nil {
					tc.groupSeq.Submit(groupId, func() {
						tc.onMsg(ctx, mr)
					})
				} else {
					utils.Submit(func() {
						tc.onMsg(ctx, mr)
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
		if ParamConf.Get().MsgSeq {
			tc.seq.Submit(func() {
				tc.onMsg(ctx, mr)
			})
		} else {
			groupId := mr.GroupId()
			if groupId != nil {
				tc.groupSeq.Submit(groupId, func() {
					tc.onMsg(ctx, mr)
				})
			} else {
				utils.Submit(func() {
					tc.onMsg(ctx, mr)
				})
			}
		}
	}
}

func (tc *TCPClient[ClientInfo]) onMsg(ctx context.Context, mr msger.RecvMsger) {
	if handle, _ := tc.md.Dispatch(ctx, mr, tc, fmt.Sprintf("RecvMsg %s Dispatch", tc.ConnName())); handle {
	} else {
		// 日志
		logLevel := msger.ParamConf.Get().LogLevel.MsgLevel(mr)
		if logLevel >= int(log.Logger.GetLevel()) {
			utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Interface("msger", mr).Msgf("RecvMsg %s", tc.ConnName())
		}
		tc.event.OnMsg(ctx, mr, tc)
	}
}
