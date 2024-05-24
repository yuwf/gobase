package tcpserver

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/yuwf/gobase/tcp"
	"github.com/yuwf/gobase/utils"

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
	conn       *tcp.TCPConn              // 连接对象
	removeAddr net.TCPAddr               // 拷贝出来 防止conn关闭时发生变化
	localAddr  net.TCPAddr               //
	event      TCPEvent[ClientInfo]      // 事件处理器
	seq        utils.Sequence            // 消息顺序处理工具 协程安全
	info       *ClientInfo               // 客户端信息 内容修改需要外层加锁控制
	connName   func() string             // // 日志调使用，输出连接名字，优先会调用ClientInfo.ClientName()函数
	wsh        *tcpWSHandler[ClientInfo] // websocket处理

	ctx context.Context // 本连接的上下文

	//RPC消息使用 [rpcid:chan interface{}]
	rpc *sync.Map

	closeReason error // 关闭原因
}

func newTCPClient[ClientInfo any](conn *tcp.TCPConn, event TCPEvent[ClientInfo]) *TCPClient[ClientInfo] {
	tc := &TCPClient[ClientInfo]{
		conn:       conn,
		removeAddr: *conn.RemoteAddr(),
		localAddr:  *conn.LocalAddr(),
		event:      event,
		info:       new(ClientInfo),
		ctx:        context.TODO(),
		rpc:        new(sync.Map),
	}

	// 调用对象的ClientName和ClientCreate函数
	clienter, ok := any(tc.info).(ClientNamer)
	if ok {
		tc.connName = func() string {
			name := clienter.ClientName()
			if len(name) == 0 || name == "0" {
				return tc.removeAddr.String()
			}
			return name
		}
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
			ch := rpc.(chan interface{})
			ch <- nil // 写一个空的
			close(ch) // 删除的地方负责关闭
		}
		return true
	})
}

func (tc *TCPClient[ClientInfo]) RemoteAddr() string {
	return tc.removeAddr.String()
}

func (tc *TCPClient[ClientInfo]) LocalAddr() string {
	return tc.localAddr.String()
}

func (tc *TCPClient[ClientInfo]) Info() *ClientInfo {
	return tc.info
}

func (tc *TCPClient[ClientInfo]) ConnName() string {
	return tc.connName()
}

func (tc *TCPClient[ClientInfo]) Send(ctx context.Context, data []byte) error {
	var err error
	if len(data) == 0 {
		err = errors.New("data is empty")
		utils.LogCtx(log.Error(), ctx).Str("Name", tc.ConnName()).Msg("Send error")
		return err
	}
	if tc.wsh != nil {
		err = wsutil.WriteServerBinary(tc.wsh, data)
	} else {
		err = tc.conn.Send(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", tc.ConnName()).Err(err).Int("Size", len(data)).Msg("Send error")
		return err
	}
	utils.LogCtx(log.Debug(), ctx).Str("Name", tc.ConnName()).Int("Size", len(data)).Msg("Send")
	return nil
}

// SendMsg 发送消息对象，会调用消息对象的MsgMarshal来编码消息
// 消息对象可实现zerolog.LogObjectMarshaler接口，更好的输出日志，通过ParamConf.LogLevelMsg配置可控制日志级别
func (tc *TCPClient[ClientInfo]) SendMsg(ctx context.Context, msg utils.SendMsger) error {
	if msg == nil {
		err := errors.New("msg is empty")
		utils.LogCtx(log.Error(), ctx).Str("Name", tc.ConnName()).Msg("SendMsg error")
		return err
	}
	data, err := msg.MsgMarshal()
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", tc.ConnName()).Err(err).Interface("Msg", msg).Msg("SendMsg error")
		return err
	}
	if tc.wsh != nil {
		err = wsutil.WriteServerBinary(tc.wsh, data)
	} else {
		err = tc.conn.Send(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", tc.ConnName()).Err(err).Interface("Msg", msg).Msg("SendMsg error")
		return err
	}
	// 日志
	logLevel := ParamConf.Get().MsgLogLevel(msg.MsgID())
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("Name", tc.ConnName()).Interface("Msg", msg).Msg("SendMsg")
	}
	return nil
}

func (tc *TCPClient[ClientInfo]) SendText(ctx context.Context, data []byte) error {
	var err error
	if len(data) == 0 {
		err = errors.New("data is empty")
		utils.LogCtx(log.Error(), ctx).Str("Name", tc.ConnName()).Msg("SendText error")
		return err
	}
	if tc.wsh != nil {
		err = wsutil.WriteServerText(tc.wsh, data)
	} else {
		err = tc.conn.Send(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", tc.ConnName()).Err(err).Int("Size", len(data)).Msg("SendText error")
		return err
	}
	utils.LogCtx(log.Debug(), ctx).Str("Name", tc.ConnName()).Int("Size", len(data)).Msg("SendText")
	return nil
}

// SendRPCMsg 发送RPC消息并等待消息回复，需要依赖event.CheckRPCResp来判断是否rpc调用
// 成功返回解析后的消息
// 消息对象可实现zerolog.LogObjectMarshaler接口，更好的输出日志，通过ParamConf.LogLevelMsg配置可控制日志级别
func (tc *TCPClient[ClientInfo]) SendRPCMsg(ctx context.Context, rpcId interface{}, msg utils.SendMsger, timeout time.Duration) (interface{}, error) {
	if msg == nil {
		err := errors.New("msg is empty")
		utils.LogCtx(log.Error(), ctx).Str("Name", tc.ConnName()).Msg("SendRPCMsg error")
		return nil, err
	}
	data, err := msg.MsgMarshal()
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", tc.ConnName()).Err(err).Interface("Msg", msg).Msg("SendRPCMsg error")
		return nil, err
	}
	_, ok := tc.rpc.Load(rpcId)
	if ok {
		err := errors.New("rpcId exist")
		utils.LogCtx(log.Error(), ctx).Str("Name", tc.ConnName()).Err(err).Interface("rpcID", rpcId).Interface("Msg", msg).Msg("SendRPCMsg error")
		return nil, err
	}
	// 先添加一个记录，防止Send还没出来就收到了回复
	ch := make(chan interface{})
	tc.rpc.Store(rpcId, ch)
	defer func() {
		_, ok = tc.rpc.LoadAndDelete(rpcId)
		if ok {
			close(ch) // 删除的地方负责关闭
		}
	}()
	// 发送
	if tc.wsh != nil {
		err = wsutil.WriteServerBinary(tc.wsh, data)
	} else {
		err = tc.conn.Send(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", tc.ConnName()).Err(err).Interface("Msg", msg).Msg("SendRPCMsg error")
		return nil, err
	}
	// 日志
	logLevel := ParamConf.Get().MsgLogLevel(msg.MsgID())
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("Name", tc.ConnName()).Interface("Msg", msg).Msg("SendRPCMsg")
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
		utils.LogCtx(log.Error(), ctx).Str("Name", tc.ConnName()).Err(err).Msg("SendRPCMsg resp error")
	}
	// 日志
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("Name", tc.ConnName()).Interface("Resp", resp).Msg("RecvRPCMsg")
	}
	return resp, err
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

//收到数据时调用
func (tc *TCPClient[ClientInfo]) recv(ctx context.Context, buf []byte) (int, error) {
	if tc.event == nil {
		return len(buf), nil
	}

	readlen := 0
	for {
		msg, l, err := tc.event.DecodeMsg(ctx, buf[readlen:], tc)
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
		if l == 0 || msg == nil {
			break
		}
		if msg != nil {
			ctx = tc.event.Context(ctx, msg)
			// rpc消息检查
			rpcId := tc.event.CheckRPCResp(msg)
			if rpcId != nil {
				// rpc
				rpc, ok := tc.rpc.LoadAndDelete(rpcId)
				if ok {
					ch := rpc.(chan interface{})
					ch <- msg
					close(ch) // 删除的地方负责关闭
				} else {
					// 没找到可能是超时了也可能是CheckRPCResp出错了 也交给OnMsg执行
					// 消息放入协程池中
					if ParamConf.Get().MsgSeq {
						tc.seq.Submit(func() {
							tc.event.OnMsg(ctx, msg, tc)
						})
					} else {
						utils.Submit(func() {
							tc.event.OnMsg(ctx, msg, tc)
						})
					}
				}
			} else {
				// 消息放入协程池中
				if ParamConf.Get().MsgSeq {
					tc.seq.Submit(func() {
						tc.event.OnMsg(ctx, msg, tc)
					})
				} else {
					utils.Submit(func() {
						tc.event.OnMsg(ctx, msg, tc)
					})
				}
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
