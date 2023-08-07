package tcpserver

// https://github.com/yuwf

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/gobwas/ws/wsutil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/yuwf/gobase/tcp"
	"github.com/yuwf/gobase/utils"
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
	seq        *utils.Sequence           // 消息顺序处理工具 协程安全
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
	if ParamConf.Get().MsgSeq {
		tc.seq = &utils.Sequence{}
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
		ch := value.(chan interface{})
		ch <- nil
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

func (tc *TCPClient[ClientInfo]) Send(data []byte) error {
	if tc.event != nil {
		msgLog := zerolog.Dict()
		buf, err := tc.event.Encode(data, tc, msgLog)
		if err != nil {
			log.Error().Str("Name", tc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("Send error")
			return err
		}
		if tc.wsh != nil {
			err = wsutil.WriteServerBinary(tc.wsh, buf)
		} else {
			err = tc.conn.Send(buf)
		}
		if err != nil {
			log.Error().Str("Name", tc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("Send error")
			return err
		}
		logVo := reflect.ValueOf(msgLog).Elem()
		logBuf := logVo.FieldByName("buf")
		if len(logBuf.Bytes()) > 1 {
			log.Debug().Str("Name", tc.ConnName()).Dict("Msg", msgLog).Msg("Send")
		}
		return nil
	}
	err := errors.New("encode is empty")
	log.Error().Str("Name", tc.ConnName()).Err(err).Int("Size", len(data)).Msg("Send error")
	return err
}

func (tc *TCPClient[ClientInfo]) SendMsg(msg interface{}) error {
	if tc.event != nil {
		msgLog := zerolog.Dict()
		buf, err := tc.event.EncodeMsg(msg, tc, msgLog)
		if err != nil {
			log.Error().Str("Name", tc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendMsg error")
			return err
		}
		if tc.wsh != nil {
			err = wsutil.WriteServerBinary(tc.wsh, buf)
		} else {
			err = tc.conn.Send(buf)
		}
		if err != nil {
			log.Error().Str("Name", tc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendMsg error")
			return err
		}
		logVo := reflect.ValueOf(msgLog).Elem()
		logBuf := logVo.FieldByName("buf")
		if len(logBuf.Bytes()) > 1 {
			log.Debug().Str("Name", tc.ConnName()).Dict("Msg", msgLog).Msg("SendMsg")
		}
		return nil
	}
	err := errors.New("encode is empty")
	log.Error().Str("Name", tc.ConnName()).Err(err).Interface("Msg", msg).Msg("SendMsg error")
	return err
}

func (tc *TCPClient[ClientInfo]) SendText(data []byte) error {
	if tc.event != nil {
		msgLog := zerolog.Dict()
		buf, err := tc.event.EncodeText(data, tc, msgLog)
		if err != nil {
			log.Error().Str("Name", tc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("Send error")
			return err
		}
		if tc.wsh != nil {
			err = wsutil.WriteServerText(tc.wsh, buf)
		} else {
			err = tc.conn.Send(buf)
		}
		if err != nil {
			log.Error().Str("Name", tc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendText error")
			return err
		}
		logVo := reflect.ValueOf(msgLog).Elem()
		logBuf := logVo.FieldByName("buf")
		if len(logBuf.Bytes()) > 1 {
			log.Debug().Str("Name", tc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendText")
		}
		return nil
	}
	err := errors.New("encode is empty")
	log.Error().Str("Name", tc.ConnName()).Err(err).Int("Size", len(data)).Msg("SendText error")
	return err
}

// 发送RPC消息并等待消息回复，需要依赖event.CheckRPCResp来判断是否rpc调用
// 成功返回解析后的消息
func (tc *TCPClient[ClientInfo]) SendRPCMsg(rpcId interface{}, msg interface{}, timeout time.Duration) (interface{}, error) {
	if tc.event != nil {
		_, ok := tc.rpc.Load(rpcId)
		if ok {
			err := errors.New("rpcId exist")
			log.Error().Str("Name", tc.ConnName()).Err(err).Interface("rpcID", rpcId).Interface("Msg", msg).Msg("SendRPCMsg error")
			return nil, err
		}
		msgLog := zerolog.Dict()
		buf, err := tc.event.EncodeMsg(msg, tc, msgLog)
		if err != nil {
			log.Error().Str("Name", tc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendRPCMsg error")
			return nil, err
		}
		if tc.wsh != nil {
			err = wsutil.WriteServerBinary(tc.wsh, buf)
		} else {
			err = tc.conn.Send(buf)
		}
		if err != nil {
			log.Error().Str("Name", tc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendRPCMsg error")
			return nil, err
		}
		logVo := reflect.ValueOf(msgLog).Elem()
		logBuf := logVo.FieldByName("buf")
		if len(logBuf.Bytes()) > 1 {
			log.Debug().Str("Name", tc.ConnName()).Dict("Msg", msgLog).Msg("SendRPCMsg")
		}
		ch := make(chan interface{})
		tc.rpc.Store(rpcId, ch)
		// 协程让出，等待消息回复或超时
		runtime.Gosched()
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
		tc.rpc.Delete(rpcId)
		close(ch)
		if resp == nil && err == nil { //clear函数的调用会触发此情况
			err = errors.New("close")
		}
		if err != nil {
			log.Error().Str("Name", tc.ConnName()).Err(err).Interface("Msg", msg).Msg("SendRPCMsg error")
		}
		return resp, err
	}
	err := errors.New("encode is empty")
	log.Error().Str("Name", tc.ConnName()).Err(err).Interface("Msg", msg).Msg("SendRPCMsg error")
	return nil, err
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
		// rpc消息检查
		rpcId := tc.event.CheckRPCResp(msg)
		if rpcId != nil {
			// rpc
			rpc, ok := tc.rpc.Load(rpcId)
			if ok {
				ch := rpc.(chan interface{})
				ch <- msg
			} else {
				// 没找到可能是超时了也可能是CheckRPCResp出错了 也交给OnMsg执行
				// 消息放入协程池中
				if tc.seq != nil {
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
			if tc.seq != nil {
				tc.seq.Submit(func() {
					tc.event.OnMsg(ctx, msg, tc)
				})
			} else {
				utils.Submit(func() {
					tc.event.OnMsg(ctx, msg, tc)
				})
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
