package gnetserver

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/yuwf/gobase/utils"

	"github.com/gobwas/ws/wsutil"
	"github.com/panjf2000/gnet"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type ClientNamer interface {
	ClientName() string
}
type ClientCreater interface {
	ClientCreate()
}

// ClientInfo是和业务相关的客户端信息结构
// 如果ClientInfo存在ClientCreate函数，创建链接时会调用
// 如果ClientInfo存在ClientName函数，输出日志是会调用
type GNetClient[ClientInfo any] struct {
	// 本身不可修改对象
	conn       gnet.Conn                  // gnet连接对象
	removeAddr net.TCPAddr                // 拷贝出来 防止conn关闭时发生变化
	localAddr  net.TCPAddr                //
	event      GNetEvent[ClientInfo]      // 事件处理器
	seq        utils.Sequence             // 消息顺序处理工具 协程安全
	info       *ClientInfo                // 客户端信息 内容修改需要外层加锁控制
	connName   func() string              // 日志调使用，输出连接名字，优先会调用ClientInfo.ClientName()函数
	wsh        *gnetWSHandler[ClientInfo] // websocket处理

	ctx context.Context // 本连接的上下文

	closeReason error // 关闭原因
}

func newGNetClient[ClientInfo any](conn gnet.Conn, event GNetEvent[ClientInfo]) *GNetClient[ClientInfo] {
	gc := &GNetClient[ClientInfo]{
		conn:       conn,
		removeAddr: *conn.RemoteAddr().(*net.TCPAddr),
		localAddr:  *conn.LocalAddr().(*net.TCPAddr),
		event:      event,
		info:       new(ClientInfo),
		ctx:        context.TODO(),
	}
	// 调用对象的ClientName函数
	namer, ok := any(gc.info).(ClientNamer)
	if ok {
		gc.connName = func() string {
			name := namer.ClientName()
			if len(name) == 0 || name == "0" {
				return gc.removeAddr.String()
			}
			return name
		}
	}
	// 调用对象的ClientCreate函数
	creater, ok := any(gc.info).(ClientCreater)
	if ok {
		creater.ClientCreate()
	}
	return gc
}

func (gc *GNetClient[ClientInfo]) RemoteAddr() string {
	return gc.removeAddr.String()
}

func (gc *GNetClient[ClientInfo]) LocalAddr() string {
	return gc.localAddr.String()
}

func (gc *GNetClient[ClientInfo]) Info() *ClientInfo {
	return gc.info
}

func (gc *GNetClient[ClientInfo]) ConnName() string {
	return gc.connName()
}

func (gc *GNetClient[ClientInfo]) Send(ctx context.Context, data []byte) error {
	var err error
	if len(data) == 0 {
		err = errors.New("data is empty")
		utils.LogCtx(log.Error(), ctx).Str("Name", gc.ConnName()).Msg("Send error")
		return err
	}
	if gc.wsh != nil {
		err = wsutil.WriteServerBinary(gc.wsh, data)
	} else {
		err = gc.conn.AsyncWrite(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", gc.ConnName()).Err(err).Int("Size", len(data)).Msg("Send error")
		return err
	}
	utils.LogCtx(log.Debug(), ctx).Str("Name", gc.ConnName()).Int("Size", len(data)).Msg("Send")
	return nil
}

// SendMsg 发送消息对象，会调用消息对象的MsgMarshal来编码消息
// 消息对象可实现zerolog.LogObjectMarshaler接口，更好的输出日志，通过ParamConf.LogLevelMsg配置可控制日志级别
func (gc *GNetClient[ClientInfo]) SendMsg(ctx context.Context, msg utils.SendMsger) error {
	if msg == nil {
		err := errors.New("msg is empty")
		utils.LogCtx(log.Error(), ctx).Str("Name", gc.ConnName()).Msg("SendMsg error")
		return err
	}
	data, err := msg.MsgMarshal()
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", gc.ConnName()).Err(err).Interface("Msg", msg).Msg("SendMsg error")
		return err
	}
	if gc.wsh != nil {
		err = wsutil.WriteServerBinary(gc.wsh, data)
	} else {
		err = gc.conn.AsyncWrite(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", gc.ConnName()).Err(err).Interface("Msg", msg).Msg("SendMsg error")
		return err
	}
	// 日志
	logLevel := ParamConf.Get().MsgLogLevel(msg.MsgID())
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Str("Name", gc.ConnName()).Interface("Msg", msg).Msg("SendMsg")
	}
	return nil
}

func (gc *GNetClient[ClientInfo]) SendText(ctx context.Context, data []byte) error {
	var err error
	if len(data) == 0 {
		err = errors.New("data is empty")
		utils.LogCtx(log.Error(), ctx).Str("Name", gc.ConnName()).Msg("SendText error")
		return err
	}
	if gc.wsh != nil {
		err = wsutil.WriteServerText(gc.wsh, data)
	} else {
		err = gc.conn.AsyncWrite(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Str("Name", gc.ConnName()).Err(err).Int("Size", len(data)).Msg("SendText error")
		return err
	}
	utils.LogCtx(log.Debug(), ctx).Str("Name", gc.ConnName()).Int("Size", len(data)).Msg("SendText")
	return nil
}

// 会回调event的OnDisConnect
// 若想不回调使用 GNetServer.CloseClient
func (gc *GNetClient[ClientInfo]) Close(err error) {
	//if gc.wsh != nil && gc.wsh.upgrade { // 这个不需要 发送客户可能还会回OpClose
	//	wsutil.WriteServerMessage(gc.wsh, ws.OpClose, nil)
	//}
	gc.closeReason = err
	gc.conn.Close()
}

//收到数据时调用
func (gc *GNetClient[ClientInfo]) recv(ctx context.Context, buf []byte) (int, error) {
	if gc.event == nil {
		return len(buf), nil
	}

	readlen := 0
	for {
		msg, l, err := gc.event.DecodeMsg(ctx, buf[readlen:], gc)
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
			ctx := gc.event.Context(ctx, msg)
			// 消息放入协程池中
			if ParamConf.Get().MsgSeq {
				gc.seq.Submit(func() {
					gc.event.OnMsg(ctx, msg, gc)
				})
			} else {
				utils.Submit(func() {
					gc.event.OnMsg(ctx, msg, gc)
				})
			}
		}
		if len(buf)-readlen == 0 {
			break // 不需要继续读取了
		}
	}
	//websocket帧数据没有解析出完整的消息数据，暂时返回错误
	if gc.wsh != nil && readlen != len(buf) {
		return 0, errors.New("wsframe not decode msg")
	}
	return readlen, nil
}
