package gnetserver

// https://github.com/yuwf

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"

	"gobase/utils"

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
	seq        *utils.Sequence            // 消息顺序处理工具 协程安全
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
	if ParamConf.Get().MsgSeq {
		gc.seq = &utils.Sequence{}
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

func (gc *GNetClient[ClientInfo]) Send(data []byte) error {
	if gc.event != nil {
		msgLog := zerolog.Dict()
		buf, err := gc.event.Encode(data, gc, msgLog)
		if err != nil {
			log.Error().Str("Name", gc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("Send error")
			return err
		}
		if gc.wsh != nil {
			err = wsutil.WriteServerBinary(gc.wsh, buf)
		} else {
			err = gc.conn.AsyncWrite(buf)
		}
		if err != nil {
			log.Error().Str("Name", gc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("Send error")
			return err
		}
		logVo := reflect.ValueOf(msgLog).Elem()
		logBuf := logVo.FieldByName("buf")
		if len(logBuf.Bytes()) > 1 {
			log.Debug().Str("Name", gc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("Send")
		}
		return nil
	}
	err := errors.New("encode is empty")
	log.Error().Str("Name", gc.ConnName()).Err(err).Int("Size", len(data)).Msg("Send error")
	return err
}

func (gc *GNetClient[ClientInfo]) SendMsg(msg interface{}) error {
	if gc.event != nil {
		msgLog := zerolog.Dict()
		buf, err := gc.event.EncodeMsg(msg, gc, msgLog)
		if err != nil {
			log.Error().Str("Name", gc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendMsg error")
			return err
		}
		if gc.wsh != nil {
			err = wsutil.WriteServerBinary(gc.wsh, buf)
		} else {
			err = gc.conn.AsyncWrite(buf)
		}
		if err != nil {
			log.Error().Str("Name", gc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendMsg error")
			return err
		}
		logVo := reflect.ValueOf(msgLog).Elem()
		logBuf := logVo.FieldByName("buf")
		if len(logBuf.Bytes()) > 1 {
			log.Debug().Str("Name", gc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendMsg")
		}
		return nil
	}
	err := errors.New("encode is empty")
	log.Error().Str("Name", gc.ConnName()).Err(err).Interface("Msg", msg).Msg("SendMsg error")
	return err
}

func (gc *GNetClient[ClientInfo]) SendText(data []byte) error {
	if gc.event != nil {
		msgLog := zerolog.Dict()
		buf, err := gc.event.EncodeText(data, gc, msgLog)
		if err != nil {
			log.Error().Str("Name", gc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("Send error")
			return err
		}
		if gc.wsh != nil {
			err = wsutil.WriteServerText(gc.wsh, buf)
		} else {
			err = gc.conn.AsyncWrite(buf)
		}
		if err != nil {
			log.Error().Str("Name", gc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendText error")
			return err
		}
		logVo := reflect.ValueOf(msgLog).Elem()
		logBuf := logVo.FieldByName("buf")
		if len(logBuf.Bytes()) > 1 {
			log.Debug().Str("Name", gc.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendText")
		}
		return nil
	}
	err := errors.New("encode is empty")
	log.Error().Str("Name", gc.ConnName()).Err(err).Int("Size", len(data)).Msg("SendText error")
	return err
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
		// 消息放入协程池中
		if gc.seq != nil {
			gc.seq.Submit(func() {
				gc.event.OnMsg(ctx, msg, gc)
			})
		} else {
			utils.Submit(func() {
				gc.event.OnMsg(ctx, msg, gc)
			})
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
