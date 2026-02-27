package gnetserver

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/yuwf/gobase/msger"
	"github.com/yuwf/gobase/utils"

	"github.com/afex/hystrix-go/hystrix"
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
	conn       gnet.Conn             // gnet连接对象
	removeAddr net.TCPAddr           // 拷贝出来 防止conn关闭时发生变化
	localAddr  net.TCPAddr           //
	event      GNetEvent[ClientInfo] // 事件处理器
	md         *msger.MsgDispatch    // 消息分发
	hook       []GNetHook[ClientInfo]
	seq        utils.Sequence             // 消息顺序处理工具 协程安全
	groupSeq   utils.GroupSequence        // 分组执行的消息, 消息设置为非顺序处理的才会分组
	info       *ClientInfo                // 客户端信息 内容修改需要外层加锁控制
	connName   func() string              // 日志调使用，输出连接名字，优先会调用ClientInfo.ClientName()函数
	wsh        *gnetWSHandler[ClientInfo] // websocket处理

	ctx          context.Context // 本连接的上下文
	lastRecvTime int64           // 最近一次接受数据的时间戳 微妙 原子访问
	lastSendTime int64           // 最近一次接受数据的时间戳 微妙 原子访问

	closeReason error // 关闭原因
}

func newGNetClient[ClientInfo any](conn gnet.Conn, event GNetEvent[ClientInfo], md *msger.MsgDispatch, hook []GNetHook[ClientInfo]) *GNetClient[ClientInfo] {
	gc := &GNetClient[ClientInfo]{
		conn:         conn,
		removeAddr:   *conn.RemoteAddr().(*net.TCPAddr),
		localAddr:    *conn.LocalAddr().(*net.TCPAddr),
		event:        event,
		md:           md,
		hook:         hook,
		info:         new(ClientInfo),
		ctx:          context.TODO(),
		lastRecvTime: time.Now().UnixMicro(),
	}
	// 调用对象的ClientCreate函数
	creater, ok := any(gc.info).(ClientCreater)
	if ok {
		creater.ClientCreate()
	}
	return gc
}

func (gc *GNetClient[ClientInfo]) RemoteAddr() net.TCPAddr {
	return gc.removeAddr
}

func (gc *GNetClient[ClientInfo]) LocalAddr() net.TCPAddr {
	return gc.localAddr
}

func (gc *GNetClient[ClientInfo]) Info() *ClientInfo {
	return gc.info
}

func (gc *GNetClient[ClientInfo]) InfoI() interface{} {
	return gc.info
}

func (gc *GNetClient[ClientInfo]) ConnName() string {
	if gc.connName == nil {
		return gc.removeAddr.String()
	}
	return gc.connName()
}

// 消息堆积数量，顺序处理和分组处理的才能计算
func (gc *GNetClient[ClientInfo]) RecvSeqCount() int {
	return gc.seq.Len() + gc.groupSeq.Len()
}

func (gc *GNetClient[ClientInfo]) LastRecvTime() time.Time {
	return time.UnixMicro(atomic.LoadInt64(&gc.lastRecvTime))
}

func (gc *GNetClient[ClientInfo]) LastSendTime() time.Time {
	return time.UnixMicro(atomic.LoadInt64(&gc.lastSendTime))
}

func (gc *GNetClient[ClientInfo]) Send(ctx context.Context, data []byte) error {
	var err error
	if len(data) == 0 {
		err = errors.New("data is empty")
		utils.LogCtx(log.Error(), ctx).Msgf("Send %s error", gc.ConnName())
		return err
	}
	// 回调
	defer func() {
		defer utils.HandlePanic()
		for _, h := range gc.hook {
			h.OnSend(gc, len(data))
		}
	}()
	// 发送
	if gc.wsh != nil {
		err = wsutil.WriteServerBinary(gc.wsh, data)
	} else {
		err = gc.conn.AsyncWrite(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Msgf("Send %s error", gc.ConnName())
		return err
	}
	atomic.StoreInt64(&gc.lastSendTime, time.Now().UnixMicro())
	// 日志
	utils.LogCtx(log.Debug(), ctx).Int("size", len(data)).Msgf("Send %s", gc.ConnName())
	return nil
}

// SendMsg 发送消息对象，会调用消息对象的MsgMarshal来编码消息
// 消息对象可实现zerolog.LogObjectMarshaler接口，更好的输出日志，通过ParamConf.LogLevelMsg配置可控制日志级别
func (gc *GNetClient[ClientInfo]) SendMsg(ctx context.Context, msg msger.Msger) error {
	if msg == nil {
		err := errors.New("msg is empty")
		utils.LogCtx(log.Error(), ctx).Msgf("SendMsg %s error", gc.ConnName())
		return err
	}
	data, err := msg.MsgMarshal()
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Interface("msger", msg).Msgf("SendMsg %s error", gc.ConnName())
		return err
	}
	// 回调
	defer func() {
		defer utils.HandlePanic()
		for _, h := range gc.hook {
			h.OnSendMsg(gc, msg, len(data))
		}
	}()
	// 发送
	if gc.wsh != nil {
		err = wsutil.WriteServerBinary(gc.wsh, data)
	} else {
		err = gc.conn.AsyncWrite(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Interface("msger", msg).Msgf("SendMsg %s error", gc.ConnName())
		return err
	}
	atomic.StoreInt64(&gc.lastSendTime, time.Now().UnixMicro())
	// 日志
	logLevel := msger.ParamConf.Get().LogLevel.MsgLevel(msg)
	if logLevel >= int(log.Logger.GetLevel()) {
		utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Interface("msger", msg).Msgf("SendMsg %s", gc.ConnName())
	}
	return nil
}

func (gc *GNetClient[ClientInfo]) SendText(ctx context.Context, data []byte) error {
	var err error
	if len(data) == 0 {
		err = errors.New("data is empty")
		utils.LogCtx(log.Error(), ctx).Msgf("SendText %s error", gc.ConnName())
		return err
	}
	// 回调
	defer func() {
		defer utils.HandlePanic()
		for _, h := range gc.hook {
			h.OnSendText(gc, len(data))
		}
	}()
	// 发送
	if gc.wsh != nil {
		err = wsutil.WriteServerText(gc.wsh, data)
	} else {
		err = gc.conn.AsyncWrite(data)
	}
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Int("size", len(data)).Msgf("SendText %s error", gc.ConnName())
		return err
	}
	atomic.StoreInt64(&gc.lastSendTime, time.Now().UnixMicro())
	// 日志
	utils.LogCtx(log.Debug(), ctx).Int("size", len(data)).Msgf("SendText %s", gc.ConnName())
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

// 收到数据时调用
func (gc *GNetClient[ClientInfo]) recv(ctx context.Context, buf []byte) (int, error) {
	if gc.event == nil {
		return len(buf), nil
	}

	readlen := 0
	for {
		mr, l, err := gc.decode(ctx, buf[readlen:])
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
			traceName := mr.MsgID()
			if mner, _ := any(mr).(msger.MsgerName); mner != nil {
				traceName = mner.MsgName()
			}
			ctx2 := utils.CtxSetTrace(ctx, 0, traceName) // 拷贝出一个新的context，防止污染了其他消息

			gc.handle(ctx2, mr)

			// 回调
			func() {
				defer utils.HandlePanic()
				for _, h := range gc.hook {
					h.OnRecvMsg(gc, mr, l)
				}
			}()
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

func (gc *GNetClient[ClientInfo]) decode(ctx context.Context, buf []byte) (msger.RecvMsger, int, error) {
	var mr msger.RecvMsger
	var l int
	var err error
	defer utils.HandlePanic2(func(r any) {
		err = fmt.Errorf("decode panic: %v", r)
	})

	mr, l, err = gc.event.DecodeMsg(ctx, buf, gc)
	return mr, l, err
}

func (gc *GNetClient[ClientInfo]) handle(ctx context.Context, mr msger.RecvMsger) {
	// 熔断
	if name, ok := msger.ParamConf.Get().IsHystrixMsg(mr.MsgID()); ok {
		hystrix.DoC(ctx, name, func(ctx context.Context) error {
			// 消息放入协程池中
			if ParamConf.Get().MsgSeq {
				gc.seq.Submit(func() {
					gc.onMsg(ctx, mr)
				})
			} else {
				groupId := mr.GroupId()
				if groupId != nil {
					gc.groupSeq.Submit(groupId, func() {
						gc.onMsg(ctx, mr)
					})
				} else {
					utils.Submit(func() {
						gc.onMsg(ctx, mr)
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
			gc.seq.Submit(func() {
				gc.onMsg(ctx, mr)
			})
		} else {
			groupId := mr.GroupId()
			if groupId != nil {
				gc.groupSeq.Submit(groupId, func() {
					gc.onMsg(ctx, mr)
				})
			} else {
				utils.Submit(func() {
					gc.onMsg(ctx, mr)
				})
			}
		}
	}
}

func (gc *GNetClient[ClientInfo]) onMsg(ctx context.Context, mr msger.RecvMsger) {
	if handle, _ := gc.md.Dispatch(ctx, mr, gc, fmt.Sprintf("RecvMsg %s Dispatch", gc.ConnName())); handle {
	} else {
		// 日志
		logLevel := msger.ParamConf.Get().LogLevel.MsgLevel(mr)
		if logLevel >= int(log.Logger.GetLevel()) {
			utils.LogCtx(log.WithLevel(zerolog.Level(logLevel)), ctx).Interface("msger", mr).Msgf("RecvMsg %s", gc.ConnName())
		}
		gc.event.OnMsg(ctx, mr, gc)
	}
}
