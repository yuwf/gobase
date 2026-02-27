package gnetserver

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"

	"github.com/yuwf/gobase/msger"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

type GNetEvent[ClientInfo any] interface {
	// 消息注册
	OnMsgReg(md *msger.MsgDispatch)

	// 收到连接
	// 异步顺序调用
	OnConnected(ctx context.Context, gc *GNetClient[ClientInfo])

	// 用户掉线
	// 异步顺序调用
	OnDisConnect(ctx context.Context, gc *GNetClient[ClientInfo])

	// DecodeMsg 解码消息实现
	// 网络协程调用
	// ctx       包括 [CtxKey_WS,CtxKey_Text]
	// 返回值为   msg,len,err
	// msg       解码出的消息体
	// len       解码消息的数据长度，内部根据len来删除已解码的数据
	// err       解码错误，若发生error，服务器将重连
	DecodeMsg(ctx context.Context, data []byte, gc *GNetClient[ClientInfo]) (msger.RecvMsger, int, error)

	// OnRecv 收到消息，解码成功后调用
	// 异步顺序调用 or 异步调用
	// ctx    包括 [CtxKey_WS,CtxKey_Text],CtxKey_traceId,CtxKey_msgId
	OnMsg(ctx context.Context, mr msger.RecvMsger, gc *GNetClient[ClientInfo])

	// OnTick 每秒调用一次
	// 异步顺序调用
	// ctx    包括 [CtxKey_WS],CtxKey_traceId,CtxKey_msgId(固定为：_tick_)
	OnTick(ctx context.Context, gc *GNetClient[ClientInfo])
}

// GNetEventHandler GNetEvent的内置实现
// 如果不想实现GNetEvent的所有接口，可以继承它实现部分方法
type GNetEventHandler[ClientInfo any] struct {
}

func (*GNetEventHandler[ClientInfo]) OnMsgReg(md *msger.MsgDispatch) {
}
func (*GNetEventHandler[ClientInfo]) OnConnected(ctx context.Context, gc *GNetClient[ClientInfo]) {
}
func (*GNetEventHandler[ClientInfo]) OnDisConnect(ctx context.Context, gc *GNetClient[ClientInfo]) {
}
func (*GNetEventHandler[ClientInfo]) DecodeMsg(ctx context.Context, data []byte, gc *GNetClient[ClientInfo]) (msger.RecvMsger, int, error) {
	return nil, len(data), errors.New("DecodeMsg not Implementation")
}
func (*GNetEventHandler[ClientInfo]) OnMsg(ctx context.Context, mr msger.RecvMsger, gc *GNetClient[ClientInfo]) {
	utils.LogCtx(log.Warn(), ctx).Interface("msger", mr).Msgf("Msg Not Handle %s", gc.ConnName())
}
func (*GNetEventHandler[ClientInfo]) OnTick(ctx context.Context, gc *GNetClient[ClientInfo]) {
}

// Hook
type GNetHook[ClientInfo any] interface {
	// 收到连接
	OnConnected(gc *GNetClient[ClientInfo])
	// ws连接握手
	OnWSHandShake(gc *GNetClient[ClientInfo])
	// 用户掉线，removeClient表示是否引起RemoveClient，但不会调用OnRemoveClient
	OnDisConnect(gc *GNetClient[ClientInfo], removeClient bool, closeReason error)

	// 添加Client
	OnAddClient(gc *GNetClient[ClientInfo])
	// 添加Client
	OnRemoveClient(gc *GNetClient[ClientInfo])

	// 发送数据 所有的发送
	OnSendData(gc *GNetClient[ClientInfo], len int)
	// 接受数据 所有的接受
	OnRecvData(gc *GNetClient[ClientInfo], len int)

	// Send后调用
	OnSend(gc *GNetClient[ClientInfo], len int)
	// SendMsg后调用
	OnSendMsg(gc *GNetClient[ClientInfo], mr msger.Msger, len int)
	// SendText后调用
	OnSendText(gc *GNetClient[ClientInfo], len int)
	// 接受消息数据，消息解码后调用
	OnRecvMsg(gc *GNetClient[ClientInfo], mr msger.RecvMsger, len int)

	// 定时调用
	OnTick()
}
