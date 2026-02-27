package tcpserver

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"time"

	"github.com/yuwf/gobase/msger"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

type TCPEvent[ClientInfo any] interface {
	// 消息注册
	OnMsgReg(md *msger.MsgDispatch)

	// 收到连接
	// 异步顺序调用
	OnConnected(ctx context.Context, tc *TCPClient[ClientInfo])

	// 用户掉线
	// 异步顺序调用
	OnDisConnect(ctx context.Context, tc *TCPClient[ClientInfo])

	// DecodeMsg 解码消息实现
	// 网络协程调用
	// ctx       包括 [CtxKey_WS,CtxKey_Text]
	// 返回值为   msg,len,err
	// msg       解码出的消息体
	// len       解码消息的数据长度，内部根据len来删除已解码的数据
	// err       解码错误，若发生error，服务器将重连
	DecodeMsg(ctx context.Context, data []byte, tc *TCPClient[ClientInfo]) (msger.RecvMsger, int, error)

	// OnRecv 收到消息，解码成功后调用，rpc不会调用此函数
	// 异步顺序调用 or 异步调用
	// ctx    包括 [CtxKey_WS,CtxKey_Text],CtxKey_traceId,CtxKey_msgId
	OnMsg(ctx context.Context, mr msger.RecvMsger, tc *TCPClient[ClientInfo])

	// OnTick 每秒调用一次
	// 异步顺序调用
	// ctx    包括 [CtxKey_WS],CtxKey_traceId,CtxKey_msgId(固定为：_tick_)
	OnTick(ctx context.Context, tc *TCPClient[ClientInfo])
}

// TCPEventHandler TCPEvent的内置实现
// 如果不想实现TCPEvent的所有接口，可以继承它实现部分方法
type TCPEventHandler[ClientInfo any] struct {
}

func (*TCPEventHandler[ClientInfo]) OnMsgReg(md *msger.MsgDispatch) {
}
func (*TCPEventHandler[ClientInfo]) OnConnected(ctx context.Context, tc *TCPClient[ClientInfo]) {
}
func (*TCPEventHandler[ClientInfo]) OnDisConnect(ctx context.Context, tc *TCPClient[ClientInfo]) {
}
func (*TCPEventHandler[ClientInfo]) DecodeMsg(ctx context.Context, data []byte, tc *TCPClient[ClientInfo]) (msger.RecvMsger, int, error) {
	return nil, len(data), errors.New("DecodeMsg not Implementation")
}
func (*TCPEventHandler[ClientInfo]) OnMsg(ctx context.Context, mr msger.RecvMsger, tc *TCPClient[ClientInfo]) {
	utils.LogCtx(log.Warn(), ctx).Interface("msger", mr).Msgf("Msg Not Handle %s", tc.ConnName())
}
func (*TCPEventHandler[ClientInfo]) OnTick(ctx context.Context, tc *TCPClient[ClientInfo]) {
}

// Hook
type TCPHook[ClientInfo any] interface {
	// 收到连接
	OnConnected(tc *TCPClient[ClientInfo])
	// ws连接握手
	OnWSHandShake(gc *TCPClient[ClientInfo])
	// 用户掉线，removeClient表示是否引起RemoveClient，但不会调用OnRemoveClient
	OnDisConnect(tc *TCPClient[ClientInfo], removeClient bool, closeReason error)

	// 添加Client
	OnAddClient(tc *TCPClient[ClientInfo])
	// 添加Client
	OnRemoveClient(tc *TCPClient[ClientInfo])

	// 发送数据 所有的发送
	OnSendData(tc *TCPClient[ClientInfo], len int)
	// 接受数据 所有的接受
	OnRecvData(tc *TCPClient[ClientInfo], len int)

	// Send后调用
	OnSend(tc *TCPClient[ClientInfo], len int)
	// SendMsg后调用
	OnSendMsg(tc *TCPClient[ClientInfo], mr msger.Msger, len int)
	// SendText后调用
	OnSendText(tc *TCPClient[ClientInfo], len int)
	// SendRPCMsg后调用， 收到的Resp在OnRecvMsg中调用，会在此函数前调用
	OnSendRPCMsg(tc *TCPClient[ClientInfo], rpcId interface{}, mr msger.Msger, elapsed time.Duration, len int)
	// 接受消息数据，消息解码后调用
	OnRecvMsg(tc *TCPClient[ClientInfo], mr msger.RecvMsger, len int)

	// 定时调用
	OnTick()
}
