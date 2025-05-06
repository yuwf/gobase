package tcpserver

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"

	"github.com/yuwf/gobase/utils"
)

type TCPEvent[ClientInfo any] interface {
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
	// rpcid     对应请求SendRPC的id， 返回nil表示非rpc调用
	// err       解码错误，若发生error，服务器将重连
	DecodeMsg(ctx context.Context, data []byte, tc *TCPClient[ClientInfo]) (utils.RecvMsger, int, interface{}, error)

	// OnRecv 收到消息，解码成功后调用，rpc不会调用此函数
	// 异步顺序调用 or 异步调用
	// ctx    包括 [CtxKey_WS,CtxKey_Text],CtxKey_traceId,CtxKey_msgId
	OnMsg(ctx context.Context, msg utils.RecvMsger, tc *TCPClient[ClientInfo])

	// OnTick 每秒调用一次
	// 异步顺序调用
	// ctx    包括 [CtxKey_WS],CtxKey_traceId,CtxKey_msgId(固定为：_tick_)
	OnTick(ctx context.Context, tc *TCPClient[ClientInfo])
}

// TCPEventHandler TCPEvent的内置实现
// 如果不想实现TCPEvent的所有接口，可以继承它实现部分方法
type TCPEventHandler[ClientInfo any] struct {
}

func (*TCPEventHandler[ClientInfo]) OnConnected(ctx context.Context, tc *TCPClient[ClientInfo]) {
}
func (*TCPEventHandler[ClientInfo]) OnDisConnect(ctx context.Context, tc *TCPClient[ClientInfo]) {
}
func (*TCPEventHandler[ClientInfo]) DecodeMsg(ctx context.Context, data []byte, tc *TCPClient[ClientInfo]) (interface{}, int, interface{}, error) {
	return nil, len(data), nil, errors.New("DecodeMsg not Implementation")
}
func (*TCPEventHandler[ClientInfo]) OnMsg(ctx context.Context, msg utils.RecvMsger, tc *TCPClient[ClientInfo]) {
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
	OnSend(tc *TCPClient[ClientInfo], len int)
	// 接受数据 所有的接受
	OnRecv(tc *TCPClient[ClientInfo], len int)

	// 发送消息数据
	OnSendMsg(tc *TCPClient[ClientInfo], msgId string, len int)
	// 接受消息数据
	OnRecvMsg(tc *TCPClient[ClientInfo], msgId string, len int)

	// 定时调用
	OnTick()
}
