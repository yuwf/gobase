package tcpserver

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"

	"github.com/yuwf/gobase/utils"
)

type TCPEvent[ClientInfo any] interface {
	// 生成Context, 目前OnMsg、OnTick参数使用
	Context(parent context.Context) context.Context

	// 收到连接
	OnConnected(ctx context.Context, tc *TCPClient[ClientInfo])

	// 用户掉线
	OnDisConnect(ctx context.Context, tc *TCPClient[ClientInfo])

	// DecodeMsg 解码消息实现
	// 返回值为 msg,len,err
	// msg     解码出的消息体
	// len     解码消息的数据长度，内部根据len来删除已解码的数据
	// err     解码错误，若发生error，服务器将重连
	DecodeMsg(ctx context.Context, data []byte, tc *TCPClient[ClientInfo]) (interface{}, int, error)

	// CheckRPCResp 判断是否RPC返回消息，如果使用SendRPCMsg需要实现此函数
	// 返回值为 rpcid
	// rpcid   对应请求SendRPC的id， 返回nil表示非rpc调用
	CheckRPCResp(msg interface{}) interface{}

	// OnRecv 收到消息，解码成功后调用 异步顺序调用，rpc不会调用此函数
	OnMsg(ctx context.Context, msg interface{}, tc *TCPClient[ClientInfo])

	// OnTick 每秒调用一次 异步调用
	OnTick(ctx context.Context, tc *TCPClient[ClientInfo])
}

// TCPEventHandler TCPEvent的内置实现
// 如果不想实现TCPEvent的所有接口，可以继承它实现部分方法
type TCPEventHandler[ClientInfo any] struct {
}

func (h *TCPEventHandler[ClientInfo]) Context(parent context.Context) context.Context {
	return context.WithValue(parent, utils.CtxKey_traceId, utils.GenTraceID())
}
func (*TCPEventHandler[ClientInfo]) OnConnected(ctx context.Context, tc *TCPClient[ClientInfo]) {
}
func (*TCPEventHandler[ClientInfo]) OnDisConnect(ctx context.Context, tc *TCPClient[ClientInfo]) {
}
func (*TCPEventHandler[ClientInfo]) DecodeMsg(ctx context.Context, data []byte, tc *TCPClient[ClientInfo]) (interface{}, int, error) {
	return nil, len(data), errors.New("DecodeMsg not Implementation")
}
func (*TCPEventHandler[ClientInfo]) CheckRPCResp(msg interface{}) interface{} {
	return nil
}
func (*TCPEventHandler[ClientInfo]) OnMsg(ctx context.Context, msg interface{}, tc *TCPClient[ClientInfo]) {
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

	// 发送数据
	OnSend(tc *TCPClient[ClientInfo], len int)
	// 接受数据
	OnRecv(tc *TCPClient[ClientInfo], len int)
}
