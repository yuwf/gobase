package gnetserver

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"

	"github.com/yuwf/gobase/utils"
)

type GNetEvent[ClientInfo any] interface {
	// 收到连接
	OnConnected(ctx context.Context, gc *GNetClient[ClientInfo])

	// 用户掉线
	OnDisConnect(ctx context.Context, gc *GNetClient[ClientInfo])

	// DecodeMsg 解码消息实现
	// 返回值为 msg,len,err
	// msg     解码出的消息体
	// len     解码消息的数据长度，内部根据len来删除已解码的数据
	// err     解码错误，若发生error，服务器将重连
	DecodeMsg(ctx context.Context, data []byte, gc *GNetClient[ClientInfo]) (interface{}, int, error)

	// Context 生成Context, 目前OnMsg、OnTick参数使用
	// msg为nil时 表示是OnTick调用
	Context(parent context.Context, msg interface{}) context.Context

	// OnRecv 收到消息，解码成功后调用 异步顺序调用
	OnMsg(ctx context.Context, msg interface{}, gc *GNetClient[ClientInfo])

	// tick每秒调用一次 异步调用
	OnTick(ctx context.Context, gc *GNetClient[ClientInfo])
}

// GNetEventHandler GNetEvent的内置实现
// 如果不想实现GNetEvent的所有接口，可以继承它实现部分方法
type GNetEventHandler[ClientInfo any] struct {
}

func (*GNetEventHandler[ClientInfo]) OnConnected(ctx context.Context, gc *GNetClient[ClientInfo]) {
}
func (*GNetEventHandler[ClientInfo]) OnDisConnect(ctx context.Context, gc *GNetClient[ClientInfo]) {
}
func (*GNetEventHandler[ClientInfo]) DecodeMsg(ctx context.Context, data []byte, gc *GNetClient[ClientInfo]) (interface{}, int, error) {
	return nil, len(data), errors.New("DecodeMsg not Implementation")
}
func (h *GNetEventHandler[ClientInfo]) Context(parent context.Context, msg interface{}) context.Context {
	return context.WithValue(parent, utils.CtxKey_traceId, utils.GenTraceID())
}
func (*GNetEventHandler[ClientInfo]) OnMsg(ctx context.Context, msg interface{}, gc *GNetClient[ClientInfo]) {
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

	// 发送数据
	OnSend(gc *GNetClient[ClientInfo], len int)
	// 接受数据
	OnRecv(gc *GNetClient[ClientInfo], len int)
}
