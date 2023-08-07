package gnetserver

// https://github.com/yuwf

import (
	"context"
	"errors"

	"github.com/rs/zerolog"
)

type GNetEvent[ClientInfo any] interface {
	// 收到连接
	OnConnected(ctx context.Context, gc *GNetClient[ClientInfo])

	// 用户掉线
	OnDisConnect(ctx context.Context, gc *GNetClient[ClientInfo])

	// Encode 编码实现，一般情况都是已经编码完的
	// msgLog 外层不写日志，内部就也不输出日志
	// 返回值为 data,err
	// data    编码出的buf内容
	// err     发生error，消息发送失败
	Encode(data []byte, gc *GNetClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error)

	// Encode 编码实现
	// msgLog 外层不写日志，内部就也不输出日志
	// 返回值为 data,err
	// data    编码出的buf内容
	// err     发生error，消息发送失败
	EncodeMsg(msg interface{}, gc *GNetClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error)

	// EncodeText 编码实现
	// msgLog 外层不写日志，内部就也不输出日志
	// 返回值为 data,err
	// data    编码出的buf内容
	// err     发生error，消息发送失败
	EncodeText(data []byte, gc *GNetClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error)

	// Decode 解码实现
	// 返回值为 msg,len,err
	// msg     解码出的消息体
	// len     解码消息的数据长度，内部根据len来删除已解码的数据
	// err     解码错误，若发生error，服务器将重连
	DecodeMsg(ctx context.Context, data []byte, gc *GNetClient[ClientInfo]) (interface{}, int, error)

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
func (*GNetEventHandler[ClientInfo]) Encode(data []byte, gc *GNetClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error) {
	return nil, errors.New("Encode not Implementation")
}
func (*GNetEventHandler[ClientInfo]) EncodeMsg(msg interface{}, gc *GNetClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error) {
	return nil, errors.New("EncodeMsg not Implementation")
}
func (*GNetEventHandler[ClientInfo]) EncodeText(data []byte, gc *GNetClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error) {
	return nil, errors.New("EncodeText not Implementation")
}
func (*GNetEventHandler[ClientInfo]) DecodeMsg(ctx context.Context, data []byte, gc *GNetClient[ClientInfo]) (interface{}, int, error) {
	return nil, len(data), errors.New("DecodeMsg not Implementation")
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
