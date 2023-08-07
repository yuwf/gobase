package tcpserver

// https://github.com/yuwf

import (
	"context"
	"errors"

	"github.com/rs/zerolog"
)

type TCPEvent[ClientInfo any] interface {
	// 收到连接
	OnConnected(ctx context.Context, tc *TCPClient[ClientInfo])

	// 用户掉线
	OnDisConnect(ctx context.Context, tc *TCPClient[ClientInfo])

	// Encode 编码实现，一般情况都是已经编码完的
	// msgLog 外层不写日志，内部就也不输出日志
	// 返回值为 data,err
	// data    编码出的buf内容
	// err     发生error，消息发送失败
	Encode(data []byte, tc *TCPClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error)

	// Encode 编码实现
	// msgLog 外层不写日志，内部就也不输出日志
	// 返回值为 data,err
	// data    编码出的buf内容
	// err     发生error，消息发送失败
	EncodeMsg(msg interface{}, tc *TCPClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error)

	// EncodeText 编码实现
	// msgLog 外层不写日志，内部就也不输出日志
	// 返回值为 data,err
	// data    编码出的buf内容
	// err     发生error，消息发送失败
	EncodeText(data []byte, tc *TCPClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error)

	// Decode 解码实现
	// 返回值为 msg,len,err
	// msg     解码出的消息体
	// len     解码消息的数据长度，内部根据len来删除已解码的数据
	// err     解码错误，若发生error，服务器将重连
	DecodeMsg(ctx context.Context, data []byte, tc *TCPClient[ClientInfo]) (interface{}, int, error)

	// CheckRPC 判断是否RPC返回消息，如果使用SendRPCMsg需要实现此函数
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

func (*TCPEventHandler[ClientInfo]) OnConnected(ctx context.Context, tc *TCPClient[ClientInfo]) {
}
func (*TCPEventHandler[ClientInfo]) OnDisConnect(ctx context.Context, tc *TCPClient[ClientInfo]) {
}
func (*TCPEventHandler[ClientInfo]) Encode(data []byte, tc *TCPClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error) {
	return nil, errors.New("Encode not Implementation")
}
func (*TCPEventHandler[ClientInfo]) EncodeMsg(msg interface{}, tc *TCPClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error) {
	return nil, errors.New("EncodeMsg not Implementation")
}
func (*TCPEventHandler[ClientInfo]) EncodeText(data []byte, gc *TCPClient[ClientInfo], msgLog *zerolog.Event) ([]byte, error) {
	return nil, errors.New("EncodeText not Implementation")
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
