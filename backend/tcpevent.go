package backend

// https://github.com/yuwf

import (
	"context"
	"errors"

	"github.com/rs/zerolog"
	"github.com/yuwf/gobase/consul"
	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/redis"
)

type TcpEvent[T any] interface {
	// consul服务器配置过滤器，返回符合条件的服务器
	ConsulFilter(confs []*consul.RegistryInfo) []*ServiceConfig

	// redis服务器配置过滤器，返回符合条件的服务器
	RedisFilter(confs []*redis.RegistryInfo) []*ServiceConfig

	// goredis服务器配置过滤器，返回符合条件的服务器
	GoRedisFilter(confs []*goredis.RegistryInfo) []*ServiceConfig

	// 网络连接成功
	OnConnected(ctx context.Context, ts *TcpService[T])

	// 网络失去连接
	OnDisConnect(ctx context.Context, ts *TcpService[T])

	// Encode 编码实现，一般情况都是已经编码完的
	// msgLog 外层不写日志，内部就也不输出日志
	// 返回值为 data,err
	// data    编码出的buf内容
	// err     发生error，消息发送失败
	Encode(data []byte, ts *TcpService[T], msgLog *zerolog.Event) ([]byte, error)

	// EncodeMsg 编码实现
	// msgLog 外层不写日志，内部就也不输出日志
	// 返回值为 data,err
	// data    编码出的buf内容
	// err     发生error，消息发送失败
	EncodeMsg(msg interface{}, ts *TcpService[T], msgLog *zerolog.Event) ([]byte, error)

	// DecodeMsg 解码实现
	// 返回值为 msg,len,err
	// msg     解码出的消息体
	// len     解码消息的数据长度，内部根据len来删除已解码的数据
	// err     解码错误，若发生error，服务器将重连
	DecodeMsg(ctx context.Context, data []byte, ts *TcpService[T]) (interface{}, int, error)

	// CheckRPC 判断是否RPC返回消息，如果使用SendRPCMsg需要实现此函数
	// 返回值为 rpcid
	// rpcid   对应请求SendRPC的id， 返回nil表示非rpc调用
	CheckRPCResp(msg interface{}) interface{}

	// OnRecv 收到消息，解码成功后调用 异步调用
	OnMsg(ctx context.Context, msg interface{}, ts *TcpService[T])

	// 每秒tick下 异步调用
	OnTick(ctx context.Context, ts *TcpService[T])
}

// TcpEventHandler TcpEvent的内置实现
// 如果不想实现TcpEvent的所有接口，可以继承它实现部分方法
type TcpEventHandler[T any] struct {
}

func (*TcpEventHandler[T]) ConsulFilter(confs []*consul.RegistryInfo) []*ServiceConfig {
	return []*ServiceConfig{}
}
func (*TcpEventHandler[T]) RedisFilter(confs []*redis.RegistryInfo) []*ServiceConfig {
	return []*ServiceConfig{}
}
func (*TcpEventHandler[T]) GoRedisFilter(confs []*goredis.RegistryInfo) []*ServiceConfig {
	return []*ServiceConfig{}
}
func (*TcpEventHandler[T]) OnConnected(ctx context.Context, ts *TcpService[T]) {
}
func (*TcpEventHandler[T]) OnDisConnect(ctx context.Context, ts *TcpService[T]) {
}
func (*TcpEventHandler[T]) Encode(data []byte, ts *TcpService[T], msgLog *zerolog.Event) ([]byte, error) {
	return nil, errors.New("Encode not Implementation")
}
func (*TcpEventHandler[T]) EncodeMsg(msg interface{}, ts *TcpService[T], msgLog *zerolog.Event) ([]byte, error) {
	return nil, errors.New("EncodeMsg not Implementation")
}
func (*TcpEventHandler[T]) DecodeMsg(ctx context.Context, data []byte, ts *TcpService[T]) (interface{}, int, error) {
	return nil, len(data), errors.New("DecodeMsg not Implementation")
}
func (*TcpEventHandler[T]) CheckRPCResp(msg interface{}) interface{} {
	return nil
}
func (*TcpEventHandler[T]) OnMsg(ctx context.Context, msg interface{}, ts *TcpService[T]) {
}
func (*TcpEventHandler[T]) OnTick(ctx context.Context, ts *TcpService[T]) {
}

// Hook
type TCPHook[T any] interface {
	// 添加服务器
	OnAdd(ts *TcpService[T])
	// 移除一个服务器，彻底移除
	OnRemove(ts *TcpService[T])

	// 连接成功
	OnConnected(ts *TcpService[T])
	// 连接掉线
	OnDisConnect(ts *TcpService[T])

	// 发送数据
	OnSend(ts *TcpService[T], len int)
	// 接受数据
	OnRecv(ts *TcpService[T], len int)
}
