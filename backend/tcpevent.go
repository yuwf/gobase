package backend

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"

	"github.com/yuwf/gobase/consul"
	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/redis"
	"github.com/yuwf/gobase/utils"
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

	// DecodeMsg 解码实现
	// ctx       包括 CtxKey_scheme
	// 返回值为   msg,len,err
	// msg       解码出的消息体
	// len       解码消息的数据长度，内部根据len来删除已解码的数据
	// err       解码错误，若发生error，服务器将重连
	DecodeMsg(ctx context.Context, data []byte, ts *TcpService[T]) (utils.RecvMsger, int, error)

	// CheckRPCResp 判断是否RPC返回消息，如果使用SendRPCMsg需要实现此函数
	// ctx          包括 CtxKey_scheme
	// 返回值为      rpcid
	// rpcid        对应请求SendRPC的id， 返回nil表示非rpc调用
	CheckRPCResp(msg utils.RecvMsger) interface{}

	// OnRecv 收到消息，解码成功后调用 异步调用
	// ctx    包括 CtxKey_scheme,CtxKey_traceId,CtxKey_msgId
	OnMsg(ctx context.Context, msg utils.RecvMsger, ts *TcpService[T])

	// OnTick 每秒tick下 异步调用
	// ctx    包括 CtxKey_scheme,CtxKey_traceId,CtxKey_msgId(固定为：_tick_)
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
func (*TcpEventHandler[T]) DecodeMsg(ctx context.Context, data []byte, ts *TcpService[T]) (utils.RecvMsger, int, error) {
	return nil, len(data), errors.New("DecodeMsg not Implementation")
}
func (*TcpEventHandler[T]) CheckRPCResp(msg utils.RecvMsger) interface{} {
	return nil
}
func (*TcpEventHandler[T]) OnMsg(ctx context.Context, msg utils.RecvMsger, ts *TcpService[T]) {
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

	// 发送数据 所有的发送
	OnSend(tc *TcpService[T], len int)
	// 接受数据 所有的接受
	OnRecv(tc *TcpService[T], len int)

	// 发送消息数据
	OnSendMsg(tc *TcpService[T], msgId string, len int)
	// 接受消息数据
	OnRecvMsg(tc *TcpService[T], msgId string, len int)
}
