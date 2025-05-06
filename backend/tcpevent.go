package backend

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"

	"github.com/yuwf/gobase/consul"
	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/nacos"
	"github.com/yuwf/gobase/utils"
)

type TcpEvent[ServiceInfo any] interface {
	// consul服务器配置过滤器，返回符合条件的服务器
	ConsulFilter(confs []*consul.RegistryInfo) []*ServiceConfig

	// consul服务器配置过滤器，返回符合条件的服务器
	NacosFilter(confs []*nacos.RegistryInfo) []*ServiceConfig

	// goredis服务器配置过滤器，返回符合条件的服务器
	GoRedisFilter(confs []*goredis.RegistryInfo) []*ServiceConfig

	// 网络连接成功
	// 异步顺序调用
	OnConnected(ctx context.Context, ts *TcpService[ServiceInfo])

	// 网络失去连接
	// 异步顺序调用
	OnDisConnect(ctx context.Context, ts *TcpService[ServiceInfo])

	// DecodeMsg 解码实现
	// 网络协程调用
	// ctx       包括 CtxKey_scheme
	// 返回值为   msg,len,err
	// msg       解码出的消息体
	// len       解码消息的数据长度，内部根据len来删除已解码的数据
	// rpcid     对应请求SendRPC的id， 返回nil表示非rpc调用
	// err       解码错误，若发生error，服务器将重连
	DecodeMsg(ctx context.Context, data []byte, ts *TcpService[ServiceInfo]) (utils.RecvMsger, int, interface{}, error)

	// OnRecv 收到消息，解码成功后调用
	// 异步顺序调用 or 异步调用
	// ctx    包括 CtxKey_scheme,CtxKey_traceId,CtxKey_msgId
	OnMsg(ctx context.Context, msg utils.RecvMsger, ts *TcpService[ServiceInfo])

	// OnTick 每秒tick下
	// 异步顺序调用
	// ctx    包括 CtxKey_scheme,CtxKey_traceId,CtxKey_msgId(固定为：_tick_)
	OnTick(ctx context.Context, ts *TcpService[ServiceInfo])
}

// TcpEventHandler TcpEvent的内置实现
// 如果不想实现TcpEvent的所有接口，可以继承它实现部分方法
type TcpEventHandler[ServiceInfo any] struct {
}

func (*TcpEventHandler[ServiceInfo]) ConsulFilter(confs []*consul.RegistryInfo) []*ServiceConfig {
	return []*ServiceConfig{}
}
func (*TcpEventHandler[ServiceInfo]) NacosFilter(confs []*nacos.RegistryInfo) []*ServiceConfig {
	return []*ServiceConfig{}
}
func (*TcpEventHandler[ServiceInfo]) GoRedisFilter(confs []*goredis.RegistryInfo) []*ServiceConfig {
	return []*ServiceConfig{}
}
func (*TcpEventHandler[ServiceInfo]) OnConnected(ctx context.Context, ts *TcpService[ServiceInfo]) {
	ts.OnLogin() // 直接标记登录成功
}
func (*TcpEventHandler[ServiceInfo]) OnDisConnect(ctx context.Context, ts *TcpService[ServiceInfo]) {
}
func (*TcpEventHandler[ServiceInfo]) DecodeMsg(ctx context.Context, data []byte, ts *TcpService[ServiceInfo]) (utils.RecvMsger, int, interface{}, error) {
	return nil, len(data), nil, errors.New("DecodeMsg not Implementation")
}
func (*TcpEventHandler[ServiceInfo]) OnMsg(ctx context.Context, msg utils.RecvMsger, ts *TcpService[ServiceInfo]) {
}
func (*TcpEventHandler[ServiceInfo]) OnTick(ctx context.Context, ts *TcpService[ServiceInfo]) {
}

// Hook
type TCPHook[ServiceInfo any] interface {
	// 添加服务器
	OnAdd(ts *TcpService[ServiceInfo])
	// 移除一个服务器，彻底移除
	OnRemove(ts *TcpService[ServiceInfo])

	// 连接成功
	OnConnected(ts *TcpService[ServiceInfo])
	// 连接掉线
	OnDisConnect(ts *TcpService[ServiceInfo])

	// 发送数据 所有的发送
	OnSend(tc *TcpService[ServiceInfo], len int)
	// 接受数据 所有的接受
	OnRecv(tc *TcpService[ServiceInfo], len int)

	// 发送消息数据
	OnSendMsg(tc *TcpService[ServiceInfo], msgId string, len int)
	// 接受消息数据
	OnRecvMsg(tc *TcpService[ServiceInfo], msgId string, len int)
}
