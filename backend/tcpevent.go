package backend

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"time"

	"github.com/yuwf/gobase/consul"
	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/msger"
	"github.com/yuwf/gobase/nacos"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

type TcpEvent[ServiceInfo any] interface {
	// 消息注册
	OnMsgReg(md *msger.MsgDispatch)

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
	// err       解码错误，若发生error，服务器将重连
	DecodeMsg(ctx context.Context, data []byte, ts *TcpService[ServiceInfo]) (msger.RecvMsger, int, error)

	// OnRecv 收到消息，解码成功后调用
	// 异步顺序调用 or 异步调用
	// ctx    包括 CtxKey_scheme,CtxKey_traceId,CtxKey_msgId
	OnMsg(ctx context.Context, mr msger.RecvMsger, ts *TcpService[ServiceInfo])

	// OnTick 根据配置的心跳间隔调用 TcpParamConfig.TickInterval
	// 异步顺序调用
	// ctx    包括 CtxKey_scheme,CtxKey_traceId,CtxKey_msgId(固定为：_tick_)
	OnTick(ctx context.Context, ts *TcpService[ServiceInfo])
}

// TcpEventHandler TcpEvent的内置实现
// 如果不想实现TcpEvent的所有接口，可以继承它实现部分方法
type TcpEventHandler[ServiceInfo any] struct {
}

func (*TcpEventHandler[ServiceInfo]) OnMsgReg(md *msger.MsgDispatch) {
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
	ts.OnConnLogined(ctx) // 直接标记登录成功
}
func (*TcpEventHandler[ServiceInfo]) OnDisConnect(ctx context.Context, ts *TcpService[ServiceInfo]) {
}
func (*TcpEventHandler[ServiceInfo]) DecodeMsg(ctx context.Context, data []byte, ts *TcpService[ServiceInfo]) (msger.RecvMsger, int, error) {
	return nil, len(data), errors.New("DecodeMsg not Implementation")
}
func (*TcpEventHandler[ServiceInfo]) OnMsg(ctx context.Context, mr msger.RecvMsger, ts *TcpService[ServiceInfo]) {
	utils.LogCtx(log.Warn(), ctx).Interface("msger", mr).Msgf("Msg Not Handle %s", ts.ConnName())
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
	OnSendData(ts *TcpService[ServiceInfo], len int)
	// 接受数据 所有的接受
	OnRecvData(ts *TcpService[ServiceInfo], len int)

	// Send后调用
	OnSend(ts *TcpService[ServiceInfo], len int)
	// SendMsg后调用
	OnSendMsg(ts *TcpService[ServiceInfo], mr msger.Msger, len int)
	// SendRPCMsg后调用， 收到的Resp在OnRecvMsg中调用，会在此函数前调用
	OnSendRPCMsg(ts *TcpService[ServiceInfo], rpcId interface{}, mr msger.Msger, elapsed time.Duration, len int)
	// 接受消息数据，消息解码后调用
	OnRecvMsg(ts *TcpService[ServiceInfo], mr msger.RecvMsger, len int)
}
