package msger

// https://github.com/yuwf/gobase

import (
	"context"
	"time"
)

type ConnName interface {
	// 连接名
	ConnName() string
}

// 连接终端 接口抽象
type ConnTermianl interface {
	// 连接名
	ConnName() string

	// ClientInfo
	InfoI() interface{}

	// 消息堆积数量
	RecvSeqCount() int

	// 发送消息
	Send(ctx context.Context, data []byte) error
	SendMsg(ctx context.Context, mr Msger) error
	SendRPCMsg(ctx context.Context, rpcId interface{}, mr Msger, timeout time.Duration, respBody interface{}) (RecvMsger, error)
	SendAsyncRPCMsg(ctx context.Context, rpcId interface{}, mr Msger, timeout time.Duration, callback interface{}) error // callback参考AsyncRPCCallback的说明
}

// 监听服务终端
type ServerTermianl interface {
	// 连接数量 握手数量
	ConnCount() (int, int)

	// 加入的Client数量
	ClientCount() int

	// 消息堆积数量, connname:int
	RecvSeqCount() map[string]int
}
