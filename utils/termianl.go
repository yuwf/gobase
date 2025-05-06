package utils

// https://github.com/yuwf/gobase

import (
	"context"
	"time"
)

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
	SendMsg(ctx context.Context, msg SendMsger) error
	SendRPCMsg(ctx context.Context, rpcId interface{}, msg SendMsger, timeout time.Duration) (interface{}, error)
}

// 监听服务终端
type ServerTermianl interface {
	// 连接数量 握手数量
	ConnCount() (int, int)

	// 加入的Client数量
	ClientCount() int

	// 消息堆积数量
	RecvSeqCount() int
}
