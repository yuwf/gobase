package utils

// https://github.com/yuwf/gobase

import (
	"sync/atomic"
)

type CtxKey string

const CtxKey_traceId = CtxKey("traceId") // context产生时，设置的唯一ID，用来链路追踪，int64
const CtxKey_nolog = CtxKey("nolog")     // 不打印日志，错误日志还会打印 值：不受限制 一般写1
const CtxKey_msgId = CtxKey("msgId")     // 接受或者发送消息时设置的msgId，用来链路追踪，string

var genTraceId int64 // 内部使用的全局traceid
func GenTraceID() int64 {
	return atomic.AddInt64(&genTraceId, 1)
}
