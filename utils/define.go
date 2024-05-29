package utils

// https://github.com/yuwf/gobase

import (
	"sync/atomic"
)

type CtxKey string

const CtxKey_traceId = CtxKey("_traceId_") // context产生时，设置的唯一ID，用来链路追踪，int64
const CtxKey_msgId = CtxKey("_msgId_")     // 接受或者发送消息时设置的msgId，用来链路追踪，string
const CtxKey_nolog = CtxKey("_nolog_")     // 不打印日志，错误日志还会打印 值：不受限制 一般写1

var genTraceId int64 // 内部使用的全局traceid
func GenTraceID() int64 {
	return atomic.AddInt64(&genTraceId, 1)
}
