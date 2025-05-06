package utils

// https://github.com/yuwf/gobase

import (
	"context"
	"sync/atomic"
)

type CtxKey string

const CtxKey_traceId = CtxKey("_traceId_") // context产生时，设置的唯一ID，用来链路追踪，int64
const CtxKey_msgId = CtxKey("_msgId_")     // 接受或者发送消息时设置的msgId，用来链路追踪，string
const CtxKey_nolog = CtxKey("_nolog_")     // 不打印日志，错误日志还会打印 值：不受限制 一般写1
const CtxKey_callers = CtxKey("_callers_") // 值：[]*CallerDescs对象

func CtxNolog(parent context.Context) context.Context {
	if parent == nil {
		parent = context.TODO()
	} else {
		if parent.Value(CtxKey_nolog) != nil {
			return parent
		}
	}
	return context.WithValue(parent, CtxKey_nolog, 1)
}

func CtxCaller(parent context.Context, skip int) context.Context {
	if parent == nil {
		parent = context.TODO()
	} else {
		if callers, ok := parent.Value(CtxKey_callers).([]*CallerDesc); ok {
			desc := GetCallerDesc(skip + 1)
			if len(callers) > 0 && callers[len(callers)-1].funname == desc.funname {
				return parent
			}
			return context.WithValue(parent, CtxKey_callers, append(callers, desc))
		}
	}
	return context.WithValue(parent, CtxKey_callers, []*CallerDesc{GetCallerDesc(skip + 1)})
}

func CtxHasNolog(ctx context.Context) bool {
	if ctx != nil {
		return ctx.Value(CtxKey_nolog) != nil
	}
	return false
}

var genTraceId int64 // 内部使用的全局traceid
func GenTraceID() int64 {
	return atomic.AddInt64(&genTraceId, 1)
}
