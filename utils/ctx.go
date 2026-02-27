package utils

// https://github.com/yuwf/gobase

import (
	"context"
	"sync/atomic"
)

type CtxKey string

var GetTraceID func(ctx context.Context) int64 // 外部重写获取追踪id的函数
var genTraceId int64                           // 内部使用的全局traceid

// 链路追踪使用，一般为底层产生
const CtxKey_traceId = CtxKey("_traceId_")     // 通过CtxSetTraceID接口设置 context产生时，设置的唯一ID，用来链路追踪，int64
const CtxKey_traceName = CtxKey("_traceName_") // 接受消息时设置的msgId或者msgName(不同的模块设置的不一样)，用来统计，string

const CtxKey_nolog = CtxKey("_nolog_")     // 通过CtxSetNolog接口设置   不打印日志，错误日志还会打印 值：不受限制 一般写1
const CtxKey_callers = CtxKey("_callers_") // 通过CtxAddCaller接口添加  值：*_ctx_callers_对象
const CtxKey_log = CtxKey("_log_")         // 通过CtxAddLog接口添加     值: *_ctx_log_对象

// 设置追踪ID
func CtxSetTrace(parent context.Context, traceId int64, traceName string) context.Context {
	if parent == nil {
		parent = context.TODO()
	} else {
		if parent.Value(CtxKey_traceId) != nil {
			return parent
		}
	}

	if traceId == 0 {
		if GetTraceID != nil {
			traceId = GetTraceID(context.Background())
		} else {
			traceId = atomic.AddInt64(&genTraceId, 1)
		}
	}
	ctx := context.WithValue(parent, CtxKey_traceId, traceId)
	ctx = context.WithValue(ctx, CtxKey_traceName, traceName)
	return ctx
}

func CtxSetNolog(parent context.Context) context.Context {
	if parent == nil {
		parent = context.TODO()
	} else {
		if parent.Value(CtxKey_nolog) != nil {
			return parent
		}
	}
	return context.WithValue(parent, CtxKey_nolog, 1)
}

func CtxHasNolog(ctx context.Context) bool {
	if ctx != nil {
		return ctx.Value(CtxKey_nolog) != nil
	}
	return false
}

type _ctx_callers_ struct {
	callers []*CallerDesc // 初始化后，只读，不可调用append
}

// 添加堆栈，耗时操作，高频率的地方不建议使用
func CtxAddCaller(parent context.Context, skip int) context.Context {
	if parent == nil {
		parent = context.TODO()
	} else {
		if old, ok := parent.Value(CtxKey_callers).(*_ctx_callers_); ok {
			desc := GetCallerDesc(skip + 1)

			if len(old.callers) > 0 && old.callers[0].funname == desc.funname {
				return parent // 第一个如果是相同的函数名，就不添加了
			}

			// 新增就重新构造一个_ctx_callers_
			return context.WithValue(parent, CtxKey_callers, &_ctx_callers_{callers: append([]*CallerDesc{desc}, old.callers...)})
		}
	}
	return context.WithValue(parent, CtxKey_callers, &_ctx_callers_{callers: []*CallerDesc{GetCallerDesc(skip + 1)}})
}

type _ctx_log_ struct {
	Key   string
	Value interface{}
}

type _ctx_logs_ struct {
	logs []*_ctx_log_ // 初始化后，只读，不可调用append
}

// 添加日志
func CtxAddLog(parent context.Context, key string, value interface{}) context.Context {
	if parent == nil {
		parent = context.TODO()
	} else {
		if old, ok := parent.Value(CtxKey_log).(*_ctx_logs_); ok {
			// 已经存在了，且值相同，就用原先的
			found := -1
			for i, item := range old.logs {
				if item.Key == key {
					if item.Value == value {
						return parent
					}
					found = i
					break
				}
			}

			var logs []*_ctx_log_
			if found != -1 {
				logs = append([]*_ctx_log_{}, old.logs...)
				logs[found] = &_ctx_log_{Key: key, Value: value} // 替换时，要确保是新的对象，否则会导致日志被修改
			} else {
				logs = append([]*_ctx_log_{{Key: key, Value: value}}, old.logs...)
			}
			return context.WithValue(parent, CtxKey_log, &_ctx_logs_{logs: logs})
		}
	}
	return context.WithValue(parent, CtxKey_log, &_ctx_logs_{logs: []*_ctx_log_{{Key: key, Value: value}}})
}

func CtxRemoveLog(parent context.Context, key string) context.Context {
	if old, ok := parent.Value(CtxKey_log).(*_ctx_logs_); ok {
		// 先判断是否存在
		found := -1
		for i, item := range old.logs {
			if item.Key == key {
				found = i
				break
			}
		}
		if found == -1 {
			return parent
		}

		logs := make([]*_ctx_log_, 0, len(old.logs)-1) // 预分配容量
		logs = append(logs, old.logs[:found]...)
		logs = append(logs, old.logs[found+1:]...)
		return context.WithValue(parent, CtxKey_log, &_ctx_logs_{logs: logs})
	} else {
		return parent
	}
}
