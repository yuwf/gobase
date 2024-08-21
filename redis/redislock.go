package redis

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog/log"
)

var deleteLockKeyScript = NewScript(1, `
	local n = redis.call('GET',KEYS[1])
	if n == ARGV[1] then
		redis.call('DEL',KEYS[1])
		return 1
	end
	return 0
`)

// 只尝试一次加锁，失败直接返回
func (r *Redis) TryLock(ctx context.Context, key string, timeout time.Duration) (func(), error) {
	caller, ok := ctx.Value(utils.CtxKey_caller).(*utils.CallerDesc)
	if !ok {
		caller = utils.GetCallerDesc(1)
		ctx = context.WithValue(ctx, utils.CtxKey_caller, caller)
	}
	uuid := utils.LocalIPString() + "-" + strconv.Itoa(os.Getpid()) + "-" + caller.Pos() + "-" + utils.RandString(8)
	redisCmd := &RedisCommond{
		ctx:     ctx,
		Cmd:     "SET",
		Args:    []interface{}{key, uuid, "PX", timeout.Milliseconds(), "NX"},
		CmdDesc: "TryLock",
	}

	logOut := true
	if ctx != nil {
		nolog := ctx.Value(CtxKey_nolog)
		if nolog != nil {
			logOut = false
		} else {
			ctx = context.WithValue(ctx, CtxKey_nolog, 1) // 命令传递下去不需要日志了
		}
	} else {
		ctx = context.WithValue(context.TODO(), CtxKey_nolog, 1)
	}

	r.docmd(ctx, redisCmd)

	if redisCmd.Err != nil {
		utils.LogCtx(log.Error(), ctx).Err(redisCmd.Err).Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
			Str("cmd", redisCmd.CmdString()).
			Msg("Redis TryLock fail")
		return nil, redisCmd.Err
	}

	var result string
	result, redisCmd.Err = redis.String(redisCmd.Reply, redisCmd.Err)
	if redisCmd.Err != nil || result != "OK" {
		if redisCmd.Err == nil {
			redisCmd.Err = errors.New("not OK")
		}
		if logOut {
			// Debug就行 毕竟是try
			utils.LogCtx(log.Debug(), ctx).Err(redisCmd.Err).Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
				Str("cmd", redisCmd.CmdString()).
				Str("reply", redisCmd.ReplyString()).
				Msg("Redis TryLock fail")
		}
		return nil, redisCmd.Err
	} else {
		if logOut {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
				Str("cmd", redisCmd.CmdString()).
				Str("reply", redisCmd.ReplyString()).
				Msg("Redis TryLock success")
		}
		return func() {
			ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1)) // 重新写一个，位置变了
			redisCmd := &RedisCommond{
				ctx:     ctx,
				Cmd:     "SCRIPT",
				Args:    []interface{}{key, uuid},
				CmdDesc: "TryLock",
			}
			r.doScriptCmd(ctx, deleteLockKeyScript, redisCmd)
		}, nil
	}
}

// 只尝试多次加锁，超时后，返回失败
func (r *Redis) Lock(ctx context.Context, key string, timeout time.Duration) (func(), error) {
	caller, ok := ctx.Value(utils.CtxKey_caller).(*utils.CallerDesc)
	if !ok {
		caller = utils.GetCallerDesc(1)
		ctx = context.WithValue(ctx, utils.CtxKey_caller, caller)
	}
	uuid := utils.LocalIPString() + "-" + strconv.Itoa(os.Getpid()) + "-" + caller.Pos() + "-" + utils.RandString(8)
	redisCmd := &RedisCommond{
		ctx:     ctx,
		Cmd:     "SET",
		Args:    []interface{}{key, uuid, "PX", timeout.Milliseconds(), "NX"},
		CmdDesc: "Lock",
	}

	logOut := true
	if ctx != nil {
		nolog := ctx.Value(CtxKey_nolog)
		if nolog != nil {
			logOut = false
		} else {
			ctx = context.WithValue(ctx, CtxKey_nolog, 1) // 命令传递下去不需要日志了
		}
	} else {
		ctx = context.WithValue(context.TODO(), CtxKey_nolog, 1)
	}

	entry := time.Now()
	spinCnt := 0 // 自旋次数
	for {
		spinCnt++

		r.docmd(ctx, redisCmd)

		if redisCmd.Err != nil {
		} else {
			var result string
			result, redisCmd.Err = redis.String(redisCmd.Reply, redisCmd.Err)
			if redisCmd.Err != nil || result != "OK" {
				if redisCmd.Err == nil {
					redisCmd.Err = errors.New("not OK")
				}
			} else {
				break
			}
		}
		// 每2秒输出一个等待日志 Err级别，便于外部查问题
		elapsed := time.Since(entry)
		if elapsed%(time.Second*2) == 0 {
			lock, _ := r.DoCmdString(ctx, "GET", key)
			utils.LogCtx(log.Error(), ctx).Int32("elapsed", int32(elapsed/time.Millisecond)).
				Str("cmd", redisCmd.CmdString()).
				Str("lock", lock).
				Msg("Redis Lock wait")
		}
		time.Sleep(time.Millisecond * 10)
		if time.Since(entry) > timeout {
			if redisCmd.Err == nil {
				redisCmd.Err = errors.New("time out")
			}
			break
		}
	}
	redisCmd.Elapsed = time.Since(entry)

	if redisCmd.Err != nil {
		utils.LogCtx(log.Error(), ctx).Err(redisCmd.Err).Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
			Str("cmd", redisCmd.CmdString()).
			Str("reply", redisCmd.ReplyString()).
			Int("spinCnt", spinCnt).
			Msg("Redis Lock fail")
		return nil, redisCmd.Err
	} else {
		if logOut {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(redisCmd.Elapsed/time.Millisecond)).
				Str("cmd", redisCmd.CmdString()).
				Str("reply", redisCmd.ReplyString()).
				Int("spinCnt", spinCnt).
				Msg("Redis Lock success")
		}
		return func() {
			ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1)) // 重新写一个，位置变了
			redisCmd := &RedisCommond{
				ctx:     ctx,
				Cmd:     "SCRIPT",
				Args:    []interface{}{key, uuid},
				CmdDesc: "Lock",
			}
			r.doScriptCmd(ctx, deleteLockKeyScript, redisCmd)
		}, nil
	}
}
