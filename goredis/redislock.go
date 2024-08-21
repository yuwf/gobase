package goredis

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

var deleteLockKeyScript = NewScript(`
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
	ctx = context.WithValue(ctx, CtxKey_nonilerr, 1) // 不要nil错误
	ctx = context.WithValue(ctx, CtxKey_cmddesc, "TryLock")

	entry := time.Now()
	cmd := r.Do(ctx, "SET", key, uuid, "PX", timeout.Milliseconds(), "NX")

	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	reply, _ := cmd.Text()
	if reply != "OK" {
		err := errors.New("not OK")
		if logOut {
			// Debug就行 毕竟是try
			utils.LogCtx(log.Debug(), ctx).Err(err).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Msg("Redis TryLock fail")
		}
		return nil, err
	} else {
		if logOut {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Msg("Redis TryLock success")
		}
		return func() {
			r.DoScript(ctx, deleteLockKeyScript, []string{key}, uuid)
		}, nil
	}
}

// 只尝试一次加锁，失败会一直等待其他解锁，其他解锁后也不会再加锁了（返回成功，但返回的func为空），成功了直接返回
func (r *Redis) TryLockWait(ctx context.Context, key string, timeout time.Duration) (func(), error) {
	caller, ok := ctx.Value(utils.CtxKey_caller).(*utils.CallerDesc)
	if !ok {
		caller = utils.GetCallerDesc(1)
		ctx = context.WithValue(ctx, utils.CtxKey_caller, caller)
	}
	uuid := utils.LocalIPString() + "-" + strconv.Itoa(os.Getpid()) + "-" + caller.Pos() + "-" + utils.RandString(8)

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
	ctx = context.WithValue(ctx, CtxKey_nonilerr, 1) // 不要nil错误
	ctx = context.WithValue(ctx, CtxKey_cmddesc, "TryLockWait")

	entry := time.Now()
	logtime := entry
	spinCnt := 0 // 自旋次数
	var err error
	for {
		spinCnt++

		if spinCnt == 1 {
			cmd := r.Do(ctx, "SET", key, uuid, "PX", timeout.Milliseconds(), "NX")
			if cmd.Err() != nil {
			} else {
				ok, _ := cmd.Text()
				if ok == "OK" {
					break
				}
			}
		} else {
			cmd := r.Do(ctx, "EXISTS", key)
			if cmd.Err() != nil {
			} else {
				ok, _ := cmd.Int()
				if ok == 0 {
					return nil, nil
				}
			}
		}

		// 每2秒输出一个等待日志 Err级别，便于外部查问题
		now := time.Now()
		if now.Sub(logtime) >= time.Second*2 {
			logtime = time.Now()
			lock := r.Get(ctx, key)
			utils.LogCtx(log.Error(), ctx).Int32("elapsed", int32(now.Sub(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Str("lock", lock.String()).
				Msg("Redis TryLockWait wait")
		}
		time.Sleep(time.Millisecond * 10)
		if time.Since(entry) > timeout {
			err = errors.New("time out")
			break
		}
	}

	if err != nil {
		// Debug就行 毕竟是try
		utils.LogCtx(log.Debug(), ctx).Err(err).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
			Str("key", key).
			Str("uuid", uuid).
			Int("spinCnt", spinCnt).
			Msg("Redis TryLockWait fail")
		return nil, err
	} else {
		if logOut {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Int("spinCnt", spinCnt).
				Msg("Redis TryLockWait success")
		}
		return func() {
			r.DoScript(ctx, deleteLockKeyScript, []string{key}, uuid)
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
	ctx = context.WithValue(ctx, CtxKey_nonilerr, 1) // 不要nil错误
	ctx = context.WithValue(ctx, CtxKey_cmddesc, "Lock")

	entry := time.Now()
	logtime := entry
	spinCnt := 0 // 自旋次数
	var err error
	for {
		spinCnt++

		cmd := r.Do(ctx, "SET", key, uuid, "PX", timeout.Milliseconds(), "NX")

		if cmd.Err() != nil {
		} else {
			ok, _ := cmd.Text()
			if ok == "OK" {
				break
			}
		}
		// 每2秒输出一个等待日志 Err级别，便于外部查问题
		now := time.Now()
		if now.Sub(logtime) >= time.Second*2 {
			logtime = time.Now()
			lock := r.Get(ctx, key)
			utils.LogCtx(log.Error(), ctx).Int32("elapsed", int32(now.Sub(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Str("lock", lock.String()).
				Msg("Redis Lock wait")
		}
		time.Sleep(time.Millisecond * 10)
		if time.Since(entry) > timeout {
			err = errors.New("time out")
			break
		}
	}

	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
			Str("key", key).
			Str("uuid", uuid).
			Int("spinCnt", spinCnt).
			Msg("Redis Lock fail")
		return nil, err
	} else {
		if logOut {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Int("spinCnt", spinCnt).
				Msg("Redis Lock success")
		}
		return func() {
			r.DoScript(ctx, deleteLockKeyScript, []string{key}, uuid)
		}, nil
	}
}
