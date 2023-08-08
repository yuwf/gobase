package goredis

// https://github.com/yuwf

import (
	"context"
	"errors"
	"os"
	"strconv"
	"time"

	"gobase/utils"

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
	caller := utils.GetCallerDesc(1)
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
	ctx = context.WithValue(ctx, CtxKey_caller, caller)
	ctx = context.WithValue(ctx, CtxKey_cmddesc, "TryLock")

	entry := time.Now()
	cmd := r.Do(ctx, "SET", key, uuid, "PX", timeout.Milliseconds(), "NX")

	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	ok, _ := cmd.Text()
	if ok != "OK" {
		err := errors.New("not OK")
		if logOut {
			// Debug就行 毕竟是try
			log.Debug().Err(err).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Str("pos", caller.Pos()).
				Msg("Redis TryLock fail")
		}
		return nil, err
	} else {
		if logOut {
			log.Debug().Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Str("pos", caller.Pos()).
				Msg("Redis TryLock success")
		}
		return func() {
			r.DoScript(ctx, deleteLockKeyScript, []string{key}, uuid)
		}, nil
	}
}

// 只尝试多次加锁，超时后，返回失败
func (r *Redis) Lock(ctx context.Context, key string, timeout time.Duration) (func(), error) {
	caller := utils.GetCallerDesc(1)
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
	ctx = context.WithValue(ctx, CtxKey_caller, caller)
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
			log.Error().Int32("elapsed", int32(now.Sub(entry)/time.Millisecond)).
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
		log.Error().Err(err).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
			Str("key", key).
			Str("uuid", uuid).
			Str("pos", caller.Pos()).
			Int("spinCnt", spinCnt).
			Msg("Redis Lock fail")
		return nil, err
	} else {
		if logOut {
			log.Debug().Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Str("pos", caller.Pos()).
				Int("spinCnt", spinCnt).
				Msg("Redis Lock success")
		}
		return func() {
			r.DoScript(ctx, deleteLockKeyScript, []string{key}, uuid)
		}, nil
	}
}
