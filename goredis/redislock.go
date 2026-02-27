package goredis

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog"
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
	uuid := utils.LocalIPString() + "-" + strconv.Itoa(os.Getpid()) + "-" + utils.RandString(16)

	logOut := !utils.CtxHasNolog(ctx)
	ctx = utils.CtxSetNolog(ctx)                     // 命令传递下去不需要日志了
	ctx = context.WithValue(ctx, CtxKey_nonilerr, 1) // 不要nil错误
	ctx = context.WithValue(ctx, CtxKey_cmddesc, "TryLock")

	entry := time.Now()
	cmd := r.Do(ctx, "SET", key, uuid, "PX", timeout.Milliseconds(), "NX")

	if cmd.Err() != nil {
		return nil, errors.New("lock err(" + cmd.Err().Error() + ")")
	}

	reply, _ := cmd.Text()
	if reply != "OK" {
		err := errors.New("lock err(not OK)")
		if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			// Debug就行 毕竟是try
			utils.LogCtx(log.Debug(), ctx).Err(err).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Msg("Redis TryLock Fail")
		}
		return nil, err
	} else {
		if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Msg("Redis TryLock Success")
		}
		return func() {
			r.DoScript(ctx, deleteLockKeyScript, []string{key}, uuid)
		}, nil
	}
}

// 只尝试一次加锁，失败会一直等待其他解锁，异常或者超时后，返回错误
// 其他解锁后也不会再加锁了（返回成功，但返回的func为空），成功了直接返回
func (r *Redis) TryLockWait(ctx context.Context, key string, timeout time.Duration) (func(), error) {
	uuid := utils.LocalIPString() + "-" + strconv.Itoa(os.Getpid()) + "-" + utils.RandString(16)

	logOut := !utils.CtxHasNolog(ctx)
	ctx = utils.CtxSetNolog(ctx)                     // 命令传递下去不需要日志了
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

		// 每2秒输出一个等待日志 Info级别，便于外部查问题
		now := time.Now()
		if now.Sub(logtime) >= time.Second*2 {
			logtime = time.Now()
			lock := r.Get(ctx, key)
			utils.LogCtx(log.Info(), ctx).Int32("elapsed", int32(now.Sub(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Str("lock", lock.String()).
				Msg("Redis TryLockWait Waiting")
		}
		time.Sleep(time.Millisecond * 10)
		if time.Since(entry) > timeout {
			err = errors.New("lock time out")
			break
		}
	}

	if err != nil {
		// Debug就行 毕竟是try
		utils.LogCtx(log.Debug(), ctx).Err(err).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
			Str("key", key).
			Str("uuid", uuid).
			Int("spinCnt", spinCnt).
			Msg("Redis TryLockWait Fail")
		return nil, err
	} else {
		if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Int("spinCnt", spinCnt).
				Msg("Redis TryLockWait Success")
		}
		return func() {
			r.DoScript(ctx, deleteLockKeyScript, []string{key}, uuid)
		}, nil
	}
}

var keyLockWaitScript = NewScript(`
	if redis.call('EXISTS', KEYS[1]) == 1 then
		return 1
	end
	if redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[2], 'NX') then
		return 2
	end
	return 0
`)

// 1:key 不存在 keylock 不存在 写入keylock      返回 func nil  func用来删除keylock
// 2:key 不存在 keylock   存在 等待keylock消失  返回 nil nil
// 3:key   存在                直接退出         返回 nil nil
func (r *Redis) KeyLockWait(ctx context.Context, key, keylock string, timeout time.Duration) (func(), error) {
	uuid := utils.LocalIPString() + "-" + strconv.Itoa(os.Getpid()) + "-" + utils.RandString(16)

	logOut := !utils.CtxHasNolog(ctx)
	ctx = utils.CtxSetNolog(ctx)                     // 命令传递下去不需要日志了
	ctx = context.WithValue(ctx, CtxKey_nonilerr, 1) // 不要nil错误
	ctx = context.WithValue(ctx, CtxKey_cmddesc, "KeyLockWait")

	entry := time.Now()
	logtime := entry
	spinCnt := 0 // 自旋次数
	var err error
	for {
		spinCnt++

		if spinCnt == 1 {
			cmd := r.DoScript(ctx, keyLockWaitScript, []string{key, keylock}, []interface{}{uuid, timeout.Milliseconds()})
			if cmd.Err() != nil {
			} else {
				ok, _ := cmd.Int()
				if ok == 1 {
					return nil, nil // key已经存在了
				} else if ok == 2 {
					break // 加锁成功
				}
				// 等待keylock消失
			}
		} else {
			cmd := r.Do(ctx, "EXISTS", keylock)
			if cmd.Err() != nil {
			} else {
				ok, _ := cmd.Int()
				if ok == 0 {
					return nil, nil
				}
			}
		}

		// 每2秒输出一个等待日志 Info级别，便于外部查问题
		now := time.Now()
		if now.Sub(logtime) >= time.Second*2 {
			logtime = time.Now()
			lock := r.Get(ctx, key)
			utils.LogCtx(log.Info(), ctx).Int32("elapsed", int32(now.Sub(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Str("lock", lock.String()).
				Msg("Redis KeyLockWait Waiting")
		}
		time.Sleep(time.Millisecond * 10)
		if time.Since(entry) > timeout {
			err = errors.New("lock time out")
			break
		}
	}

	if err != nil {
		// Debug就行 毕竟是try
		utils.LogCtx(log.Debug(), ctx).Err(err).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
			Str("key", key).
			Str("uuid", uuid).
			Int("spinCnt", spinCnt).
			Msg("Redis KeyLockWait Fail")
		return nil, err
	} else {
		if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Int("spinCnt", spinCnt).
				Msg("Redis KeyLockWait Success")
		}
		return func() {
			r.DoScript(ctx, deleteLockKeyScript, []string{keylock}, uuid)
		}, nil
	}
}

// 只尝试多次加锁，异常或者超时后，返回错误
func (r *Redis) Lock(ctx context.Context, key string, timeout time.Duration) (func(), error) {
	uuid := utils.LocalIPString() + "-" + strconv.Itoa(os.Getpid()) + "-" + utils.RandString(16)

	logOut := !utils.CtxHasNolog(ctx)
	ctx = utils.CtxSetNolog(ctx)                     // 命令传递下去不需要日志了
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
		// 每2秒输出一个等待日志 Info级别，便于外部查问题
		now := time.Now()
		if now.Sub(logtime) >= time.Second*2 {
			logtime = time.Now()
			lock := r.Get(ctx, key)
			utils.LogCtx(log.Info(), ctx).Int32("elapsed", int32(now.Sub(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Str("lock", lock.String()).
				Msg("Redis Lock Waiting")
		}
		time.Sleep(time.Millisecond * 10)
		if time.Since(entry) > timeout {
			err = errors.New("lock time out")
			break
		}
	}

	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
			Str("key", key).
			Str("uuid", uuid).
			Int("spinCnt", spinCnt).
			Msg("Redis Lock Fail")
		return nil, err
	} else {
		if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(time.Since(entry)/time.Millisecond)).
				Str("key", key).
				Str("uuid", uuid).
				Int("spinCnt", spinCnt).
				Msg("Redis Lock Success")
		}
		return func() {
			r.DoScript(ctx, deleteLockKeyScript, []string{key}, uuid)
		}, nil
	}
}
