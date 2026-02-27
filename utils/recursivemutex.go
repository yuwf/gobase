package utils

// https://github.com/yuwf/gobase

import (
	"context"
	"sync"
	"time"

	"github.com/petermattis/goid"
	"github.com/rs/zerolog/log"
)

const (
	RecursiveMutexWaitLock = 1 // 开始枷锁等待中
	RecursiveMutexLocked   = 2 // 加锁成功 TryLock或者递归加锁，会忽略RecursiveMutexWaitLock
	RecursiveMutexUnLock   = 3 // 解锁
)

// 报警配置
var RecursiveMutexWaitAlert = time.Minute
var RecursiveMutexLockAlert = time.Minute * 3

// 全局活跃 RecursiveMutex 表
var allRecursiveMutexs sync.Map // key = *RecursiveMutex, value = struct{}

// 执行命令时的回调 不使用锁，默认要求提前注册好
var recursiveMutexHook []func(ctx context.Context, opt int, info *RecursiveMutexInfo)

// TryLock和递归加锁不会触发RecursiveMutexWaitLock回调
// 回调会在锁内调用，不可在调用该锁操作，info信息只读
func RegRecursiveMutexHook(f func(ctx context.Context, opt int, info *RecursiveMutexInfo)) {
	recursiveMutexHook = append(recursiveMutexHook, f)
}

func init() {
	// 报警, 每分钟检查一次
	CronAddFunc("0 * * * * *", func() {
		now := time.Now()
		allRecursiveMutexs.Range(func(k, _ any) bool {
			m := k.(*RecursiveMutex)
			m.stateMu.Lock()
			for _, info := range m.infos {
				if info.LockTime.IsZero() {
					if info.alert != 1 && now.Sub(info.WaitTime) > RecursiveMutexWaitAlert {
						info.alert = 1
						LogCtx(log.Error(), info.Ctx).Str("callPos", info.LockPos.Pos()).Int64("gid", info.GID).TimeDiff("elapse", now, info.WaitTime).Msg("RecursiveMutex Wait timeout")
					}
				} else {
					if info.alert != 2 && now.Sub(info.LockTime) > RecursiveMutexLockAlert {
						info.alert = 2
						LogCtx(log.Error(), info.Ctx).Str("callPos", info.LockPos.Pos()).Int64("gid", info.GID).TimeDiff("elapse", now, info.LockTime).Msg("RecursiveMutex Lock timeout")
					}
				}
			}
			m.stateMu.Unlock()
			return true
		})
	})
}

// RecursiveMutexInfo 记录每次加锁的信息
type RecursiveMutexInfo struct {
	Ctx         context.Context
	GID         int64       // 协程ID
	IsTry       bool        // 是否TryLock， TryLock没有RecursiveMutexWaitLock操作，不直接RecursiveMutexLocked
	IsRecursive bool        // 是否递归加锁 递归加锁RecursiveMutexWaitLock操作，不直接RecursiveMutexLocked
	LockPos     *CallerDesc // 加锁位置
	WaitTime    time.Time   // 尝试加锁时间
	LockTime    time.Time   // 成功加锁时间；IsZero表示未成功
	UnLockTime  time.Time   // 解锁时间

	// 记录是否已经报警了
	alert int // 0:未报警 1:等待时报警了 2:加锁成功后报警了
}

// RecursiveMutex 封装可递归锁
type RecursiveMutex struct {
	mu sync.Mutex // 真正的等待锁

	stateMu sync.Mutex
	owner   int64                 // 协程ID
	infos   []*RecursiveMutexInfo // 加锁信息
}

// Lock 阻塞加锁
func (t *RecursiveMutex) Lock(ctx context.Context) {
	gid := goid.Get()
	pos := GetCallerDesc(1)
	tryStart := time.Now()

	info := &RecursiveMutexInfo{
		Ctx:      ctx,
		GID:      gid,
		LockPos:  pos,
		WaitTime: tryStart,
	}

	t.stateMu.Lock()
	if t.owner == gid {
		// 递归加锁，立即成功
		info.IsRecursive = true
		info.LockTime = tryStart
		t.infos = append(t.infos, info)
		t.stateMu.Unlock()

		// 回调
		func() {
			defer HandlePanic()
			for _, h := range recursiveMutexHook {
				h(ctx, RecursiveMutexLocked, info)
			}
		}()
		return
	}

	// 如果 infos 为空，说明锁当前未活跃，注册全局表
	if len(t.infos) == 0 {
		allRecursiveMutexs.Store(t, struct{}{})
	}
	t.infos = append(t.infos, info)
	t.stateMu.Unlock()

	// 回调
	func() {
		defer HandlePanic()
		for _, h := range recursiveMutexHook {
			h(ctx, RecursiveMutexWaitLock, info)
		}
	}()

	// 阻塞获取实际锁
	t.mu.Lock()
	lockTime := time.Now()

	t.stateMu.Lock()
	t.owner = gid
	info.LockTime = lockTime
	t.stateMu.Unlock()

	// 回调
	func() {
		defer HandlePanic()
		for _, h := range recursiveMutexHook {
			h(ctx, RecursiveMutexLocked, info)
		}
	}()
}

// TryLock 尝试加锁
func (t *RecursiveMutex) TryLock(ctx context.Context) bool {
	gid := goid.Get()
	pos := GetCallerDesc(1)
	tryStart := time.Now()

	t.stateMu.Lock()
	// 递归加锁，立即成功
	if t.owner == gid {
		info := &RecursiveMutexInfo{
			Ctx:         ctx,
			GID:         gid,
			IsTry:       true,
			IsRecursive: true,
			LockPos:     pos,
			WaitTime:    tryStart,
			LockTime:    tryStart,
		}
		t.infos = append(t.infos, info)
		t.stateMu.Unlock()

		// 回调
		func() {
			defer HandlePanic()
			for _, h := range recursiveMutexHook {
				h(ctx, RecursiveMutexLocked, info)
			}
		}()
		return true
	}
	t.stateMu.Unlock()

	// 尝试获取实际锁
	if !t.mu.TryLock() {
		return false
	}

	// 成功获取锁后再加入 infos
	info := &RecursiveMutexInfo{
		Ctx:      ctx,
		GID:      gid,
		IsTry:    true,
		LockPos:  pos,
		WaitTime: tryStart,
		LockTime: time.Now(),
	}

	t.stateMu.Lock()
	// 如果 infos 为空，说明锁当前未活跃，注册全局表
	if len(t.infos) == 0 {
		allRecursiveMutexs.Store(t, struct{}{})
	}
	t.owner = gid
	t.infos = append(t.infos, info)
	t.stateMu.Unlock()

	// 回调
	func() {
		defer HandlePanic()
		for _, h := range recursiveMutexHook {
			h(ctx, RecursiveMutexLocked, info)
		}
	}()

	return true
}

// Unlock 解锁
func (t *RecursiveMutex) Unlock(ctx context.Context) {
	gid := goid.Get()
	unLockTime := time.Now()

	t.stateMu.Lock()
	if t.owner != gid {
		t.stateMu.Unlock()
		//log.Printf("[RecursiveMutex] unlock mismatch: owner=%d, current=%d", t.owner, gid)
		return
	}

	// 删除当前递归层对应 RecursiveMutexInfo（尾部最近的 GID）
	var info *RecursiveMutexInfo
	for i := len(t.infos) - 1; i >= 0; i-- {
		if t.infos[i].GID == gid {
			info = t.infos[i]
			info.UnLockTime = unLockTime
			t.infos = append(t.infos[:i], t.infos[i+1:]...)
			break
		}
	}

	// 回调 在锁里调用，保证顺序
	if info != nil {
		func() {
			defer HandlePanic()
			for _, h := range recursiveMutexHook {
				h(ctx, RecursiveMutexUnLock, info)
			}
		}()
	}

	// 检查该 goroutine 是否还有持锁信息
	for _, info := range t.infos {
		if info.GID == gid {
			t.stateMu.Unlock()
			return
		}
	}

	// 该协程释放锁
	t.owner = 0
	// 如果 infos 为空，说明锁完全释放
	if len(t.infos) == 0 {
		allRecursiveMutexs.Delete(t)
	}
	t.stateMu.Unlock()

	// 释放锁
	t.mu.Unlock()
}

// 获取当前锁活跃记录（返回深拷贝，外部可安全只读）
func (t *RecursiveMutex) GetInfos(pre []*RecursiveMutexInfo) []*RecursiveMutexInfo {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()

	out := make([]*RecursiveMutexInfo, len(pre)+len(t.infos))
	copy(out, pre)
	for i := range t.infos {
		c := *t.infos[i]
		out[len(pre)+i] = &c
	}
	return out
}

// 全局打印所有活跃锁
func DumpAllRecursiveMutexs() {
	allRecursiveMutexs.Range(func(k, _ any) bool {
		m := k.(*RecursiveMutex)
		m.stateMu.Lock()
		for _, info := range m.infos {
			if info.LockTime.IsZero() {
				LogCtx(log.Error(), info.Ctx).Str("callPos", info.LockPos.Pos()).Int64("gid", info.GID).TimeDiff("wait", time.Now(), info.WaitTime).Msg("RecursiveMutex")
			} else {
				LogCtx(log.Error(), info.Ctx).Str("callPos", info.LockPos.Pos()).Int64("gid", info.GID).TimeDiff("held", time.Now(), info.LockTime).Msg("RecursiveMutex")
			}
		}
		m.stateMu.Unlock()
		return true
	})
}

// 获取所有活跃锁
func AllRecursiveMutexsInfo() []*RecursiveMutexInfo {
	var result []*RecursiveMutexInfo
	allRecursiveMutexs.Range(func(k, _ any) bool {
		m := k.(*RecursiveMutex)
		result = m.GetInfos(result)
		return true
	})
	return result
}
