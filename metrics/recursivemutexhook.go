package metrics

// https://github.com/yuwf/gobase

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	recursiveMutexHookOnce sync.Once
	recursiveMutexOnce     sync.Once

	recursiveMutexLockCount   *prometheus.CounterVec
	recursiveMutexUnLockCount *prometheus.CounterVec
	recursiveMutexWaitTime    *prometheus.CounterVec
	recursiveMutexWorkTime    *prometheus.CounterVec

	recursiveMutexCronEntryID  int
	recursiveMutexingOnce      sync.Once
	recursiveMutexWaitingCount *prometheus.GaugeVec
	recursiveMutexLockingCount *prometheus.GaugeVec
	recursiveMutexWaitingTime  *prometheus.GaugeVec
	recursiveMutexLockintTime  *prometheus.GaugeVec
)

func init() {
	recursiveMutexCrontMetrics()
}

func recursiveMutexCrontMetrics() {
	if recursiveMutexCronEntryID != 0 {
		utils.CronRemoveFunc(recursiveMutexCronEntryID)
	}
	// 统计递归锁的等待和锁定情况
	spec := fmt.Sprintf("*/%d * * * * *", ScrapeInterval)
	recursiveMutexCronEntryID, _ = utils.CronAddFunc(spec, func() {
		recursiveMutexingOnce.Do(func() {
			recursiveMutexWaitingCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "recursive_mutex_waiting"}, []string{"pos"})
			recursiveMutexLockingCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "recursive_mutex_locking"}, []string{"pos"})
			recursiveMutexWaitingTime = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "recursive_mutex_waiting_time"}, []string{"pos"})
			recursiveMutexLockintTime = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "recursive_mutex_locking_time"}, []string{"pos"})
		})

		// 清除之前的指标
		recursiveMutexWaitingCount.Reset()
		recursiveMutexLockingCount.Reset()
		recursiveMutexWaitingTime.Reset()
		recursiveMutexLockintTime.Reset()

		now := time.Now()
		infos := utils.AllRecursiveMutexsInfo()
		for _, info := range infos {
			if info.LockTime.IsZero() {
				recursiveMutexWaitingCount.WithLabelValues(info.LockPos.Loc()).Inc()
				recursiveMutexWaitingTime.WithLabelValues(info.LockPos.Loc()).Add(float64(now.Sub(info.WaitTime).Nanoseconds()))
			} else {
				recursiveMutexLockingCount.WithLabelValues(info.LockPos.Loc()).Inc()
				recursiveMutexLockintTime.WithLabelValues(info.LockPos.Loc()).Add(float64(now.Sub(info.LockTime).Nanoseconds()))
			}
		}
	})
}

func recursiveMutexHook(ctx context.Context, opt int, info *utils.RecursiveMutexInfo) {
	recursiveMutexOnce.Do(func() {
		recursiveMutexLockCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "recursive_mutex_lock"}, []string{"pos"})
		recursiveMutexUnLockCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "recursive_mutex_unlock"}, []string{"pos"})
		recursiveMutexWaitTime = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "recursive_mutex_wait_time"}, []string{"pos"})
		recursiveMutexWorkTime = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "recursive_mutex_work_time"}, []string{"pos"})
	})

	if opt == utils.RecursiveMutexLocked {
		recursiveMutexLockCount.WithLabelValues(info.LockPos.Loc()).Inc()
		if !info.IsTry && !info.IsRecursive {
			recursiveMutexWaitTime.WithLabelValues(info.LockPos.Loc()).Add(float64(info.LockTime.Sub(info.WaitTime).Nanoseconds()))
		}
	}
	if opt == utils.RecursiveMutexUnLock {
		recursiveMutexUnLockCount.WithLabelValues(info.LockPos.Loc()).Inc()
		if !info.IsTry && !info.IsRecursive {
			recursiveMutexWorkTime.WithLabelValues(info.LockPos.Loc()).Add(float64(info.UnLockTime.Sub(info.LockTime).Nanoseconds()))
		}
	}
}
