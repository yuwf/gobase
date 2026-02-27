package metrics

// https://github.com/yuwf/gobase

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/utils"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// MySQL
	mysqlOnce sync.Once

	mysqlErrorCount *prometheus.CounterVec

	mysqlLatency *prometheus.HistogramVec
	mysqlCount   *prometheus.CounterVec
	mysqlSum     *prometheus.CounterVec // 耗时之和

	mysqlTraceCount *prometheus.CounterVec // 如果context中函有utils.CtxKey_traceName，会加入统计
	mysqlTraceTime  *prometheus.CounterVec

	mysqlCronEntryID            int
	allMysqlOnce                sync.Once
	mysqlConnsCount             *prometheus.GaugeVec
	mysqlConnsInUseCount        *prometheus.GaugeVec
	mysqlWaitCount              *prometheus.CounterVec
	mysqlMaxIdleClosedCount     *prometheus.CounterVec // 因最大空闲满了关闭的数量
	mysqlMaxIdleTimeClosedCount *prometheus.CounterVec // 因在空闲时间内关闭的数量
	mysqlMaxLifetimeClosedCount *prometheus.CounterVec // 因超过最大存活时间关闭的数量
	mysqlWaittime               *prometheus.CounterVec // 等待新链接的时间
)

func init() {
	mysqlCronMetrics()
}

func mysqlCronMetrics() {
	if mysqlCronEntryID != 0 {
		utils.CronRemoveFunc(mysqlCronEntryID)
	}
	// 统计mysql连接信息
	spec := fmt.Sprintf("*/%d * * * * *", ScrapeInterval)
	mysqlCronEntryID, _ = utils.CronAddFunc(spec, func() {
		allMysqlOnce.Do(func() {
			mysqlConnsCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "mysql_conns_count"}, []string{"source"})
			mysqlConnsInUseCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "mysql_conns_inuse_count"}, []string{"source"})
			mysqlWaitCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "mysql_wait_count"}, []string{"source"})

			mysqlMaxIdleClosedCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "mysql_maxidle_close_count"}, []string{"source"})
			mysqlMaxIdleTimeClosedCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "mysql_maxidletime_close_count"}, []string{"source"})
			mysqlMaxLifetimeClosedCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "mysql_maxlifetime_close_count"}, []string{"source"})
			mysqlWaittime = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "mysql_waittime"}, []string{"source"})
		})

		// 清除之前的指标
		mysqlConnsCount.Reset()
		mysqlConnsInUseCount.Reset()
		mysqlWaitCount.Reset()
		mysqlMaxIdleClosedCount.Reset()
		mysqlMaxIdleTimeClosedCount.Reset()
		mysqlMaxLifetimeClosedCount.Reset()
		mysqlWaittime.Reset()

		// 获取MySQL的所有连接
		mysql.AllMySQL.Range(func(key, value any) bool {
			m := key.(*mysql.MySQL)
			source := m.Source()
			cfg, err := dmysql.ParseDSN(source)
			if err != nil {
				return true
			}
			stats := m.DB().Stats()

			s := fmt.Sprintf("%s/%s", cfg.Addr, cfg.DBName)
			mysqlConnsCount.WithLabelValues(s).Add(float64(stats.OpenConnections))
			mysqlConnsInUseCount.WithLabelValues(s).Add(float64(stats.InUse))
			mysqlWaitCount.WithLabelValues(s).Add(float64(stats.WaitCount))

			mysqlMaxIdleClosedCount.WithLabelValues(s).Add(float64(stats.MaxIdleClosed))
			mysqlMaxIdleTimeClosedCount.WithLabelValues(s).Add(float64(stats.MaxIdleTimeClosed))
			mysqlMaxLifetimeClosedCount.WithLabelValues(s).Add(float64(stats.MaxLifetimeClosed))
			mysqlWaittime.WithLabelValues(s).Add(float64(stats.WaitDuration.Nanoseconds()))

			return true
		})

	})
}

func mysqlHook(ctx context.Context, cmd *mysql.MySQLCommond) {
	mysqlOnce.Do(func() {
		mysqlErrorCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "mysql_error_count"}, []string{"cmd"})
		if MysqlHistogram {
			mysqlLatency = DefaultReg().NewHistogramVec(prometheus.HistogramOpts{Name: "mysql",
				Buckets: []float64{256, 512, 1000, 2000, 4000, 16000, 64000, 256000, 1000000, 2000000, 4000000, 16000000}},
				[]string{"cmd"},
			)
		} else {
			mysqlCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "mysql_count"}, []string{"cmd"})
			mysqlSum = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "mysql_sum"}, []string{"cmd"})
		}

		mysqlTraceCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "mysql_trace_count"}, []string{"name"})
		mysqlTraceTime = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "mysql_trace_time"}, []string{"name"})
	})

	cmdName := strings.ToUpper(cmd.Cmd)

	if cmd.Err != nil {
		mysqlErrorCount.WithLabelValues(cmdName).Inc()
	}
	if mysqlLatency != nil {
		mysqlLatency.WithLabelValues(cmdName).Observe(float64(cmd.Elapsed.Nanoseconds()))
	} else {
		mysqlCount.WithLabelValues(cmdName).Inc()
		mysqlSum.WithLabelValues(cmdName).Add(float64(cmd.Elapsed.Nanoseconds()))
	}

	// 消息统计
	if ctx != nil {
		if traceName := ctx.Value(utils.CtxKey_traceName); traceName != nil {
			if s, ok := traceName.(string); ok && len(s) > 0 {
				mysqlTraceCount.WithLabelValues(s).Inc()
				mysqlTraceTime.WithLabelValues(s).Add(float64(cmd.Elapsed.Nanoseconds()))
			}
		}
	}
}
