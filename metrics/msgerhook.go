package metrics

// https://github.com/yuwf/gobase

import (
	"context"
	"sync"
	"time"

	"github.com/yuwf/gobase/msger"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// MsgDispatch
	msgerOnce    sync.Once
	msgerLatency *prometheus.HistogramVec
	msgerCount   *prometheus.CounterVec
	msgerSum     *prometheus.CounterVec // 耗时之和
)

func msgDispatchHook(ctx context.Context, mr msger.Msger, elapsed time.Duration) {
	msgerOnce.Do(func() {
		if MsgerHistogram {
			msgerLatency = DefaultReg().NewHistogramVec(prometheus.HistogramOpts{Name: "msger",
				Buckets: []float64{256, 512, 1000, 2000, 4000, 16000, 64000, 256000, 1000000, 2000000, 4000000, 16000000}},
				[]string{"name"},
			)
		} else {
			msgerCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "msger_count"}, []string{"name"})
			msgerSum = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "msger_sum"}, []string{"name"})
		}
	})

	if msgerLatency != nil {
		if mner, _ := any(mr).(msger.MsgerName); mner != nil {
			msgerLatency.WithLabelValues(mner.MsgName()).Observe(float64(elapsed.Nanoseconds()))
		} else {
			msgerLatency.WithLabelValues(mr.MsgID()).Observe(float64(elapsed.Nanoseconds()))
		}
	} else {
		if mner, _ := any(mr).(msger.MsgerName); mner != nil {
			msgerCount.WithLabelValues(mner.MsgName()).Inc()
			msgerSum.WithLabelValues(mner.MsgName()).Add(float64(elapsed.Nanoseconds()))
		} else {
			msgerCount.WithLabelValues(mr.MsgID()).Inc()
			msgerSum.WithLabelValues(mr.MsgID()).Add(float64(elapsed.Nanoseconds()))
		}
	}
}
