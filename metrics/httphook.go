package metrics

// https://github.com/yuwf/gobase

import (
	"context"
	"sync"

	"github.com/yuwf/gobase/httprequest"
	"github.com/yuwf/gobase/utils"

	"github.com/dlclark/regexp2"
	"github.com/prometheus/client_golang/prometheus"
)

var (

	// http
	httpOnce sync.Once
	//httpCnt        *prometheus.CounterVec
	httpErrorCount *prometheus.CounterVec

	httpLatency *prometheus.HistogramVec
	httpCount   *prometheus.CounterVec
	httpSum     *prometheus.CounterVec // 耗时之和

	httpTraceCount *prometheus.CounterVec // 如果context中函有utils.CtxKey_traceName，会加入统计
	httpTraceTime  *prometheus.CounterVec
	httpPathRegexp *regexp2.Regexp
)

func init() {
	var err error

	// 分割 /
	httpPathRegexp, err = regexp2.Compile(`(?<=[/])(\d+|[^/]{17,})(?=[/]|$)`, regexp2.None)
	if err != nil {
		panic(err.Error())
	}
}

func httpHook(ctx context.Context, request *httprequest.HttpRequest) {
	httpOnce.Do(func() {
		httpErrorCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "http_error_count"}, []string{"host", "path"})
		if HTTPHistogram {
			httpLatency = DefaultReg().NewHistogramVec(prometheus.HistogramOpts{Name: "http",
				Buckets: []float64{256, 512, 1000, 2000, 4000, 16000, 64000, 256000, 1000000, 2000000, 4000000, 16000000}},
				[]string{"host", "path"},
			)
		} else {
			httpCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "http_count"}, []string{"host", "path"})
			httpSum = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "http_sum"}, []string{"host", "path"})
		}

		httpTraceCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "http_trace_count"}, []string{"name"})
		httpTraceTime = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "http_trace_time"}, []string{"name"})
	})
	var host string
	var path string
	if request.URL != nil {
		host = request.URL.Host
		k, err := httpPathRegexp.Replace(request.URL.Path, "*", 0, -1)
		if err == nil {
			path = k
		}
	}
	httpErrorCount.WithLabelValues(host, path).Inc()
	if httpLatency != nil {
		httpLatency.WithLabelValues(host, path).Observe(float64(request.Elapsed.Nanoseconds()))
	} else {
		httpCount.WithLabelValues(host, path).Inc()
		httpCount.WithLabelValues(host, path).Add(float64(request.Elapsed.Nanoseconds()))
	}

	// 消息统计
	if ctx != nil {
		if traceName := ctx.Value(utils.CtxKey_traceName); traceName != nil {
			if s, ok := traceName.(string); ok && len(s) > 0 {
				httpTraceCount.WithLabelValues(s).Inc()
				httpTraceTime.WithLabelValues(s).Add(float64(request.Elapsed.Nanoseconds()))
			}
		}
	}
}
