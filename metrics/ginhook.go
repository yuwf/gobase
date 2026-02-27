package metrics

// https://github.com/yuwf/gobase

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// gin
	ginOnce sync.Once
	//ginCnt        *prometheus.CounterVec
	ginErrorCount *prometheus.CounterVec

	ginLatency *prometheus.HistogramVec
	ginCount   *prometheus.CounterVec
	ginSum     *prometheus.CounterVec

	ginPathRegexp *regexp2.Regexp
)

func init() {
	var err error
	// 分割 /
	ginPathRegexp, err = regexp2.Compile(`(?<=[/])(\d+|[^/]{17,})(?=[/]|$)`, regexp2.None) //regexp \d+|[^/]{16,}
	if err != nil {
		panic(err.Error())
	}
}

func ginHook(ctx context.Context, c *gin.Context, elapsed time.Duration) {
	ginOnce.Do(func() {
		ginErrorCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "gin_error_count"}, []string{"method", "path", "error"})
		if GinHistogram {
			ginLatency = DefaultReg().NewHistogramVec(prometheus.HistogramOpts{Name: "gin",
				Buckets: []float64{256, 512, 1000, 2000, 4000, 16000, 64000, 256000, 1000000, 2000000, 4000000, 16000000}},
				[]string{"method", "path"},
			)
		} else {
			ginCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "gin_count"}, []string{"method", "path"})
			ginSum = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "gin_sum"}, []string{"method", "path"})
		}
	})
	if c.Writer.Status() == http.StatusNotFound || c.Writer.Status() == http.StatusBadGateway {
		return
	}
	var path string
	k, err := ginPathRegexp.Replace(c.Request.URL.Path, "*", 0, -1)
	if err == nil {
		path = k
	}
	//ginCnt.WithLabelValues(strings.ToUpper(c.Request.Method), path).Inc()
	if len(c.Errors) > 0 {
		ginErrorCount.WithLabelValues(strings.ToUpper(c.Request.Method), path, c.Errors[0].Error()).Inc()
	} else if c.Writer.Status() != http.StatusOK {
		ginErrorCount.WithLabelValues(strings.ToUpper(c.Request.Method), path, strconv.Itoa(c.Writer.Status())).Inc()
	}
	if ginLatency != nil {
		ginLatency.WithLabelValues(strings.ToUpper(c.Request.Method), path).Observe(float64(elapsed.Nanoseconds()))
	} else {
		ginCount.WithLabelValues(strings.ToUpper(c.Request.Method), path).Inc()
		ginSum.WithLabelValues(strings.ToUpper(c.Request.Method), path).Add(float64(elapsed.Nanoseconds()))
	}
}
