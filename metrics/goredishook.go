package metrics

// https://github.com/yuwf/gobase

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/utils"

	"github.com/dlclark/regexp2"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// Redis
	redisOnce sync.Once
	//redisCnt       *prometheus.CounterVec
	redisErrorCount *prometheus.CounterVec

	redisLatency *prometheus.HistogramVec
	redisCount   *prometheus.CounterVec
	redisSum     *prometheus.CounterVec // 耗时之和

	redisTraceCount *prometheus.CounterVec // 如果context中函有utils.CtxKey_traceName，会加入统计
	redisTraceTime  *prometheus.CounterVec
	redisKeyRegexp  []*regexp2.Regexp
)

func init() {
	var err error
	redisExpr := []string{
		`(?<=[/\\\{:_\-\.@#])(\d+|[^/\\\{\}:_\-\.@#]{17,})(?=[/\\\{\}:_\-\.@#]|$)`, //分割 /\{}[]<>_-:.@#
		`(?<=[/\\\{:_\.@#])(\d+|[^/\\\{\}:_\.@#]{17,})(?=[/\\\{\}:_\.@#]|$)`,       //分割 /\{}[]<>_:.@#
		`(?<=[/\\\{_\-\.@#])(\d+|[^/\\\{\}_\-\.@#]{17,})(?=[/\\\{\}_\-\.@#]|$)`,    //分割 /\{}[]<>_-.@#
		`(?<=[/\\\{:_\-@#])(\d+|[^/\\\{\}:_\-@#]{17,})(?=[/\\\{\}:_\-@#]|$)`,       //分割 /\{}[]<>_-:@#
	}
	redisKeyRegexp = make([]*regexp2.Regexp, len(redisExpr))
	for i, s := range redisExpr {
		// 分割
		redisKeyRegexp[i], err = regexp2.Compile(s, regexp2.None)
		if err != nil {
			panic(err.Error())
		}
	}
}

func goredisHook(ctx context.Context, cmd *goredis.RedisCommond) {
	redisOnce.Do(func() {
		redisErrorCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "redis_error_count"}, []string{"cmd", "key"})
		if GoRedisHistogram {
			redisLatency = DefaultReg().NewHistogramVec(prometheus.HistogramOpts{Name: "redis",
				Buckets: []float64{256, 512, 1000, 2000, 4000, 16000, 64000, 256000, 1000000, 2000000, 4000000, 16000000}},
				[]string{"cmd", "key"},
			)
		} else {
			redisCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "redis_count"}, []string{"cmd", "key"})
			redisSum = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "redis_sum"}, []string{"cmd", "key"})
		}
		redisTraceCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "redis_trace_count"}, []string{"name"})
		redisTraceTime = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "redis_trace_time"}, []string{"name"})
	})

	if cmd.Cmd != nil && len(cmd.Cmd.Args()) > 0 {
		// 找到key
		cmdName := fmt.Sprint(cmd.Cmd.Args()[0])
		var key string
		pos := goredis.GetFirstKeyPos(cmd.Cmd)
		if pos < len(cmd.Cmd.Args()) {
			//for i := 1; i < pos; i++ {
			//	cmdName += fmt.Sprint(cmd.Cmd.Args()[i])
			//}
			k := fmt.Sprint(cmd.Cmd.Args()[pos])
			for _, exp := range redisKeyRegexp {
				k, err := exp.Replace(k, "*", 0, -1)
				if err == nil && (len(key) == 0 || len(k) < len(key)) {
					key = k
				}
			}
		}
		cmdName = strings.ToUpper(cmdName)

		if cmd.Cmd.Err() != nil && !goredis.IsNilError(cmd.Cmd.Err()) {
			redisErrorCount.WithLabelValues(cmdName, key).Inc()
		}
		if redisLatency != nil {
			redisLatency.WithLabelValues(cmdName, key).Observe(float64(cmd.Elapsed.Nanoseconds()))
		} else {
			redisCount.WithLabelValues(cmdName, key).Inc()
			redisSum.WithLabelValues(cmdName, key).Add(float64(cmd.Elapsed.Nanoseconds()))
		}

		// 消息统计
		if ctx != nil {
			if traceName := ctx.Value(utils.CtxKey_traceName); traceName != nil {
				if s, ok := traceName.(string); ok && len(s) > 0 {
					redisTraceCount.WithLabelValues(s).Inc()
					redisTraceTime.WithLabelValues(s).Add(float64(cmd.Elapsed.Nanoseconds()))
				}
			}
		}
	}
}
