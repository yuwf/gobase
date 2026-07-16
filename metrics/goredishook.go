package metrics

// https://github.com/yuwf/gobase

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/utils"

	"github.com/dlclark/regexp2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
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

func goredisHook(ctx context.Context, cmd redis.Cmder, cmds []redis.Cmder, elapsed time.Duration) {
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

	if cmd != nil && len(cmd.Args()) > 0 {
		// 找到key
		cmdName := fmt.Sprint(cmd.Args()[0])
		var key string
		pos := goredis.GetFirstKeyPos(cmd)
		if pos < len(cmd.Args()) {
			//for i := 1; i < pos; i++ {
			//	cmdName += fmt.Sprint(cmd.Args()[i])
			//}
			k := fmt.Sprint(cmd.Args()[pos])
			for _, exp := range redisKeyRegexp {
				k, err := exp.Replace(k, "*", 0, -1)
				if err == nil && (len(key) == 0 || len(k) < len(key)) {
					key = k
				}
			}
		}
		cmdName = strings.ToUpper(cmdName)
		if len(key) > 64 {
			key = key[:64] + "..."
		}

		if cmd.Err() != nil && !goredis.IsNil(cmd.Err()) {
			redisErrorCount.WithLabelValues(cmdName, key).Inc()
		}
		if redisLatency != nil {
			redisLatency.WithLabelValues(cmdName, key).Observe(float64(elapsed.Nanoseconds()))
		} else {
			redisCount.WithLabelValues(cmdName, key).Inc()
			redisSum.WithLabelValues(cmdName, key).Add(float64(elapsed.Nanoseconds()))
		}

		// 消息统计
		if ctx != nil {
			if traceName := ctx.Value(utils.CtxKey_traceName); traceName != nil {
				if s, ok := traceName.(string); ok && len(s) > 0 {
					redisTraceCount.WithLabelValues(s).Inc()
					redisTraceTime.WithLabelValues(s).Add(float64(elapsed.Nanoseconds()))
				}
			}
		}
	}
	if len(cmds) > 0 {
		keys := map[string]int{}
		for _, cmd := range cmds {
			// 找到key
			cmdName := fmt.Sprint(cmd.Args()[0])
			var key string
			pos := goredis.GetFirstKeyPos(cmd)
			if pos < len(cmd.Args()) {
				//for i := 1; i < pos; i++ {
				//	cmdName += fmt.Sprint(cmd.Args()[i])
				//}
				k := fmt.Sprint(cmd.Args()[pos])
				for _, exp := range redisKeyRegexp {
					k, err := exp.Replace(k, "*", 0, -1)
					if err == nil && (len(key) == 0 || len(k) < len(key)) {
						key = k
					}
				}
			}
			cmdName = strings.ToUpper(cmdName)
			keys[key]++

			if cmd.Err() != nil && !goredis.IsNil(cmd.Err()) {
				redisErrorCount.WithLabelValues(cmdName, key).Inc()
			}
			// 管道命令无法统计每条命令的耗时，只能统计管道命令的耗时
		}
		keys2 := make([]string, 0, len(keys))
		for k, v := range keys {
			keys2 = append(keys2, k+":"+fmt.Sprintf("%d", v))
		}
		sort.Strings(keys2) // 排序防止key组合太多
		key := strings.Join(keys2, ",")
		if len(key) > 128 {
			key = key[:128] + "..."
		}
		if redisLatency != nil {
			redisLatency.WithLabelValues("pipeline", key).Observe(float64(elapsed.Nanoseconds()))
		} else {
			redisCount.WithLabelValues("pipeline", key).Inc()
			redisSum.WithLabelValues("pipeline", key).Add(float64(elapsed.Nanoseconds()))
		}
		// 消息统计
		if ctx != nil {
			if traceName := ctx.Value(utils.CtxKey_traceName); traceName != nil {
				if s, ok := traceName.(string); ok && len(s) > 0 {
					redisTraceCount.WithLabelValues(s).Add(float64(len(cmds)))
					redisTraceTime.WithLabelValues(s).Add(float64(elapsed.Nanoseconds()))
				}
			}
		}
	}
}
