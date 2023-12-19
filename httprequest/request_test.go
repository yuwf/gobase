package httprequest

import (
	"context"
	"testing"
	"time"

	"github.com/afex/hystrix-go/hystrix"
	_ "github.com/yuwf/gobase/log"
)

func BenchmarkHttpRequest(b *testing.B) {
	ctx := context.TODO()
	ParamConf.Get().Hystrix = map[string]*hystrix.CommandConfig{
		"*wwww.baidu*": &hystrix.CommandConfig{MaxConcurrentRequests: 1, SleepWindow: 10 * 1000},
	}
	ParamConf.Get().Normalize()
	go Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	go Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	go Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	go Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	go Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	go Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	go Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	go Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	time.Sleep(time.Second * 10)
}
