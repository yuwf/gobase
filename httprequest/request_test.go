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
	ParamConf.Get().LogLevelHeadByHost = map[string]int{"wwww.baidu.com*": 6}
	ParamConf.Get().Normalize()
	Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	time.Sleep(time.Second * 2)
	Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	time.Sleep(time.Second * 2)
	Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	time.Sleep(time.Second * 2)
	Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	time.Sleep(time.Second * 2)
	Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	time.Sleep(time.Second * 10)
}

func BenchmarkHttpRequestHystrix(b *testing.B) {
	ctx := context.TODO()
	//ParamConf.Get().TimeOutCheck = 2
	ParamConf.Get().Hystrix = map[string]*hystrix.CommandConfig{
		"*wwww.baidu*": {MaxConcurrentRequests: 1, SleepWindow: 10 * 1000},
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
