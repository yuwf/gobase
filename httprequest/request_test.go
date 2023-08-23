package httprequest

import (
	"context"
	"testing"
	"time"

	_ "gobase/log"
)

func BenchmarkHttpRequest(b *testing.B) {
	ctx := context.TODO()
	Request(ctx, "GET", "https://wwww.baidu.com", nil, nil)
	time.Sleep(time.Second * 10)
}
