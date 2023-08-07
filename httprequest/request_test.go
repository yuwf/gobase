package httprequest

import (
	"context"
	"testing"
	"time"
)

func BenchmarkHttpRequest(b *testing.B) {
	ctx := context.TODO()
	Request(ctx, "POST", "http://127.0.0.1:9054/livegame/api/v1/douyin/live/data/push?asdfjj=23&dfj=456", nil, nil)
	time.Sleep(time.Second * 10)
}
