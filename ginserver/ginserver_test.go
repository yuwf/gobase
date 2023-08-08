package ginserver

import (
	"context"
	"net/http"
	"os"
	"testing"

	_ "gobase/log"
	"gobase/utils"

	"github.com/gin-gonic/gin"
)

type getNameReq struct {
	Name string `json:"name"`
}

type getNameResp struct {
	Name string `json:"name"`
}

func getName1(ctx context.Context, c *gin.Context, resp *getNameResp) {
	resp.Name = "hello"
}

func getName2(ctx context.Context, c *gin.Context, req *getNameReq, resp *getNameResp) {
	resp.Name = "hello"
}

func noroute(ctx context.Context, c *gin.Context) {
	c.AbortWithStatus(http.StatusNotFound)
}

func BenchmarkGNetServer(b *testing.B) {
	server := NewGinServer(1234)
	server.RegJsonHandler("GET", "/getname1", getName1)
	server.RegJsonHandler("GET", "/getname2", getName2)
	server.RegNoRouteHandler(noroute)
	server.Start()
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})

	utils.ExitWait()
}
