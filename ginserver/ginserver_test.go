package ginserver

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	_ "gobase/log"
	"gobase/utils"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
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

func BenchmarkGinServer(b *testing.B) {
	ParamConf.Get().IgnorePath = []string{"/getnae?"}
	ParamConf.Get().Hystrix = map[string]*hystrix.CommandConfig{
		"/hystrix": &hystrix.CommandConfig{Timeout: 10 * 1000, MaxConcurrentRequests: 10, RequestVolumeThreshold: 1, SleepWindow: 10 * 1000},
	}
	ParamConf.Get().Normalize()
	server := NewGinServer(1234)
	server.RegJsonHandler("GET", "/getname1", getName1)
	server.RegJsonHandler("GET", "/getname2", getName2)
	server.RegHandler("GET", "/health", func(c *gin.Context) { c.String(http.StatusOK, "success") })
	server.RegHandler("GET", "/hystrix", func(c *gin.Context) {
		log.Info().Msg("sleep begin")
		time.Sleep(10 * time.Second)
		log.Info().Msg("sleep over")
		c.String(http.StatusOK, "hystrix")
	})
	server.RegNoRouteHandler(noroute)
	server.Start()
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})

	utils.ExitWait()
}
