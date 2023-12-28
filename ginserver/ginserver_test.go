package ginserver

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	_ "github.com/yuwf/gobase/log"
	"github.com/yuwf/gobase/utils"

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

func getNameTimeOut(ctx context.Context, c *gin.Context, resp *getNameResp) {
	time.Sleep(time.Second * 5)
	resp.Name = "hello"
}

func getName2(ctx context.Context, c *gin.Context, req *getNameReq, resp *getNameResp) {
	resp.Name = "hello"
	rawdata, err := c.GetRawData()
	log.Info().Err(err).Bytes("tt", rawdata).Msg("rawdata")
}

func noroute(ctx context.Context, c *gin.Context) {
	c.AbortWithStatus(http.StatusNotFound)
}

func BenchmarkGinServer(b *testing.B) {
	ParamConf.Get().LogLevelHeadByPath = map[string]int{"/getna*": 0}
	//ParamConf.Get().TimeOutCheck = 2
	ParamConf.Get().Hystrix = map[string]*hystrix.CommandConfig{
		"/hystrix": &hystrix.CommandConfig{Timeout: 10 * 1000, MaxConcurrentRequests: 10, RequestVolumeThreshold: 1, SleepWindow: 10 * 1000},
	}
	ParamConf.Get().Normalize()
	server := NewGinServer(1234)
	server.RegJsonHandler("", "/getname1", getNameTimeOut)
	server.RegJsonHandler("", "/getname2", getName2)
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

func BenchmarkGinServerRawData(b *testing.B) {
	server := NewGinServer(1234)

	server.RegHandler("", "/rawdata", func(c *gin.Context) {
		rawdata, err := c.GetRawData()
		log.Info().Err(err).Bytes("tt", rawdata).Msg("rawdata")
	})
	server.Start()
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})

	utils.ExitWait()
}

func BenchmarkGinServerTimeOut(b *testing.B) {
	ParamConf.Get().TimeOutCheck = 2
	ParamConf.Get().Normalize()
	server := NewGinServer(1234)
	server.RegJsonHandler("", "/getnametimeout", getNameTimeOut)
	server.Start()
	utils.RegExit(func(s os.Signal) {
		server.Stop() // 退出服务监听
	})

	utils.ExitWait()
}
