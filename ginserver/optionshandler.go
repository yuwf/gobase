package ginserver

// https://github.com/yuwf/gobase

import (
	"context"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

// 外层注册时 可选
func (gs *GinServer) TimeOutCheck(c *gin.Context) {
	// 消息超时检查
	if ParamConf.Get().TimeOutCheck > 0 {
		ctxv, _ := c.Get("ctx")
		ctx, _ := ctxv.(context.Context)

		utils.Submit(func() {
			timer := time.NewTimer(time.Duration(ParamConf.Get().TimeOutCheck) * time.Second)
			select {
			case <-c.Request.Context().Done():
				if !timer.Stop() {
					select {
					case <-timer.C: // try to drain the channel
					default:
					}
				}
			case <-timer.C:
				// 消息超时了
				logHeadOut := true
				logLevelHead := ParamConf.Get().GetLogLevelHead(c.Request.URL.Path, c.ClientIP())
				if logLevelHead == int(zerolog.Disabled) || logLevelHead < int(zerolog.ErrorLevel) {
					logHeadOut = false
				}
				gs.log(ctx, c, int(zerolog.ErrorLevel), logHeadOut, time.Duration(ParamConf.Get().TimeOutCheck), nil, "GinServer TimeOut")
			}
		})
	}

	c.Next()
}
