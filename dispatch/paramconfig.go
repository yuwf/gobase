package dispatch

// https://github.com/yuwf/gobase

import (
	"github.com/yuwf/gobase/alert"
	"github.com/yuwf/gobase/loader"

	"github.com/afex/hystrix-go/hystrix"
)

func init() {
	alert.AddErrorLogPrefix("MsgDispatch Reg")
	alert.AddErrorLogPrefix("MsgDispatch TimeOut")
	alert.AddErrorLogPrefix("MsgDispatch Hystrix")
}

// 参数配置
type ParamConfig struct {
	// 日志级别和zerolog.Level一致
	LogLevelMsg   int            `json:"loglevelmsg,omitempty"`   // msg消息默认的消息级别，不配置就是debug级别
	LogLevelByMsg map[string]int `json:"loglevelbymsg,omitempty"` // 根据消息ID区分的消息日志级别，消息ID：日志级别，不配置就使用LogLevelMsg级别
	RegFuncShort  bool           `json:"logfuncshort,omitempty"`  // 函数名使用简短一些，否则就是类似dispatch.(*Server).onTestHeatBeatResp

	TimeOutCheck int `json:"timeoutcheck,omitempty"` // 消息超时监控 单位秒 默认0不开启监控
	// Timeout: 执行 command 的超时时间 单位为毫秒
	// MaxConcurrentRequests: 最大并发量
	// RequestVolumeThreshold: 一个统计窗口 10 秒内请求数量 达到这个请求数量后才去判断是否要开启熔断
	// SleepWindow: 熔断器被打开后 SleepWindow的时间就是控制过多久后去尝试服务是否可用了 单位为毫秒
	// ErrorPercentThreshold: 错误百分比 请求数量大于等于 RequestVolumeThreshold 并且错误率到达这个百分比后就会启动熔断
	HystrixMsg map[string]*hystrix.CommandConfig `json:"hystrixmsg,omitempty"` // 熔断器 [msid:Config]，目前不支持动态删除
}

var ParamConf loader.JsonLoader[ParamConfig]

func (c *ParamConfig) Create() {
	c.LogLevelByMsg = map[string]int{}
}

func (c *ParamConfig) Normalize() {
	for msgid, config := range c.HystrixMsg {
		c.HystrixMsg[msgid] = config
		hystrix.ConfigureCommand("msg_"+msgid, *config) // 加个msg_前缀，区别其他模块使用
	}
}

func (c *ParamConfig) MsgLogLevel(msgid string) int {
	loglevel, ok := c.LogLevelByMsg[msgid]
	if !ok {
		return c.LogLevelMsg
	}
	return loglevel
}

func (c *ParamConfig) IsHystrixMsg(msgid string) (string, bool) {
	_, ok := c.HystrixMsg[msgid]
	if ok {
		return "msg_" + msgid, true
	}
	return "", false
}
