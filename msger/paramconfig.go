package msger

// https://github.com/yuwf/gobase

import (
	"github.com/yuwf/gobase/loader"

	"github.com/afex/hystrix-go/hystrix"
)

// 参数配置
type ParamConfig struct {
	LogLevel     LogLevel `json:"loglevel,omitempty"`     // 消息日志级别
	RegFuncShort bool     `json:"logfuncshort,omitempty"` // 函数名使用简短一些，否则就是类似dispatch.(*Server).onTestHeatBeatResp
	LogMaxLimit  int      `json:"logmaxlimit,omitempty"`  // 日志限制 <=0 表示不限制

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
	c.LogMaxLimit = 4096 // 4KB body日志限制
}

func (c *ParamConfig) Normalize() {
	for msgid, config := range c.HystrixMsg {
		c.HystrixMsg[msgid] = config
		hystrix.ConfigureCommand("msg_"+msgid, *config) // 加个msg_前缀，区别其他模块使用
	}
}

func (c *ParamConfig) IsHystrixMsg(msgid string) (string, bool) {
	_, ok := c.HystrixMsg[msgid]
	if ok {
		return "msg_" + msgid, true
	}
	return "", false
}
