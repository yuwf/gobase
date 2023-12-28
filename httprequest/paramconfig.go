package httprequest

// https://github.com/yuwf/gobase

import (
	"strings"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/yuwf/gobase/loader"
	"github.com/yuwf/gobase/utils"
)

const CtxKey_nolog = utils.CtxKey_nolog // 不打印日志，错误日志还会打印 值：不受限制 一般写1

func init() {
	// 因为报警依赖httprequest， 所以下面的报警直接在alert中添加好了
	//alert.AddErrorLogPrefix("HttpRequest Err")
	//alert.AddErrorLogPrefix("HttpRequest TimeOut")
}

// 参数配置
type ParamConfig struct {
	// 日志级别和zerolog.Level一致
	// Host和Path共存的配置，优先使用Path配置
	LogLevel           int            `json:"loglevel,omitempty"`           // 默认的级别，不配置就是info级别
	LogLevelByHost     map[string]int `json:"loglevelbyhost,omitempty"`     // 根据Host区分的日志级别，Host:日志级别, Host支持?*通配符 不区分大小写，不配置默认使用LogLevel级别
	LogLevelByPath     map[string]int `json:"loglevelbypath,omitempty"`     // 根据Path区分的日志级别，Path:日志级别 Path支持?*通配符 不区分大小写，不配置默认使用LogLevel级别
	LogLevelHead       int            `json:"loglevelhead,omitempty"`       // 请求头和回复头的日志级别，不配置就是debug级别
	LogLevelHeadByHost map[string]int `json:"loglevelheadbyhost,omitempty"` // 根据Host区分的请求头和回复头日志级别，Host:日志级别, Host支持?*通配符 不区分大小写，不配置默认使用LogLevelHead级别
	LogLevelHeadByPath map[string]int `json:"loglevelheadbypath,omitempty"` // 根据Path区分的请求头和回复头日志级别，Path:日志级别 Path支持?*通配符 不区分大小写，不配置默认使用LogLevelHead级别

	TimeOutCheck int  `json:"timeoutcheck,omitempty"` // 消息超时监控 单位秒 默认0不开启监控
	Pool         bool `json:"pool,omitempty"`         // 是否使用连接池，有些网址处理完请求就执行关闭，会导致连接池有问题，默认不使用

	BodyLogLimit int `json:"bodyloglimit,omitempty"` // body日志限制 <=0 表示不限制
	// Timeout: 执行 command 的超时时间 单位为毫秒
	// MaxConcurrentRequests: 最大并发量
	// RequestVolumeThreshold: 一个统计窗口 10 秒内请求数量 达到这个请求数量后才去判断是否要开启熔断
	// SleepWindow: 熔断器被打开后 SleepWindow的时间就是控制过多久后去尝试服务是否可用了 单位为毫秒
	// ErrorPercentThreshold: 错误百分比 请求数量大于等于 RequestVolumeThreshold 并且错误率到达这个百分比后就会启动熔断
	Hystrix map[string]*hystrix.CommandConfig `json:"hystrix,omitempty"` // 熔断器 [url:Config]，path 支持?*通配符 不区分大小写 目前不支持动态删除
}

var ParamConf loader.JsonLoader[ParamConfig]

func (c *ParamConfig) Create() {
	c.LogLevel = 1 // 外层配置比较好控制 这里默认info级别
	c.LogLevelByHost = map[string]int{}
	c.LogLevelByPath = map[string]int{}
	c.LogLevelHeadByHost = map[string]int{}
	c.LogLevelHeadByPath = map[string]int{}
	c.TimeOutCheck = 8 // 8秒超时报警
	c.BodyLogLimit = 1024
}

func (c *ParamConfig) Normalize() {
	for k, v := range c.LogLevelByHost {
		c.LogLevelByHost[strings.ToLower(k)] = v
	}
	for k, v := range c.LogLevelByPath {
		c.LogLevelByPath[strings.ToLower(k)] = v
	}
	for k, v := range c.LogLevelHeadByHost {
		c.LogLevelHeadByHost[strings.ToLower(k)] = v
	}
	for k, v := range c.LogLevelHeadByPath {
		c.LogLevelHeadByPath[strings.ToLower(k)] = v
	}
	for url, config := range c.Hystrix {
		url = strings.ToLower(url)
		c.Hystrix[url] = config
		hystrix.ConfigureCommand("http_"+url, *config) // 加个http_前缀，区别其他模块使用
	}
}

func (c *ParamConfig) GetLogLevel(path, host string) int {
	path = strings.ToLower(path)
	for k, v := range c.LogLevelByPath {
		if utils.IsMatch(k, path) {
			return v
		}
	}
	host = strings.ToLower(host)
	for k, v := range c.LogLevelByHost {
		if utils.IsMatch(k, host) {
			return v
		}
	}
	return c.LogLevel
}

func (c *ParamConfig) GetLogLevelHead(path, host string) int {
	path = strings.ToLower(path)
	for k, v := range c.LogLevelHeadByPath {
		if utils.IsMatch(k, path) {
			return v
		}
	}
	host = strings.ToLower(host)
	for k, v := range c.LogLevelHeadByHost {
		if utils.IsMatch(k, host) {
			return v
		}
	}
	return c.LogLevelHead
}

func (c *ParamConfig) IsHystrixURL(url string) (string, bool) {
	v := strings.ToLower(url)
	for path := range c.Hystrix {
		if utils.IsMatch(path, v) {
			return "http_" + path, true
		}
	}
	return "", false
}
