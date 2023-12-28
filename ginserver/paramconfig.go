package ginserver

// https://github.com/yuwf/gobase

import (
	"strings"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/yuwf/gobase/alert"
	"github.com/yuwf/gobase/loader"
	"github.com/yuwf/gobase/utils"
)

func init() {
	alert.AddErrorLogPrefix("GinServer RegHandler")
	alert.AddErrorLogPrefix("GinServer TimeOut")
	alert.AddErrorLogPrefix("GinServer Hystrix")
}

// 参数配置
type ParamConfig struct {
	// 日志级别和zerolog.Level一致
	// IP和Path共存的配置，优先使用Path配置
	LogLevel           int             `json:"loglevel,omitempty"`           // 默认的级别，不配置就是info级别
	LogLevelByIP       map[string]int  `json:"loglevelbyip,omitempty"`       // 根据IP区分的日志级别，IP:日志级别, IP支持?*通配符 不区分大小写，不配置默认使用LogLevel级别
	LogLevelByPath     map[string]int  `json:"loglevelbypath,omitempty"`     // 根据Path区分的日志级别，Path:日志级别 Path支持?*通配符 不区分大小写，不配置默认使用LogLevel级别
	LogLevelHead       int             `json:"loglevelhead,omitempty"`       // 请求头和回复头的日志级别，不配置就是debug级别
	LogLevelHeadByIP   map[string]int  `json:"loglevelheadbyip,omitempty"`   // 根据IP区分的请求头和回复头日志级别，IP:日志级别, IP支持?*通配符 不区分大小写，不配置默认使用LogLevelHead级别
	LogLevelHeadByPath map[string]int  `json:"loglevelheadbypath,omitempty"` // 根据Path区分的请求头和回复头日志级别，Path:日志级别 Path支持?*通配符 不区分大小写，不配置默认使用LogLevelHead级别
	LogMerge           bool            `json:"logmerge,omitempty"`           // req和resp是否合并成一条日志 默认合并
	LogMergeByIP       map[string]bool `json:"logmergebyip,omitempty"`       // 根据IP区分req和resp是否合并一条日志，IP:是否合并, IP支持?*通配符 不区分大小写，不配置默认使用LogMerge配置
	LogMergeByPath     map[string]bool `json:"logmergebypath,omitempty"`     // 根据Path区分req和resp是否合并一条日志，Path:是否合并 Path支持?*通配符 不区分大小写，不配置默认使用LogMerge配置

	TimeOutCheck int `json:"timeoutcheck,omitempty"` // 消息超时监控 单位秒 默认0不开启监控

	Cors         *CorsConfig `json:"cors,omitempty"`
	BodyLogLimit int         `json:"bodyloglimit,omitempty"` // body日志限制 <=0 表示不限制
	// Timeout: 执行 command 的超时时间 单位为毫秒
	// MaxConcurrentRequests: 最大并发量
	// RequestVolumeThreshold: 一个统计窗口 10 秒内请求数量 达到这个请求数量后才去判断是否要开启熔断
	// SleepWindow: 熔断器被打开后 SleepWindow的时间就是控制过多久后去尝试服务是否可用了 单位为毫秒
	// ErrorPercentThreshold: 错误百分比 请求数量大于等于 RequestVolumeThreshold 并且错误率到达这个百分比后就会启动熔断
	Hystrix map[string]*hystrix.CommandConfig `json:"hystrix,omitempty"` // 熔断器 [path:Config]，path 支持?*通配符 不区分大小写 目前不支持动态删除
}

var ParamConf loader.JsonLoader[ParamConfig]

func (c *ParamConfig) Create() {
	c.LogLevel = 1 // 外层配置比较好控制 这里默认info级别
	c.LogLevelByIP = map[string]int{}
	c.LogLevelByPath = map[string]int{}
	c.LogLevelHeadByIP = map[string]int{}
	c.LogLevelHeadByPath = map[string]int{}
	c.LogMerge = true
	c.LogMergeByIP = map[string]bool{}
	c.LogMergeByPath = map[string]bool{}
	c.TimeOutCheck = 8 // 8秒超时报警
	c.Cors = defaultCorsOptions
	c.BodyLogLimit = 1024
}

func (c *ParamConfig) Normalize() {
	for k, v := range c.LogLevelByIP {
		c.LogLevelByIP[strings.ToLower(k)] = v
	}
	for k, v := range c.LogLevelByPath {
		c.LogLevelByPath[strings.ToLower(k)] = v
	}
	for k, v := range c.LogLevelHeadByIP {
		c.LogLevelHeadByIP[strings.ToLower(k)] = v
	}
	for k, v := range c.LogLevelHeadByPath {
		c.LogLevelHeadByPath[strings.ToLower(k)] = v
	}
	for k, v := range c.LogMergeByIP {
		c.LogMergeByIP[strings.ToLower(k)] = v
	}
	for k, v := range c.LogMergeByPath {
		c.LogMergeByPath[strings.ToLower(k)] = v
	}
	for path, config := range c.Hystrix {
		path = strings.ToLower(path)
		c.Hystrix[path] = config
		hystrix.ConfigureCommand("gin_"+path, *config) // 加个gin_前缀，区别其他模块使用
	}
	c.Cors.Normalize()
}

func (c *ParamConfig) GetLogLevel(path, ip string) int {
	path = strings.ToLower(path)
	for k, v := range c.LogLevelByPath {
		if utils.IsMatch(k, path) {
			return v
		}
	}
	ip = strings.ToLower(ip)
	for k, v := range c.LogLevelByIP {
		if utils.IsMatch(k, ip) {
			return v
		}
	}
	return c.LogLevel
}

func (c *ParamConfig) GetLogLevelHead(path, ip string) int {
	path = strings.ToLower(path)
	for k, v := range c.LogLevelHeadByPath {
		if utils.IsMatch(k, path) {
			return v
		}
	}
	ip = strings.ToLower(ip)
	for k, v := range c.LogLevelHeadByIP {
		if utils.IsMatch(k, ip) {
			return v
		}
	}
	return c.LogLevelHead
}

func (c *ParamConfig) GetLogMerge(path, ip string) bool {
	path = strings.ToLower(path)
	for k, v := range c.LogMergeByPath {
		if utils.IsMatch(k, path) {
			return v
		}
	}
	ip = strings.ToLower(ip)
	for k, v := range c.LogMergeByIP {
		if utils.IsMatch(k, ip) {
			return v
		}
	}
	return c.LogMerge
}

func (c *ParamConfig) IsHystrixPath(path string) (string, bool) {
	v := strings.ToLower(path)
	for path := range c.Hystrix {
		if utils.IsMatch(path, v) {
			return "gin_" + path, true
		}
	}
	return "", false
}
