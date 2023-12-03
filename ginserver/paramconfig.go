package ginserver

// https://github.com/yuwf/gobase

import (
	"strings"

	"gobase/loader"
	"gobase/utils"

	"github.com/afex/hystrix-go/hystrix"
)

// 参数配置
type ParamConfig struct {
	IgnoreIP     []string    `json:"ignoreip,omitempty"`   // 日志忽略的IP 支持?*通配符 不区分大小写
	IgnorePath   []string    `json:"ignorepath,omitempty"` // 日志忽略的Path 支持?*通配符 不区分大小写
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
	c.Cors = defaultCorsOptions
	c.BodyLogLimit = 1024
}

func (c *ParamConfig) Normalize() {
	for i := 0; i < len(c.IgnoreIP); i++ {
		c.IgnoreIP[i] = strings.ToLower(c.IgnoreIP[i])
	}
	for i := 0; i < len(c.IgnorePath); i++ {
		c.IgnorePath[i] = strings.ToLower(c.IgnorePath[i])
	}
	for path, config := range c.Hystrix {
		path = strings.ToLower(path)
		c.Hystrix[path] = config
		hystrix.ConfigureCommand("gin_"+path, *config) // 加个gin_前缀，区别其他模块使用
	}
	c.Cors.Normalize()
}

func (c *ParamConfig) IsIgnoreIP(ip string) bool {
	v := strings.ToLower(ip)
	for _, o := range c.IgnoreIP {
		if utils.IsMatch(o, v) {
			return true
		}
	}
	return false
}

func (c *ParamConfig) IsIgnorePath(path string) bool {
	v := strings.ToLower(path)
	for _, o := range c.IgnorePath {
		if utils.IsMatch(o, v) {
			return true
		}
	}
	return false
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
