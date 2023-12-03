package httprequest

// https://github.com/yuwf/gobase

import (
	"strings"

	"gobase/loader"
	"gobase/utils"

	"github.com/afex/hystrix-go/hystrix"
)

const CtxKey_nolog = utils.CtxKey_nolog // 不打印日志，错误日志还会打印 值：不受限制 一般写1

// 参数配置
type ParamConfig struct {
	IgnoreHost   []string `json:"ignorehost,omitempty"`   // 日志忽略的Host 支持?*通配符 不区分大小写
	IgnorePath   []string `json:"ignorepath,omitempty"`   // 日志忽略的Path 支持?*通配符 不区分大小写
	BodyLogLimit int      `json:"bodyloglimit,omitempty"` // body日志限制 <=0 表示不限制
	// Timeout: 执行 command 的超时时间 单位为毫秒
	// MaxConcurrentRequests: 最大并发量
	// RequestVolumeThreshold: 一个统计窗口 10 秒内请求数量 达到这个请求数量后才去判断是否要开启熔断
	// SleepWindow: 熔断器被打开后 SleepWindow的时间就是控制过多久后去尝试服务是否可用了 单位为毫秒
	// ErrorPercentThreshold: 错误百分比 请求数量大于等于 RequestVolumeThreshold 并且错误率到达这个百分比后就会启动熔断
	Hystrix map[string]*hystrix.CommandConfig `json:"hystrix,omitempty"` // 熔断器 [url:Config]，path 支持?*通配符 不区分大小写 目前不支持动态删除
}

var ParamConf loader.JsonLoader[ParamConfig]

func (c *ParamConfig) Create() {
	c.BodyLogLimit = 1024
}

func (c *ParamConfig) Normalize() {
	for i := 0; i < len(c.IgnoreHost); i++ {
		c.IgnoreHost[i] = strings.ToLower(c.IgnoreHost[i])
	}
	for i := 0; i < len(c.IgnorePath); i++ {
		c.IgnorePath[i] = strings.ToLower(c.IgnorePath[i])
	}
	for url, config := range c.Hystrix {
		url = strings.ToLower(url)
		c.Hystrix[url] = config
		hystrix.ConfigureCommand("http_"+url, *config) // 加个http_前缀，区别其他模块使用
	}
}

func (c *ParamConfig) IsIgnoreHost(host string) bool {
	v := strings.ToLower(host)
	for _, o := range c.IgnoreHost {
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

func (c *ParamConfig) IsHystrixURL(url string) (string, bool) {
	v := strings.ToLower(url)
	for path := range c.Hystrix {
		if utils.IsMatch(path, v) {
			return "http_" + path, true
		}
	}
	return "", false
}
