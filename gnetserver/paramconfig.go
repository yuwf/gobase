package gnetserver

// https://github.com/yuwf/gobase

import (
	"strings"

	"github.com/yuwf/gobase/loader"
	"github.com/yuwf/gobase/utils"
)

const CtxKey_WS = utils.CtxKey("ws")     // 存在表示ws连接 值：不受限制 一般写1
const CtxKey_Text = utils.CtxKey("text") // 存在表示数据为text格式，否则为二进制格式 值：不受限制 一般写1

// 参数配置
type ParamConfig struct {
	IgnoreIp      []string            `json:"ignoreip,omitempty"`      // 建立连接和失去连接时，log输出忽略的ip， 支持?*通配符 不区分大小写
	AsyncDispatch bool                `json:"asyncdispatch,omitempty"` // 非分组消息消息是否异步分发执行，见msger.接消息分发处理说明
	WSHeader      map[string][]string `json:"wsheader,omitempty"`      // websocket握手时 回复的头
}

var ParamConf loader.JsonLoader[ParamConfig]

func (c *ParamConfig) Create() {
}

func (c *ParamConfig) Normalize() {
	for i := 0; i < len(c.IgnoreIp); i++ {
		c.IgnoreIp[i] = strings.ToLower(c.IgnoreIp[i])
	}
}

func (c *ParamConfig) IsIgnoreIp(ip string) bool {
	v := strings.ToLower(ip)
	for _, o := range c.IgnoreIp {
		if utils.IsMatch(o, v) {
			return true
		}
	}
	return false
}
