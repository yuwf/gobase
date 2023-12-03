package gnetserver

// https://github.com/yuwf/gobase

import (
	"strings"

	"gobase/loader"
	"gobase/utils"
)

const CtxKey_WS = "ws"     // 表示ws连接 值：不受限制 一般写1
const CtxKey_Text = "text" // 数据为text格式，否则为二进制格式 值：不受限制 一般写1

// 参数配置
type ParamConfig struct {
	IgnoreIp      []string            `json:"ignoreip,omitempty"`      // log输出忽略的ip 支持?*通配符 不区分大小写
	ActiveTimeout int                 `json:"activetimeout,omitempty"` // 连接活跃超时时间 单位秒 <=0表示不检查活跃
	MsgSeq        bool                `json:"msgseq,omitempty"`        // 消息顺序执行
	WSHeader      map[string][]string `json:"wsheader,omitempty"`      // websocket握手时 回复的头
}

var ParamConf loader.JsonLoader[ParamConfig]

func (c *ParamConfig) Create() {
	c.MsgSeq = true // 默认为按顺序执行
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
