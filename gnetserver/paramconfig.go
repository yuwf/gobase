package gnetserver

// https://github.com/yuwf/gobase

import (
	"strings"

	"github.com/yuwf/gobase/loader"
	"github.com/yuwf/gobase/utils"
)

const CtxKey_WS = utils.CtxKey("ws")     // 表示ws连接 值：不受限制 一般写1
const CtxKey_Text = utils.CtxKey("text") // 数据为text格式，否则为二进制格式 值：不受限制 一般写1

// 参数配置
type ParamConfig struct {
	IgnoreIp []string `json:"ignoreip,omitempty"` // log输出忽略的ip 支持?*通配符 不区分大小写
	// SendMsg接口中，参数msg实现了MsgIDer接口，输出日志等级会按照下面的配置来执行，否则按照Debug输出
	// 日志级别和zerolog.Level一致
	LogLevelMsg   int                 `json:"loglevelmsg,omitempty"`   // msg消息默认的消息级别，不配置就是debug级别
	LogLevelByMsg map[string]int      `json:"loglevelbymsg,omitempty"` // 根据消息ID区分的消息日志级别，消息ID：日志级别，不配置就使用LogLevelMsg级别
	ActiveTimeout int                 `json:"activetimeout,omitempty"` // 连接活跃超时时间 单位秒 <=0表示不检查活跃
	MsgSeq        bool                `json:"msgseq,omitempty"`        // 消息顺序执行
	WSHeader      map[string][]string `json:"wsheader,omitempty"`      // websocket握手时 回复的头
}

var ParamConf loader.JsonLoader[ParamConfig]

func (c *ParamConfig) Create() {
	c.LogLevelByMsg = map[string]int{}
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

func (c *ParamConfig) MsgLogLevel(msgid string) int {
	loglevel, ok := c.LogLevelByMsg[msgid]
	if !ok {
		return c.LogLevelMsg
	}
	return loglevel
}
