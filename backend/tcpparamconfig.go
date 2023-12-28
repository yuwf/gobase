package backend

// https://github.com/yuwf/gobase

import "github.com/yuwf/gobase/loader"

// 参数配置
type TcpParamConfig struct {
	// SendMsg接口中，输出日志等级会按照下面的配置来执行，否则按照Debug输出
	// 日志级别和zerolog.Level一致
	LogLevelMsg   int            `json:"loglevelmsg,omitempty"`   // msg消息默认的消息级别，不配置就是debug级别
	LogLevelByMsg map[string]int `json:"loglevelbymsg,omitempty"` // 根据消息ID区分的消息日志级别，消息ID：日志级别，不配置就使用LogLevelMsg级别

	MsgSeq      bool `json:"msgseq,omitempty"`      // 消息顺序执行
	Immediately bool `json:"immediately,omitempty"` // 立即模式 如果服务器发现逻辑服务器不存在了立刻删除服务对象，否则等socket失去连接后删除服务对象
}

var TcpParamConf loader.JsonLoader[TcpParamConfig]

func (c *TcpParamConfig) Create() {
	c.LogLevelByMsg = map[string]int{}
	c.MsgSeq = true // 默认为按顺序执行
}

func (c *TcpParamConfig) MsgLogLevel(msgid string) int {
	loglevel, ok := c.LogLevelByMsg[msgid]
	if !ok {
		return c.LogLevelMsg
	}
	return loglevel
}
