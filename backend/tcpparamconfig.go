package backend

// https://github.com/yuwf/gobase

import (
	"github.com/yuwf/gobase/loader"
	"github.com/yuwf/gobase/utils"
)

// 参数配置
type TcpParamConfig struct {
	MsgLogLevel utils.MsgLogLevel `json:"msgloglevel,omitempty"` // 消息日志级别

	MsgSeq      bool `json:"msgseq,omitempty"`      // 消息顺序执行
	Immediately bool `json:"immediately,omitempty"` // 立即模式 如果服务器发现逻辑服务器不存在了立刻删除服务对象，否则等socket失去连接后删除服务对象
}

var TcpParamConf loader.JsonLoader[TcpParamConfig]

func (c *TcpParamConfig) Create() {
	c.MsgSeq = true // 默认为按顺序执行
}
