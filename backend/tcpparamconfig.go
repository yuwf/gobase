package backend

// https://github.com/yuwf/gobase

import "github.com/yuwf/gobase/loader"

// 参数配置
type TcpParamConfig struct {
	MsgSeq      bool `json:"msgseq,omitempty"`      // 消息顺序执行
	Immediately bool `json:"immediately,omitempty"` // 立即模式 如果服务器发现逻辑服务器不存在了立刻关闭socket
}

var TcpParamConf loader.JsonLoader[TcpParamConfig]

func (c *TcpParamConfig) Create() {
	c.MsgSeq = true // 默认为按顺序执行
}
