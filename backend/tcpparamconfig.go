package backend

// https://github.com/yuwf/gobase

import (
	"github.com/yuwf/gobase/loader"
)

// 参数配置
type TcpParamConfig struct {
	AsyncDispatch bool    `json:"asyncdispatch,omitempty"` // 非分组消息消息是否异步分发执行，见msger.接消息分发处理说明
	Immediately   bool    `json:"immediately,omitempty"`   // 立即模式 如果服务器发现逻辑服务器不存在了立刻删除服务对象，否则等socket失去连接后删除服务对象
	TickInterval  float32 `json:"tickinterval,omitempty"`  // 心跳间隔 单位秒 默认1秒
}

var TcpParamConf loader.JsonLoader[TcpParamConfig]

func (c *TcpParamConfig) Create() {
	c.TickInterval = 1.0
}
