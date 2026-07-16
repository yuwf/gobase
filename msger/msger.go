package msger

// https://github.com/yuwf/gobase

import (
	"github.com/yuwf/gobase/utils"
)

type Msger interface {
	MsgID() string               // 获取msgid
	MsgMarshal() ([]byte, error) // 编码整个消息，返回的[]byte用来发送数据
}

/* 消息分发处理说明
消息处理有两种方式：
  1：OnMsg
  2：注册的消息走分发，分发支持配置AsyncDispatch来表示是否异步分发
调用消息处理的协程有两种模型：
  1：分组    每个组是一个协程，组内消息按顺序调用OnMsg或者分发（组内消息不支持异步分发，即忽略AsyncDispatch配置，一定是顺序处理）
  2：不分组  共同走一个协程，如果配置AsyncDispatch，消息分发逻辑会脱离这个协程
*/

type RecvMsger interface {
	Msger

	RPCId() interface{}                  // 如果是RPC返回消息，返回RCPID，nil表示非RPC返回消息
	GroupId() interface{}                // 返回分组ID，nil表示不分组，见消息分发处理说明
	TraceId() int64                      // 如果消息带着TraceID，返回TraceID，否则返回0，用于日志跟踪
	BodyUnMarshal(dst interface{}) error // 根据具体消息类型，解析出消息体到dst
}

// 可选择实现，有些底层会根据这个名字适配一些功能，如日志输出
type MsgerName interface {
	MsgName() string // 获取消息名
}

type LogLevel struct {
	// 日志级别和zerolog.Level一致 0是debug级别 7禁用
	// 消息ID：日志级别，不配置就使用Default级别，支持?*通配符 区分大小
	Default int `json:"default,omitempty"`

	MsgByID   map[string]int `json:"msgbyid,omitempty"`
	MsgByName map[string]int `json:"msgbyname,omitempty"` // 消息需要实现MsgNameer接口
}

func (m *LogLevel) MsgLevel(msg Msger) int {
	msgid := msg.MsgID()
	msgname := ""
	if mner, _ := any(msg).(MsgerName); mner != nil {
		msgname = mner.MsgName()
	}
	// 先直接全匹配
	if len(msgname) > 0 {
		if loglevel, ok := m.MsgByName[msgname]; ok {
			return loglevel
		}
	}
	if loglevel, ok := m.MsgByID[msgname]; ok {
		return loglevel
	}
	// 匹配
	if len(msgname) > 0 {
		if loglevel, ok := m.MsgByName[msgname]; ok {
			return loglevel
		}
		for pattern, loglevel := range m.MsgByName {
			if utils.IsMatch(pattern, msgname) {
				return loglevel
			}
		}
	}
	for pattern, loglevel := range m.MsgByID {
		if utils.IsMatch(pattern, msgid) {
			return loglevel
		}
	}
	return m.Default
}
