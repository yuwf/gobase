package msger

// https://github.com/yuwf/gobase

import (
	"github.com/yuwf/gobase/utils"
)

type Msger interface {
	MsgID() string               // 获取msgid
	MsgMarshal() ([]byte, error) // 编码整个消息，返回的[]byte用来发送数据
}

type RecvMsger interface {
	Msger

	RPCId() interface{}                  // 如果是RPC返回消息，返回RCPID，nil表示非RPC返回消息
	GroupId() interface{}                // 如果消息分组处理，返回分组ID，设置为非顺序处理时，会根据分组ID分组处理，nil表示不分组
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
