package utils

// https://github.com/yuwf/gobase

import (
	"reflect"
)

// 可选择实现，有些底层会根据这个名字适配一些功能，如日志输出
type MsgNameer interface {
	MsgName() string // 获取消息名
}

// 发送消息接口
type SendMsger interface {
	MsgID() string               // 获取msgid
	MsgMarshal() ([]byte, error) // 编码整个消息，返回的[]byte用来发送数据
}

// 接受消息接口
type RecvMsger interface {
	MsgID() string                                           // 获取msgid
	BodyUnMarshal(msgType reflect.Type) (interface{}, error) // 根据具体消息类型，解析出消息体
}

type MsgLogLevel struct {
	// 日志级别和zerolog.Level一致 0是debug级别 7禁用
	// 消息ID：日志级别，不配置就使用Default级别，支持?*通配符 区分大小
	Default    int            `json:"default,omitempty"`
	ByID       map[string]int `json:"byid,omitempty"`
	ByName     map[string]int `json:"byname,omitempty"` // 消息需要实现MsgNameer接口
	SendByID   map[string]int `json:"sendbyid,omitempty"`
	SendByName map[string]int `json:"sendbyname,omitempty"`
	RecvByID   map[string]int `json:"recvbyid,omitempty"` // 消息需要实现MsgNameer接口
	RecvByName map[string]int `json:"recvbyname,omitempty"`
}

func (m *MsgLogLevel) SendLevel(msg SendMsger) int {
	msgid := msg.MsgID()
	msgname := ""
	mner, _ := any(msg).(MsgNameer)
	if mner != nil {
		msgname = mner.MsgName()
	}
	if len(msgname) > 0 {
		for pattern, loglevel := range m.SendByName {
			if IsMatch(pattern, msgname) {
				return loglevel
			}
		}
	}
	for pattern, loglevel := range m.SendByID {
		if IsMatch(pattern, msgid) {
			return loglevel
		}
	}
	if len(msgname) > 0 {
		for pattern, loglevel := range m.ByName {
			if IsMatch(pattern, msgname) {
				return loglevel
			}
		}
	}
	for pattern, loglevel := range m.ByID {
		if IsMatch(pattern, msgid) {
			return loglevel
		}
	}
	return m.Default
}

func (m *MsgLogLevel) RecvLevel(msg RecvMsger) int {
	msgid := msg.MsgID()
	msgname := ""
	mner, _ := any(msg).(MsgNameer)
	if mner != nil {
		msgname = mner.MsgName()
	}
	if len(msgname) > 0 {
		for pattern, loglevel := range m.RecvByName {
			if IsMatch(pattern, msgname) {
				return loglevel
			}
		}
	}
	for pattern, loglevel := range m.RecvByID {
		if IsMatch(pattern, msgid) {
			return loglevel
		}
	}
	if len(msgname) > 0 {
		for pattern, loglevel := range m.ByName {
			if IsMatch(pattern, msgname) {
				return loglevel
			}
		}
	}
	for pattern, loglevel := range m.ByID {
		if IsMatch(pattern, msgid) {
			return loglevel
		}
	}
	return m.Default
}
