package utils

// https://github.com/yuwf/gobase

import "reflect"

// 发送消息接口
type SendMsger interface {
	MsgID() string               // 获取msgid
	MsgMarshal() ([]byte, error) // 编码整个消息，返回的[]byte用来发送数据
}

// 接受消息接口，包括消息头和消息体结构
type RecvMsger interface {
	MsgID() string                                           // 获取msgid
	BodyUnMarshal(msgType reflect.Type) (interface{}, error) // 根据具体消息类型，解析出消息体
}
