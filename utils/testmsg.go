package utils

// https://github.com/yuwf/gobase

import (
	"encoding/binary"
	"errors"
	"reflect"
	"strconv"

	"github.com/rs/zerolog"
)

type TestMsgHead struct {
	Msgid uint32 `json:"msgid,omitempty"`
	Len   uint32 `json:"len,omitempty"`
}

type TestMsgBody interface {
	MsgID() uint32
	Marshal() ([]byte, error)
	UnMarshal([]byte) error
}

// 通用的消息测试使用
type TestMsg struct {
	TestMsgHead
	// 根据方向不一样，填充的字段不一样
	RecvData []byte      // 接受数据时填充
	SendMsg  TestMsgBody // 发送数据填充
}

func (tm *TestMsg) Head() interface{} {
	return &tm.TestMsgHead
}
func (tm *TestMsg) Body() interface{} {
	return tm.SendMsg
}
func (tm *TestMsg) MsgID() string {
	return strconv.Itoa(int(tm.Msgid))
}
func (tm *TestMsg) MsgMarshal() ([]byte, error) {
	buf, _ := tm.SendMsg.Marshal()
	data := make([]byte, 4+4+len(buf))
	binary.LittleEndian.PutUint32(data, tm.SendMsg.MsgID())
	binary.LittleEndian.PutUint32(data[4:], uint32(len(buf)))
	copy(data[8:], buf)
	return data, nil
}
func (tm *TestMsg) BodyData() []byte {
	return tm.RecvData
}
func (tm *TestMsg) BodyUnMarshal(msgType reflect.Type) (interface{}, error) {
	// 测试消息Data就是字符串 不用解析
	msg := reflect.New(msgType).Interface().(TestMsgBody)
	msg.UnMarshal(tm.RecvData)
	return msg, nil
}
func (tm *TestMsg) MarshalZerologObject(e *zerolog.Event) {
	e.Interface("Head", &tm.TestMsgHead)
	if tm.SendMsg != nil {
		e.Interface("Body", tm.SendMsg)
	}
	if tm.RecvData != nil {
		e.Interface("Data", string(tm.RecvData))
	}
}

type TestHeatBeatReq struct {
	Data string `json:"data,omitempty"` // 消息体就是一个字符串
}

func (m *TestHeatBeatReq) MsgID() uint32 {
	return 1
}
func (m *TestHeatBeatReq) Marshal() ([]byte, error) {
	return []byte(m.Data), nil
}
func (m *TestHeatBeatReq) UnMarshal(buf []byte) error {
	m.Data = string(buf)
	return nil
}

type TestHeatBeatResp struct {
	Data string `json:"data,omitempty"` // 消息体就是一个字符串
}

func (m *TestHeatBeatResp) MsgID() uint32 {
	return 2
}
func (m *TestHeatBeatResp) Marshal() ([]byte, error) {
	return []byte(m.Data), nil
}
func (m *TestHeatBeatResp) UnMarshal(buf []byte) error {
	m.Data = string(buf)
	return nil
}

// 根据二进制解码出TestMsg结构
func TestDecodeMsg(data []byte) (*TestMsg, int, error) {
	if len(data) >= 8 {
		msgid := binary.LittleEndian.Uint32(data)
		msglen := binary.LittleEndian.Uint32(data[4:])
		if msglen < 0 {
			return nil, 0, errors.New("Msg format error")
		}
		if msglen == 0 {
			m := &TestMsg{
				TestMsgHead: TestMsgHead{
					Msgid: msgid,
					Len:   msglen,
				},
			}
			return m, 8, nil
		}
		if len(data[4:]) >= int(msglen) {
			m := &TestMsg{
				TestMsgHead: TestMsgHead{
					Msgid: msgid,
					Len:   msglen,
				},
				RecvData: data[8 : 8+msglen],
			}
			return m, int(8 + msglen), nil
		} else {
			return nil, 0, nil
		}
	}
	return nil, 0, nil
}

var (
	TestHeatBeatReqMsg = &TestMsg{
		TestMsgHead: TestMsgHead{
			Msgid: 1,
		},
		SendMsg: &TestHeatBeatReq{Data: string("heatreqmsg")},
	}
	TestHeatBeatRespMsg = &TestMsg{
		TestMsgHead: TestMsgHead{
			Msgid: 2,
		},
		SendMsg: &TestHeatBeatResp{Data: string("heatrespmsg")},
	}

	TestRegMsgID = func(msgType reflect.Type) string {
		m, ok := reflect.New(msgType).Interface().(TestMsgBody)
		if ok {
			return strconv.Itoa(int(m.MsgID()))
		}
		return ""
	}
)
