package utils

import (
	"encoding/binary"
	"errors"
	"reflect"
	"strconv"
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
	Body    []byte      // 接受数据时填充
	BodyMsg TestMsgBody // 发送数据填充
}

func (tm *TestMsg) Head() interface{} {
	return &tm.TestMsgHead
}
func (tm *TestMsg) MsgID() string {
	return strconv.Itoa(int(tm.Msgid))
}
func (tm *TestMsg) MsgUnMarshal(msgType reflect.Type) (interface{}, error) {
	// 测试消息Data就是字符串 不用解析
	msg := reflect.New(msgType).Interface().(TestMsgBody)
	msg.UnMarshal(tm.Body)
	return msg, nil
}
func (tm *TestMsg) Resp(resp interface{}) interface{} {
	msg, _ := resp.(TestMsgBody)
	return &TestMsg{
		TestMsgHead: TestMsgHead{
			Msgid: msg.MsgID(),
		},
		BodyMsg: msg,
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

// 根据要发送的消息编码出发送的二进制文件
func TestEncodeMsg(msg interface{}) ([]byte, error) {
	m, _ := msg.(*TestMsg)
	buf, _ := m.BodyMsg.Marshal()
	data := make([]byte, 4+4+len(buf))
	binary.LittleEndian.PutUint32(data, m.BodyMsg.MsgID())
	binary.LittleEndian.PutUint32(data[4:], uint32(len(buf)))
	copy(data[8:], buf)
	return data, nil
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
				Body: data[8 : 8+msglen],
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
		BodyMsg: &TestHeatBeatReq{Data: string("heatreqmsg")},
	}
	TestHeatBeatRespMsg = &TestMsg{
		TestMsgHead: TestMsgHead{
			Msgid: 2,
		},
		BodyMsg: &TestHeatBeatResp{Data: string("heatrespmsg")},
	}

	TestRegMsgID = func(msgType reflect.Type) string {
		m, ok := reflect.New(msgType).Interface().(TestMsgBody)
		if ok {
			return strconv.Itoa(int(m.MsgID()))
		}
		return ""
	}
)
