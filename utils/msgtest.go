package utils

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"

	"github.com/rs/zerolog/log"
)

// 通用的消息测试使用
type TestMsg struct {
	Msgid uint32 `json:"msgid,omitempty"`
	Len   uint32 `json:"len,omitempty"`
	Data  string `json:"data,omitempty"`
}

var TestHeatBeatReqMsg = &TestMsg{
	Msgid: 1,
	Len:   10,
	Data:  string("heatreqmsg"),
}

var TestHeatBeatRespMsg = &TestMsg{
	Msgid: 2,
	Len:   10,
	Data:  string("heatrespmsg"),
}

func TestEncodeMsg(msg interface{}) ([]byte, error) {
	m, _ := msg.(*TestMsg)
	m.Len = uint32(len(m.Data))
	data := make([]byte, 4+4+len(m.Data))
	binary.LittleEndian.PutUint32(data, m.Msgid)
	binary.LittleEndian.PutUint32(data[4:], m.Len)
	copy(data[8:], m.Data)
	return data, nil
}

func TestDecodeMsg(data []byte) (interface{}, int, error) {
	if len(data) >= 8 {
		msgid := binary.LittleEndian.Uint32(data)
		msglen := binary.LittleEndian.Uint32(data[4:])
		if msglen < 0 {
			return nil, 0, errors.New("Msg format error")
		}
		if msglen == 0 {
			m := &TestMsg{
				Msgid: msgid,
				Len:   msglen,
			}
			return m, 8, nil
		}
		if len(data[4:]) >= int(msglen) {
			m := &TestMsg{
				Msgid: msgid,
				Len:   msglen,
				Data:  string(data[8 : 8+msglen]),
			}
			return m, int(8 + msglen), nil
		} else {
			return nil, 0, nil
		}
	}
	return nil, 0, nil
}

type TestMsgHandler struct {
	FunValue reflect.Value
	MsgType  reflect.Type
}

// 消息注册与分发
// T是自定义类型
type TestMsgRegister[T any] struct {
	handlers map[uint32]*TestMsgHandler
	tType    reflect.Type // T类型
}

type Ter interface {
	ConnName() string
}

// 处理消息的函数参数是(ctx context.Context, m *Msg, msg *消息结构, t *T)
func (r *TestMsgRegister[T]) RegMsgHandler(msgid uint32, fun interface{}) {
	if r.handlers == nil {
		r.handlers = map[uint32]*TestMsgHandler{}
		t := new(T)
		r.tType = reflect.TypeOf(t)
	}

	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	// 必须是函数
	if funType.Kind() != reflect.Func {
		log.Error().Msg("TestMsgRegister param must be function")
		return
	}
	// 必须有四个参数
	numPara := funType.NumIn()
	if numPara != 3 {
		log.Error().Msg("TestMsgRegister fun param num must be 3")
		return
	}
	// 第一个参数必须是context.Context
	if funType.In(0).String() != "context.Context" {
		log.Error().Str("type", funType.In(0).String()).Msg("TestMsgRegister first param num must be context.Context")
		return
	}
	// 第二个参数必须是消息类型，创建一个获取他的信息
	if funType.In(1).String() != "*utils.TestMsg" {
		log.Error().Str("type", funType.In(1).String()).Msg("TestMsgRegister second num must be *TestMsg")
		return
	}
	// 第三个参数必须是*T
	if funType.In(2) != r.tType {
		log.Error().Str("type", funType.In(3).String()).Str("musttype", r.tType.Name()).Msg("TestMsgRegister third param err")
		return
	}

	// 保存
	r.handlers[msgid] = &TestMsgHandler{
		FunValue: funValue,
		MsgType:  funType.In(1),
	}
}

// 处理函数调用，返回表示是否处理了
func (r *TestMsgRegister[T]) OnMsg(ctx context.Context, msg interface{}, t *T) bool {
	m, ok := msg.(*TestMsg)
	if !ok {
		return false // 理论上不会出现
	}
	handler, ok := r.handlers[m.Msgid]
	if ok {
		l := log.Debug()
		// 调用对象的ConnName函数，输入消息日志
		ter, ok := any(t).(Ter)
		if ok {
			l.Str("Name", ter.ConnName())
		}
		l.Interface("Msg", m).Msg("RecvMsg")
		handler.FunValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(m), reflect.ValueOf(t)})
		return true
	}
	return false
}
