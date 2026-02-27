package msger

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	_ "github.com/yuwf/gobase/log"

	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

type Client[T any] struct {
	name T
}

func (c *Client[T]) SendMsg(msg interface{}) error {
	//m, _ := msg.(*utils.TestMsg)
	log.Info().Interface("resp", msg).Msg("SendRespMsg")
	return nil
}

type Server struct {
	*MsgDispatch
}

func NewServer() *Server {
	s := &Server{}
	s.MsgDispatch, _ = NewMsgDispatch[utils.TestMsg, Client[string]]()
	s.SendResp(func(ctx context.Context, m *utils.TestMsg, c *Client[string], respid string, resp interface{}) {
		msg, _ := resp.(utils.TestMsgBody)
		sendMsg := &utils.TestMsg{
			TestMsgHead: utils.TestMsgHead{
				Msgid: msg.MsgID(),
			},
			SendMsg: msg,
		}
		c.SendMsg(sendMsg)
	})
	return s
}

func (h *Server) OnTestHeatBeat(ctx context.Context, m *utils.TestMsg, msg *utils.TestHeatBeatReq, t *Client[string]) {
	log.Info().Interface("msger", msg).Msg("全局注册")
}

func (h *Server) onTestHeatBeat(ctx context.Context, m *utils.TestMsg, msg *utils.TestHeatBeatReq, t *Client[string]) {
}

func (h *Server) onTestHeatBeatResp(ctx context.Context, req *utils.TestHeatBeatReq, resp *utils.TestHeatBeatResp, t *Client[string]) {
	//time.Sleep(time.Second * 10)
	resp.Data = "resp123"
}

func (h *Server) onTestHeatBeatResp2(ctx context.Context, req *utils.TestHeatBeatReq, resp *utils.TestHeatBeatResp, t *Client[string]) {
	time.Sleep(time.Second * 10)
	resp.Data = "resp123"
}

func (h *Server) onTestHeatBeatReply(ctx context.Context, req *utils.TestHeatBeatReq, reply *ReplyResp[utils.TestHeatBeatResp], t *Client[string]) {
	//time.Sleep(time.Second * 10)
	reply.Resp.Data = "resp123"
	reply.Reply()
}

func BenchmarkReg(b *testing.B) {
	s := NewServer()

	s.Reg(s, func(msgType reflect.Type) string {
		m, ok := reflect.New(msgType).Interface().(utils.TestMsgBody)
		if ok {
			return strconv.Itoa(int(m.MsgID()))
		}
		return ""
	})

	// 发送一个消息
	t := &Client[string]{}
	s.Dispatch(context.TODO(), utils.TestHeatBeatReqMsg, t, "")

	s.WaitAllMsgDone(time.Second * 30)
}

func BenchmarkRegMsg(b *testing.B) {
	s := NewServer()

	s.RegMsg(utils.TestHeatBeatReqMsg.MsgID(), s.onTestHeatBeat)

	// 发送一个消息
	t := &Client[string]{}
	s.Dispatch(context.TODO(), utils.TestHeatBeatReqMsg, t, "")

	s.WaitAllMsgDone(time.Second * 30)
}

func BenchmarkRegReqResp(b *testing.B) {
	s := NewServer()

	s.RegReqResp(utils.TestHeatBeatReqMsg.MsgID(), utils.TestHeatBeatRespMsg.MsgID(), s.onTestHeatBeatResp)

	// 发送一个消息
	t := &Client[string]{}
	s.Dispatch(context.TODO(), utils.TestHeatBeatReqMsg, t, "")

	s.WaitAllMsgDone(time.Second * 30)
}

func BenchmarkRegReqReply(b *testing.B) {
	s := NewServer()

	s.RegReqReply(utils.TestHeatBeatReqMsg.MsgID(), utils.TestHeatBeatRespMsg.MsgID(), s.onTestHeatBeatReply)

	// 发送一个消息
	t := &Client[string]{}
	s.Dispatch(context.TODO(), utils.TestHeatBeatReqMsg, t, "")

	s.WaitAllMsgDone(time.Second * 30)
}

func BenchmarkRegReqRespTimeOut(b *testing.B) {
	s := NewServer()
	ParamConf.Get().TimeOutCheck = 2

	s.RegReqResp(utils.TestHeatBeatReqMsg.MsgID(), utils.TestHeatBeatRespMsg.MsgID(), s.onTestHeatBeatResp2) // 会超时

	// 发送一个消息
	t := &Client[string]{}
	s.Dispatch(context.TODO(), utils.TestHeatBeatReqMsg, t, "")

	s.WaitAllMsgDone(time.Second * 30)
}
