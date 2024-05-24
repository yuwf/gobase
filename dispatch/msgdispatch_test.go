package dispatch

import (
	"context"
	"testing"
	"time"

	_ "github.com/yuwf/gobase/log"

	"github.com/rs/zerolog/log"
	"github.com/yuwf/gobase/utils"
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
	MsgDispatch[*utils.TestMsg, Client[string]]
}

func (h *Server) OnTestHeatBeat(ctx context.Context, m *utils.TestMsg, msg *utils.TestHeatBeatReq, t *Client[string]) {
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

func BenchmarkReg(b *testing.B) {
	s := &Server{}
	s.RegMsgID = utils.TestRegMsgID
	//ParamConf.Get().RegFuncShort = true

	s.Reg(s)

	// 发送一个消息
	t := &Client[string]{}
	s.Dispatch(context.TODO(), utils.TestHeatBeatReqMsg, t)

	s.WaitAllMsgDone(time.Second * 30)
}

func BenchmarkRegMsg(b *testing.B) {
	s := &Server{}
	s.RegMsgID = utils.TestRegMsgID

	s.RegMsg(s.onTestHeatBeat)

	// 发送一个消息
	t := &Client[string]{}
	s.Dispatch(context.TODO(), utils.TestHeatBeatReqMsg, t)

	s.WaitAllMsgDone(time.Second * 30)
}

func BenchmarkRegReqResp(b *testing.B) {
	s := &Server{}
	s.RegMsgID = utils.TestRegMsgID
	s.SendResp = func(ctx context.Context, m *utils.TestMsg, c *Client[string], resp interface{}) {
		msg, _ := resp.(utils.TestMsgBody)
		sendMsg := &utils.TestMsg{
			TestMsgHead: utils.TestMsgHead{
				Msgid: msg.MsgID(),
			},
			BodyMsg: msg,
		}
		c.SendMsg(sendMsg)
	}

	s.RegReqResp(s.onTestHeatBeatResp)

	// 发送一个消息
	t := &Client[string]{}
	s.Dispatch(context.TODO(), utils.TestHeatBeatReqMsg, t)

	s.WaitAllMsgDone(time.Second * 30)
}

func BenchmarkRegReqRespTimeOut(b *testing.B) {
	s := &Server{}
	s.RegMsgID = utils.TestRegMsgID
	s.SendResp = func(ctx context.Context, m *utils.TestMsg, c *Client[string], resp interface{}) {
		msg, _ := resp.(utils.TestMsgBody)
		sendMsg := &utils.TestMsg{
			TestMsgHead: utils.TestMsgHead{
				Msgid: msg.MsgID(),
			},
			BodyMsg: msg,
		}
		c.SendMsg(sendMsg)
	}
	ParamConf.Get().TimeOutCheck = 2

	s.RegReqResp(s.onTestHeatBeatResp2) // 会超时

	// 发送一个消息
	t := &Client[string]{}
	s.Dispatch(context.TODO(), utils.TestHeatBeatReqMsg, t)

	s.WaitAllMsgDone(time.Second * 30)
}
