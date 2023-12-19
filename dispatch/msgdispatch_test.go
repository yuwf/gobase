package dispatch

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/yuwf/gobase/utils"
)

type Client[T any] struct {
	name T
}

func (c *Client[T]) SendMsg(msg interface{}) error {
	m, _ := msg.(*utils.TestMsg)
	log.Info().Interface("Head", m.Head()).Interface("Body", m.BodyMsg).Msg("SendMsg")
	return nil
}

type Server struct {
	MsgDispatch[utils.TestMsg, Client[string]]
}

func (h *Server) onTestHeatBeatReq(ctx context.Context, m *utils.TestMsg, msg *utils.TestHeatBeatReq, t *Client[string]) {
}

func (h *Server) onTestHeatBeatResp(ctx context.Context, m *utils.TestMsg, msg *utils.TestHeatBeatResp, t *Client[string]) {

}

func (h *Server) onRPC(ctx context.Context, req *utils.TestHeatBeatReq, resp *utils.TestHeatBeatResp, t *Client[string]) {
	time.Sleep(time.Second * 10)
}

func BenchmarkRegister(b *testing.B) {
	s := &Server{}
	s.RegMsgID = utils.TestRegMsgID
	ParamConf.Get().TimeOutCheck = 2

	s.RegMsg(s.onTestHeatBeatReq)
	//s.RegMsg(s.onTestHeatBeatResp)

	// RPC 会超时
	//s.RegReqResp(s.onRPC)

	// 发送一个消息
	t := &Client[string]{}
	s.Dispatch(context.TODO(), utils.TestHeatBeatReqMsg, t)

	s.WaitAllMsgDone(time.Second * 30)
}
