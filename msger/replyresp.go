package msger

// https://github.com/yuwf/gobase

import (
	"context"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

type ReplyResper interface {
	respType() reflect.Type
	create(md *MsgDispatch, ctx context.Context, mr Msger, reqid, respid string, msg interface{}, t interface{}, checkMsgDone chan int)
}

type ReplyResp[Resp any] struct {
	Resp *Resp

	// 内部使用
	md           *MsgDispatch
	ctx          context.Context
	reqid        string
	respId       string
	mr           Msger
	t            interface{}
	checkMsgDone chan int

	entry time.Time // 创建时间
	reply int32     // 是否回复了 原子操作

	// 回复钩子函数，在Reply()时调用，正常是只有一个线程处理消息，所以这里不需要锁
	replyHook []func(ctx context.Context, reply *ReplyResp[Resp])
}

func (reply *ReplyResp[Resp]) Reply() {
	if !atomic.CompareAndSwapInt32(&reply.reply, 0, 1) {
		return
	}

	// 调用回复钩子函数
	func() {
		defer utils.HandlePanic()
		for _, hook := range reply.replyHook {
			hook(reply.ctx, reply)
		}
	}()

	elapsed := time.Since(reply.entry)

	// 回复完成后，关闭检查消息超时的channel
	if reply.checkMsgDone != nil {
		close(reply.checkMsgDone)
		reply.checkMsgDone = nil
	}

	if reply.md.sendRespValue.IsValid() {
		reply.md.sendRespValue.Call([]reflect.Value{reflect.ValueOf(reply.ctx), reflect.ValueOf(reply.mr), reflect.ValueOf(reply.t), reflect.ValueOf(reply.respId), reflect.ValueOf(reply.Resp)})
	} else {
		utils.LogCtx(log.Error(), reply.ctx).Str("ReqID", reply.reqid).Str("RespID", reply.respId).Interface("Resp", reply).Msg("MsgDispatch Dispatch SendResp is nil")
	}

	reply.md.callhook(reply.ctx, reply.mr, elapsed)
}

// 注册回复钩子函数
func (reply *ReplyResp[Resp]) RegHook(hook func(ctx context.Context, reply *ReplyResp[Resp])) {
	reply.replyHook = append(reply.replyHook, hook)
}

func (reply *ReplyResp[Resp]) respType() reflect.Type {
	return reflect.TypeOf((*Resp)(nil)).Elem()
}

func (reply *ReplyResp[Resp]) create(md *MsgDispatch, ctx context.Context, mr Msger, reqid, respid string, msg interface{}, t interface{}, checkMsgDone chan int) {
	reply.Resp = new(Resp)
	reply.md = md
	reply.ctx = ctx
	reply.reqid = reqid
	reply.respId = respid
	reply.mr = mr
	reply.t = t
	reply.checkMsgDone = checkMsgDone

	reply.reply = 0
	reply.entry = time.Now()
}
