package msger

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	sync "sync"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// MsgDispatch 提供消息注册和分发功能
type MsgDispatch struct {
	handlers sync.Map // 处理函数  [msgid:*msgHandler]

	mType reflect.Type // Msg的类型
	tType reflect.Type // Termianl的类型
	// 如果使用RegReqResp注册必须设置，用来发送Resp消息
	sendRespValue reflect.Value // 处理函数 func(ctx context.Context, req *Msg, t *Termianl, respid string, resp interface{})

	wg sync.WaitGroup // 用于所有消息的处理完毕等待

	// 请求处理完后回调 不使用锁，默认要求提前注册好
	hook []func(ctx context.Context, mr Msger, elapsed time.Duration)
}

// Msg表示用来透传的消息类型，必须实现Msger接口，否则无法分发
// Termianl表示用来透传的终端对象，一般为连接对象

func NewMsgDispatch[Msg any, Termianl any]() (*MsgDispatch, error) {
	var zero Msg
	if _, ok := any(&zero).(Msger); !ok {
		return nil, fmt.Errorf("type %T does not implement Msger interface", zero)
	}
	md := &MsgDispatch{
		mType: reflect.TypeOf((*Msg)(nil)),
		tType: reflect.TypeOf((*Termianl)(nil)),
	}
	return md, nil
}

// 如果使用RegReqResp注册必须设置SendResp参数，用来发送Resp消息
// 函数样式(ctx context.Context, req *Msg, t *Termianl, respid string, resp interface{})
func (md *MsgDispatch) SendResp(fun interface{}) error {
	// 获取函数类型
	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	// 必须是函数
	if funType.Kind() != reflect.Func {
		err := errors.New("param must be function")
		log.Error().Err(err).Str("type", funType.String()).Msg("MsgDispatch SendResp error")
		return err
	}
	funName, _ := getFuncName(funValue)

	// 必须有五个参数
	paramNum := funType.NumIn()
	if paramNum != 5 {
		err := errors.New("param num must be 5")
		log.Error().Err(err).Str("Func", funName).Msg("MsgDispatch SendResp error")
		return err
	}
	// 第一个参数必须是context.Context
	if funType.In(0).String() != "context.Context" {
		err := errors.New("the first param must be context.Context")
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(0).String()).Msg("MsgDispatch SendResp error")
		return err
	}
	// 第二个参数必须是*Msg
	if funType.In(1) != md.mType {
		err := errors.New("the second param must be " + md.tType.String())
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(1).String()).Msg("MsgDispatch SendResp error")
		return err
	}
	// 第三个参数必须是*Termianl
	if funType.In(2) != md.tType {
		err := errors.New("the third param must be " + md.mType.String())
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(2).String()).Msg("MsgDispatch SendResp error")
		return err
	}
	// 第四个参数必须是string
	if funType.In(3).Kind() != reflect.String {
		err := errors.New("the fourth param must be string")
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(3).String()).Msg("MsgDispatch SendResp error")
		return err
	}
	// 第五个参数必须是string
	if funType.In(4).Kind() != reflect.Interface {
		err := errors.New("the fifth param must be interface{}")
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(4).String()).Msg("MsgDispatch SendResp error")
		return err
	}
	md.sendRespValue = funValue
	return nil
}

// 注册整个结构，遍历大写开头的函数，符合函数规范的自动注册
// (ctx context.Context, msg *具体消息, t *Termianl)
// (ctx context.Context, m *Msg, msg *具体消息, t *Termianl)
// (ctx context.Context, req *具体消息, resp *具体消息, t *Termianl)
// (ctx context.Context, m *Msg, req *具体消息, resp *具体消息, t *Termianl)
func (md *MsgDispatch) Reg(v interface{}, regMsgID func(msgType reflect.Type) string) {
	vType := reflect.TypeOf(v)
	if vType.Kind() != reflect.Pointer || vType.Elem().Kind() != reflect.Struct {
		log.Error().Str("type", vType.String()).Msg("MsgDispatch Reg param must be Struct Pointer")
		return
	}
	if regMsgID == nil {
		log.Error().Msg("MsgDispatch Reg regMsgID is nil")
		return
	}
	vValue := reflect.ValueOf(v)
	for i := 0; i < vValue.NumMethod(); i++ {
		// 如果是函数类型
		funValue := vValue.Method(i)
		funType := funValue.Type()
		paramNum := funType.NumIn()
		// 函数参数大于3个
		if paramNum < 3 {
			continue
		}
		// 如果第一个参数是context.Context
		if funType.In(0).String() != "context.Context" {
			continue
		}
		// 倒数第二个参数是具体消息类型指针
		if funType.In(paramNum-2).Kind() != reflect.Ptr || funType.In(paramNum-2).Elem().Kind() != reflect.Struct {
			continue
		}
		// 最后一个参数是*Cr
		if funType.In(paramNum-1) != md.tType {
			continue
		}
		funName := vType.Method(i).Name // 名字获取的方式和RegMsg不太一样
		funNameShort := funName
		var handler *MsgHandler
		if paramNum == 3 {
			handler = &MsgHandler{
				RegType:      RegType_Msg3,
				FunValue:     funValue,
				FunName:      funName,
				FunNameShort: funNameShort,
				MsgType:      funType.In(1).Elem(),
			}
		} else if paramNum == 4 {
			// 如果第二个参数是Mr
			if funType.In(1) == md.mType {
				handler = &MsgHandler{
					RegType:      RegType_Msg4,
					FunValue:     funValue,
					FunName:      funName,
					FunNameShort: funNameShort,
					MsgType:      funType.In(2).Elem(),
				}
			} else if funType.In(1).Kind() == reflect.Ptr && funType.In(1).Elem().Kind() == reflect.Struct {
				handler = &MsgHandler{
					RegType:      RegType_ReqResp4,
					FunValue:     funValue,
					FunName:      funName,
					FunNameShort: funNameShort,
					MsgType:      funType.In(1).Elem(),
					RespType:     funType.In(2).Elem(),
				}
			} else {
				continue
			}
		} else if paramNum == 5 {
			// 第二个参数必须是Mr
			if funType.In(1) != md.mType {
				continue
			}
			// 第三个参数是具体消息类型指针
			if funType.In(2).Kind() != reflect.Ptr || funType.In(2).Elem().Kind() != reflect.Struct {
				continue
			}
			handler = &MsgHandler{
				RegType:      RegType_ReqResp5,
				FunValue:     funValue,
				FunName:      funName,
				FunNameShort: funNameShort,
				MsgType:      funType.In(2).Elem(),
				RespType:     funType.In(3).Elem(),
			}
		} else {
			continue
		}

		// 获取下他的msgid
		msgid := regMsgID(handler.MsgType)
		if msgid == "" {
			continue
		}

		// 保存
		old, ok := md.handlers.Load(msgid)
		if ok {
			err := errors.New("already exist")
			log.Error().Err(err).Str("Exist", old.(*MsgHandler).FunName).Str("Func", funName).Str("MsgID", msgid).Msg("MsgDispatch Reg error")
			continue
		}
		md.handlers.Store(msgid, handler)
		log.Debug().Str("Func", funName).Str("MsgID", msgid).Msg("MsgDispatch Reg")
	}
}

// 注册消息处理函数
// fun的参数支持以下写法
// (ctx context.Context, msg *具体消息, t *Termianl)
// (ctx context.Context, m *Msg, msg *具体消息, t *Termianl)
func (md *MsgDispatch) RegMsg(msgid string, fun interface{}) error {
	// 获取函数类型
	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	// 必须是函数
	if funType.Kind() != reflect.Func {
		err := errors.New("param must be function")
		log.Error().Err(err).Str("type", funType.String()).Msg("MsgDispatch RegMsg error")
		return err
	}
	funName, funNameShort := getFuncName(funValue)

	// 必须有三个或者四个参数
	paramNum := funType.NumIn()
	if paramNum != 3 && paramNum != 4 {
		err := errors.New("param num must be 3 or 4")
		log.Error().Err(err).Str("Func", funName).Msg("MsgDispatch RegMsg error")
		return err
	}
	// 第一个参数必须是context.Context
	if funType.In(0).String() != "context.Context" {
		err := errors.New("the first param must be context.Context")
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(0).String()).Msg("MsgDispatch RegMsg error")
		return err
	}
	if paramNum == 4 {
		// 第二个参数必须是*Msg
		if funType.In(1) != md.mType {
			err := errors.New("the second param must be " + md.tType.String())
			log.Error().Err(err).Str("Func", funName).Str("type", funType.In(1).String()).Msg("MsgDispatch RegMsg error")
			return err
		}
	}
	// 倒数第二个参数是具体消息类型指针
	if funType.In(paramNum-2).Kind() != reflect.Ptr || funType.In(paramNum-2).Elem().Kind() != reflect.Struct {
		err := errors.New("the " + ordinalName[paramNum-2] + " param must be Struct Pointer")
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(paramNum-2).String()).Msg("MsgDispatch RegMsg error")
		return err
	}
	// 最后一个参数必须是*Termianl
	if funType.In(paramNum-1) != md.tType {
		err := errors.New("the " + ordinalName[paramNum-1] + " param must be " + md.mType.String())
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(paramNum-1).String()).Msg("MsgDispatch RegMsg error")
		return err
	}

	var handler *MsgHandler
	if paramNum == 3 {
		handler = &MsgHandler{
			RegType:      RegType_Msg3,
			FunValue:     funValue,
			FunName:      funName,
			FunNameShort: funNameShort,
			MsgType:      funType.In(1).Elem(),
		}
	} else {
		handler = &MsgHandler{
			RegType:      RegType_Msg4,
			FunValue:     funValue,
			FunName:      funName,
			FunNameShort: funNameShort,
			MsgType:      funType.In(2).Elem(),
		}
	}

	// 检查下消息id
	if msgid == "" {
		err := errors.New("msgid is nil")
		log.Error().Err(err).Str("Func", funName).Str("type", handler.MsgType.String()).Msg("MsgDispatch RegMsg error")
		return err
	}

	// 保存
	old, ok := md.handlers.Load(msgid)
	if ok {
		err := errors.New("already exist")
		log.Error().Err(err).Str("Exist", old.(*MsgHandler).FunName).Str("Func", funName).Str("MsgID", msgid).Msg("MsgDispatch RegMsg error")
		return err
	}
	md.handlers.Store(msgid, handler)
	log.Debug().Str("Func", funName).Str("MsgID", msgid).Msg("MsgDispatch RegMsg")
	return nil
}

// 注册请求响应消息处理函数，处理函数执行后，此函数会自动发送响应消息
// fun的参数支持以下写法
// (ctx context.Context, req *具体消息, resp *具体消息, t *Termianl)
// (ctx context.Context, m *MSg, req *具体消息, resp *具体消息, t *Termianl)
func (md *MsgDispatch) RegReqResp(reqid, respid string, fun interface{}) error {
	// 获取函数类型和函数名
	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	// 必须是函数
	if funType.Kind() != reflect.Func {
		err := errors.New("param must be function")
		log.Error().Err(err).Str("type", funType.String()).Msg("MsgDispatch RegReqResp error")
		return err
	}
	funName, funNameShort := getFuncName(funValue)

	// 必须有四个参数
	paramNum := funType.NumIn()
	if paramNum != 4 && paramNum != 5 {
		err := errors.New("param num must be 4 or 5")
		log.Error().Err(err).Str("Func", funName).Msg("MsgDispatch RegReqResp error")
		return err
	}
	// 第一个参数必须是context.Context
	if funType.In(0).String() != "context.Context" {
		err := errors.New("the first param must be context.Context")
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(0).String()).Msg("MsgDispatch RegReqResp error")
		return err
	}
	if paramNum == 5 {
		// 第二个参数必须是*Msg
		if funType.In(1) != md.mType {
			err := errors.New("the second param must be " + md.tType.String())
			log.Error().Err(err).Str("Func", funName).Str("type", funType.In(1).String()).Msg("MsgDispatch RegReqResp error")
			return err
		}
	}
	// 倒数第三个参数是具体消息类型指针
	if funType.In(paramNum-3).Kind() != reflect.Ptr || funType.In(paramNum-3).Elem().Kind() != reflect.Struct {
		err := errors.New("the " + ordinalName[paramNum-3] + " param must be Struct Pointer")
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(paramNum-3).String()).Msg("MsgDispatch RegReqResp error")
		return err
	}
	// 倒数第二个参数是具体消息类型指针
	if funType.In(paramNum-2).Kind() != reflect.Ptr || funType.In(paramNum-2).Elem().Kind() != reflect.Struct {
		err := errors.New("the " + ordinalName[paramNum-2] + " param must be Struct Pointer")
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(paramNum-2).String()).Msg("MsgDispatch RegReqResp error")
		return err
	}
	// 最后一个参数必须是*Termianl
	if funType.In(paramNum-1) != md.tType {
		err := errors.New("the " + ordinalName[paramNum-1] + " param must be " + md.mType.String())
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(paramNum-1).String()).Msg("MsgDispatch RegReqResp error")
		return err
	}

	var handler *MsgHandler
	if paramNum == 4 {
		handler = &MsgHandler{
			RegType:      RegType_ReqResp4,
			FunValue:     funValue,
			FunName:      funName,
			FunNameShort: funNameShort,
			MsgType:      funType.In(1).Elem(),
			RespType:     funType.In(2).Elem(),
		}
	} else {
		handler = &MsgHandler{
			RegType:      RegType_ReqResp5,
			FunValue:     funValue,
			FunName:      funName,
			FunNameShort: funNameShort,
			MsgType:      funType.In(2).Elem(),
			RespType:     funType.In(3).Elem(),
		}
	}

	// 检查下消息id
	if reqid == "" {
		err := errors.New("reqid is nil")
		log.Error().Err(err).Str("Func", funName).Str("type", handler.MsgType.String()).Msg("MsgDispatch RegReqResp error")
		return err
	}
	if respid == "" {
		err := errors.New("respid is nil")
		log.Error().Err(err).Str("Func", funName).Str("type", handler.MsgType.String()).Msg("MsgDispatch RegReqResp error")
		return err
	}
	handler.RespId = respid

	// 保存
	old, ok := md.handlers.Load(reqid)
	if ok {
		err := errors.New("already exist")
		log.Error().Err(err).Str("Exist", old.(*MsgHandler).FunName).Str("Func", funName).Str("MsgID", reqid).Msg("MsgDispatch RegReqResp error")
		return err
	}
	md.handlers.Store(reqid, handler)
	log.Debug().Str("Func", funName).Msg("MsgDispatch RegReqResp")
	return nil
}

// 注册请求响应消息处理函数，处理函数需要显式调用Reply方法来回复消息
// fun的参数支持以下写法
// (ctx context.Context, req *具体消息, resp *ReplyResp[具体消息], t *Termianl)
// (ctx context.Context, m *MSg, req *具体消息, resp *ReplyResp[具体消息], t *Termianl)
func (md *MsgDispatch) RegReqReply(reqid, respid string, fun interface{}) error {
	// 获取函数类型和函数名
	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	// 必须是函数
	if funType.Kind() != reflect.Func {
		err := errors.New("param must be function")
		log.Error().Err(err).Str("type", funType.String()).Msg("MsgDispatch RegReqReply error")
		return err
	}
	funName, funNameShort := getFuncName(funValue)

	// 必须有四个参数
	paramNum := funType.NumIn()
	if paramNum != 4 && paramNum != 5 {
		err := errors.New("param num must be 4 or 5")
		log.Error().Err(err).Str("Func", funName).Msg("MsgDispatch RegReqReply error")
		return err
	}
	// 第一个参数必须是context.Context
	if funType.In(0).String() != "context.Context" {
		err := errors.New("the first param must be context.Context")
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(0).String()).Msg("MsgDispatch RegReqReply error")
		return err
	}
	if paramNum == 5 {
		// 第二个参数必须是*Msg
		if funType.In(1) != md.mType {
			err := errors.New("the second param must be " + md.tType.String())
			log.Error().Err(err).Str("Func", funName).Str("type", funType.In(1).String()).Msg("MsgDispatch RegReqReply error")
			return err
		}
	}
	// 倒数第三个参数是具体消息类型指针
	if funType.In(paramNum-3).Kind() != reflect.Ptr || funType.In(paramNum-3).Elem().Kind() != reflect.Struct {
		err := errors.New("the " + ordinalName[paramNum-3] + " param must be Struct Pointer")
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(paramNum-3).String()).Msg("MsgDispatch RegReqReply error")
		return err
	}
	// 倒数第二个参数是*ReplyResp[具体消息]
	if !strings.HasPrefix(funType.In(paramNum-2).String(), "*msger.ReplyResp") {
		err := errors.New("the " + ordinalName[paramNum-2] + " param must be *msger.ReplyResp")
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(paramNum-2).String()).Msg("MsgDispatch RegReqReply error")
		return err
	}
	reply := reflect.New(funType.In(paramNum - 2).Elem()).Interface()
	creater := reply.(ReplyResper)
	if creater == nil {
		err := errors.New("the " + ordinalName[paramNum-2] + " param must be ReplyResp]")
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(paramNum-2).String()).Msg("MsgDispatch RegReqReply error")
		return err
	}
	// 模板元素
	replyRespType := creater.respType()
	if replyRespType.Kind() != reflect.Struct {
		err := errors.New("the " + ordinalName[paramNum-2] + " param elem must be Struct")
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(paramNum-2).String()).Msg("MsgDispatch RegReqResp error")
		return err
	}

	// 最后一个参数必须是*Termianl
	if funType.In(paramNum-1) != md.tType {
		err := errors.New("the " + ordinalName[paramNum-1] + " param must be " + md.mType.String())
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(paramNum-1).String()).Msg("MsgDispatch RegReqReply error")
		return err
	}

	var handler *MsgHandler
	if paramNum == 4 {
		handler = &MsgHandler{
			RegType:      RegType_ReqReply4,
			FunValue:     funValue,
			FunName:      funName,
			FunNameShort: funNameShort,
			MsgType:      funType.In(1).Elem(),
			RespType:     funType.In(2).Elem(),
		}
	} else {
		handler = &MsgHandler{
			RegType:      RegType_ReqReply5,
			FunValue:     funValue,
			FunName:      funName,
			FunNameShort: funNameShort,
			MsgType:      funType.In(2).Elem(),
			RespType:     funType.In(3).Elem(),
		}
	}

	// 检查下消息id
	if reqid == "" {
		err := errors.New("reqid is nil")
		log.Error().Err(err).Str("Func", funName).Str("type", handler.MsgType.String()).Msg("MsgDispatch RegReqReply error")
		return err
	}
	if respid == "" {
		err := errors.New("respid is nil")
		log.Error().Err(err).Str("Func", funName).Str("type", handler.MsgType.String()).Msg("MsgDispatch RegReqReply error")
		return err
	}
	handler.RespId = respid

	// 保存
	old, ok := md.handlers.Load(reqid)
	if ok {
		err := errors.New("already exist")
		log.Error().Err(err).Str("Exist", old.(*MsgHandler).FunName).Str("Func", funName).Str("MsgID", reqid).Msg("MsgDispatch RegReqReply error")
		return err
	}
	md.handlers.Store(reqid, handler)
	log.Debug().Str("Func", funName).Msg("MsgDispatch RegReqReply")
	return nil
}

func (r *MsgDispatch) RegHook(f func(ctx context.Context, mr Msger, elapsed time.Duration)) {
	r.hook = append(r.hook, f)
}

// 注册的消息ID
func (md *MsgDispatch) RegMsgIds() []string {
	msgids := []string{}
	md.handlers.Range(func(key, value any) bool {
		msgids = append(msgids, key.(string))
		return true
	})
	return msgids
}

// 注册的handler，外层不得修改
func (md *MsgDispatch) RegMsgHandler(msgid string) *MsgHandler {
	if value, ok := md.handlers.Load(msgid); ok {
		return value.(*MsgHandler)
	}
	return nil
}

func (md *MsgDispatch) WaitAllMsgDone(timeout time.Duration) {
	ch := make(chan int)
	go func() {
		md.wg.Wait()
		close(ch)
	}()
	// 超时等待
	timer := time.NewTimer(timeout)
	select {
	case <-ch:
		if !timer.Stop() {
			select {
			case <-timer.C: // try to drain the channel
			default:
			}
		}
	case <-timer.C:
	}
}

// 消息分发
// logPrefix 日志前缀, 为空时默认值为"MsgDispatch"
func (md *MsgDispatch) Dispatch(ctx context.Context, mr RecvMsger, t interface{}, logPrefix string) (bool, error) {
	if len(logPrefix) == 0 {
		logPrefix = "MsgDispatch"
	}
	// 判断参数是否符合要求
	mrType := reflect.ValueOf(mr).Type()
	if reflect.ValueOf(mr).Type() != md.mType {
		err := errors.New("prame must be " + md.mType.String() + ", but is " + mrType.String())
		md.log(ctx, nil, mr, t, int(zerolog.ErrorLevel), logPrefix+" Param error, "+err.Error())
		return false, err
	}
	tType := reflect.ValueOf(t).Type()
	if tType != md.tType {
		err := errors.New("prame must be " + md.mType.String() + ", but is " + tType.String())
		md.log(ctx, nil, mr, t, int(zerolog.ErrorLevel), logPrefix+" Param error, "+err.Error())
		return false, err
	}

	msgid := mr.MsgID()
	value1, ok1 := md.handlers.Load(msgid)
	if ok1 {
		handler, _ := value1.(*MsgHandler)
		msg := reflect.New(handler.MsgType).Interface()
		err := mr.BodyUnMarshal(msg)
		if err == nil {
			md.handle(ctx, handler, mr, msgid, msg, t, logPrefix)
		} else {
			md.log(ctx, nil, mr, t, int(zerolog.ErrorLevel), logPrefix+" error, "+err.Error())
		}
		return true, err
	}
	return false, nil
}

func (md *MsgDispatch) handle(ctx context.Context, handler *MsgHandler, mr Msger, msgid string, msg interface{}, t interface{}, logPrefix string) {
	md.wg.Add(1)
	defer md.wg.Done()

	// 日志
	logLevel := ParamConf.Get().LogLevel.MsgLevel(mr)
	md.log(ctx, handler, mr, t, logLevel, logPrefix)

	// 消息函数调用
	entry := time.Now()
	resp := md.callFunc(ctx, handler, mr, msgid, msg, t)
	elapsed := time.Since(entry)

	// rpc回复
	if resp != nil {
		if md.sendRespValue.IsValid() {
			md.sendRespValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(mr), reflect.ValueOf(t), reflect.ValueOf(handler.RespId), reflect.ValueOf(resp)})
		} else {
			utils.LogCtx(log.Error(), ctx).Str("MsgID", msgid).Interface("Resp", resp).Msg("MsgDispatch Dispatch SendResp is nil")
		}
	}

	if handler.RegType == RegType_Msg3 || handler.RegType == RegType_Msg4 || handler.RegType == RegType_ReqResp4 || handler.RegType == RegType_ReqResp5 {
		md.callhook(ctx, mr, elapsed)
		// RegType_ReqReply4, RegType_ReqReply5 放到Reply中调用
	}
}

// 如果有Resp，返回Resp消息
func (md *MsgDispatch) callFunc(ctx context.Context, handler *MsgHandler, mr Msger, msgid string, msg interface{}, t interface{}) interface{} {
	defer utils.HandlePanic()

	// 消息超时检查
	var checkMsgDone chan int
	if ParamConf.Get().TimeOutCheck > 0 {
		// 消息处理超时监控逻辑
		checkMsgDone = make(chan int, 1)

		utils.Submit(func() {
			timer := time.NewTimer(time.Duration(ParamConf.Get().TimeOutCheck) * time.Second)
			select {
			case <-checkMsgDone:
				if !timer.Stop() {
					select {
					case <-timer.C: // try to drain the channel
					default:
					}
				}
			case <-timer.C:
				// 消息超时了
				md.log(ctx, handler, mr, t, int(zerolog.ErrorLevel), "MsgDispatch TimeOut")
			}
		})
	}

	switch handler.RegType {
	case RegType_Msg3:
		handler.FunValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(msg), reflect.ValueOf(t)})
		if checkMsgDone != nil {
			close(checkMsgDone)
		}

	case RegType_Msg4:
		handler.FunValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(mr), reflect.ValueOf(msg), reflect.ValueOf(t)})
		if checkMsgDone != nil {
			close(checkMsgDone)
		}

	case RegType_ReqResp4, RegType_ReqResp5:
		resp := reflect.New(handler.RespType).Interface()
		if handler.RegType == RegType_ReqResp4 {
			handler.FunValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(msg), reflect.ValueOf(resp), reflect.ValueOf(t)})
		} else {
			handler.FunValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(mr), reflect.ValueOf(msg), reflect.ValueOf(resp), reflect.ValueOf(t)})
		}
		if checkMsgDone != nil {
			close(checkMsgDone)
		}
		return resp

	case RegType_ReqReply4, RegType_ReqReply5:
		reply := reflect.New(handler.RespType).Interface()
		// 调用 create(ctx, md)
		reply.(ReplyResper).create(md, ctx, mr, msgid, handler.RespId, msg, t, checkMsgDone)
		if handler.RegType == RegType_ReqReply4 {
			handler.FunValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(msg), reflect.ValueOf(reply), reflect.ValueOf(t)})
		} else {
			handler.FunValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(mr), reflect.ValueOf(msg), reflect.ValueOf(reply), reflect.ValueOf(t)})
		}
		return nil // 不返回，外层调用reply的Reply方法
	}
	return nil
}

func (md *MsgDispatch) callhook(ctx context.Context, mr Msger, elapsed time.Duration) {
	defer utils.HandlePanic()
	// 回调
	func() {
		defer utils.HandlePanic()
		for _, f := range md.hook {
			f(ctx, mr, elapsed)
		}
	}()
}

func (md *MsgDispatch) log(ctx context.Context, handler *MsgHandler, mr Msger, t interface{}, logLevel int, logmsg string) {
	if logLevel < int(log.Logger.GetLevel()) {
		return
	}
	l := log.WithLevel(zerolog.Level(logLevel))
	if l == nil {
		return
	}
	l = utils.LogCtx(l, ctx)
	if handler != nil {
		if ParamConf.Get().RegFuncShort {
			l.Str("func", handler.FunNameShort)
		} else {
			l.Str("func", handler.FunName)
		}
	}
	l.Interface("msger", mr).Msg(logmsg)
}
