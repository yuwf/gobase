package dispatch

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	sync "sync"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	regType_Invalid = iota
	regType_Msg3
	regType_Msg4
	regType_ReqResp4
	regType_ReqResp5
)

var ordinalName = []string{"first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth", "tenth"}

type msgHandler struct {
	regType  int           // RegType类型
	funValue reflect.Value // 处理函数
	funName  string        // 处理函数名，用来输出日志，不稳定不要用来做逻辑
	msgType  reflect.Type  // 处理消息的类型
	respType reflect.Type  // 回复消息的类型
	respId   string        // 回复消息的id
}

// 透传对象接口
type ConnNameer interface {
	ConnName() string // 连接对象名字，用来输出日志
}

// MsgDispatch 提供消息注册和分发功能
// Mr表示用来透传的消息指针类型，必须实现utils.RecvMsger接口
// Cr表示用来透传的类型，一般为连接对象，可以选择实现ConnNameer接口，日志使用
type MsgDispatch[Mr utils.RecvMsger, Cr any] struct {
	//【需要外部来设置】
	// 必须设置，注册时获取具体消息的msgid
	RegMsgID func(msgType reflect.Type) string
	// 如果使用RegReqResp注册必须设置，用来发送Resp消息
	SendResp func(ctx context.Context, req Mr, c *Cr, respid string, resp interface{})

	handlers sync.Map // 处理函数  [msgid:*msgHandler]

	initOnce sync.Once
	mType    reflect.Type // Mr的类型
	cType    reflect.Type // Cr的类型

	wg sync.WaitGroup // 用于所有消息的处理完毕等待

	// 请求处理完后回调 不使用锁，默认要求提前注册好
	hook []func(ctx context.Context, msgid string, elapsed time.Duration)
}

func (md *MsgDispatch[Mr, Cr]) lazyInit() {
	md.initOnce.Do(func() {
		md.mType = reflect.TypeOf(new(Mr)).Elem()
		md.cType = reflect.TypeOf(new(Cr))
	})
}

// 注册整个结构，遍历大写开头的函数，符合函数规范的自动注册
// (ctx context.Context, msg *具体消息, c *Cr)
// (ctx context.Context, m *Mr, msg *具体消息, c *Cr)
// (ctx context.Context, req *具体消息, resp *具体消息, c *Cr)
// (ctx context.Context, m *Mr, req *具体消息, resp *具体消息, c *Cr)
func (md *MsgDispatch[Mr, Cr]) Reg(v interface{}) {
	md.lazyInit()

	vType := reflect.TypeOf(v)
	if vType.Kind() != reflect.Pointer || vType.Elem().Kind() != reflect.Struct {
		log.Error().Str("type", vType.String()).Msg("MsgDispatch Reg param must be Struct Pointer")
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
		if funType.In(paramNum-1) != md.cType {
			continue
		}
		funName := vType.Method(i).Name // 名字获取的方式和RegMsg不太一样
		if !ParamConf.Get().RegFuncShort {
			funName = vType.String() + "." + funName
		}
		var handler *msgHandler
		if paramNum == 3 {
			handler = &msgHandler{
				regType:  regType_Msg3,
				funValue: funValue,
				funName:  funName,
				msgType:  funType.In(1).Elem(),
			}
		} else if paramNum == 4 {
			// 如果第二个参数是Mr
			if funType.In(1) == md.mType {
				handler = &msgHandler{
					regType:  regType_Msg4,
					funValue: funValue,
					funName:  funName,
					msgType:  funType.In(2).Elem(),
				}
			} else if funType.In(1).Kind() == reflect.Ptr && funType.In(1).Elem().Kind() == reflect.Struct {
				handler = &msgHandler{
					regType:  regType_ReqResp4,
					funValue: funValue,
					funName:  funName,
					msgType:  funType.In(1).Elem(),
					respType: funType.In(2).Elem(),
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
			handler = &msgHandler{
				regType:  regType_ReqResp5,
				funValue: funValue,
				funName:  funName,
				msgType:  funType.In(2).Elem(),
				respType: funType.In(3).Elem(),
			}
		} else {
			continue
		}

		// 获取下他的msgid
		var msgid string
		if md.RegMsgID != nil {
			msgid = md.RegMsgID(handler.msgType)
		}
		if msgid == "" {
			continue
		}

		// 保存
		old, ok := md.handlers.Load(msgid)
		if ok {
			err := errors.New("already exist")
			log.Error().Err(err).Str("Exist", old.(*msgHandler).funName).Str("Func", funName).Str("MsgID", msgid).Msg("MsgDispatch Reg error")
			continue
		}
		md.handlers.Store(msgid, handler)
		log.Debug().Str("Func", funName).Str("MsgID", msgid).Msg("MsgDispatch Reg")
	}
}

// 获取函数名
func getFuncName(fun reflect.Value) string {
	funName := runtime.FuncForPC(fun.Pointer()).Name()
	var slice []string
	if ParamConf.Get().RegFuncShort {
		slice = strings.Split(funName, ".")
	} else {
		slice = strings.Split(funName, "/")
	}
	if len(slice) > 0 {
		funName = slice[len(slice)-1]
		funName = strings.Replace(funName, "-fm", "", -1)
	}
	return funName
}

// 注册消息处理函数
// fun的参数支持以下写法
// (ctx context.Context, msg *具体消息, c *Cr)
// (ctx context.Context, m *Mr, msg *具体消息, c *Cr)
func (md *MsgDispatch[Mr, Cr]) RegMsg(fun interface{}) error {
	return md.RegMsgI("", fun)
}
func (md *MsgDispatch[Mr, Cr]) RegMsgI(msgid string, fun interface{}) error {
	md.lazyInit()

	// 获取函数类型
	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	// 必须是函数
	if funType.Kind() != reflect.Func {
		err := errors.New("param must be function")
		log.Error().Err(err).Str("type", funType.String()).Msg("MsgDispatch RegMsg error")
		return err
	}
	funName := getFuncName(funValue)

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
		// 第二个参数必须是Mr
		if funType.In(1) != md.mType {
			err := errors.New("the second param must be " + md.cType.String())
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
	// 最后一个参数必须是*Cr
	if funType.In(paramNum-1) != md.cType {
		err := errors.New("the " + ordinalName[paramNum-1] + " param must be " + md.mType.String())
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(paramNum-1).String()).Msg("MsgDispatch RegMsg error")
		return err
	}

	var handler *msgHandler
	if paramNum == 3 {
		handler = &msgHandler{
			regType:  regType_Msg3,
			funValue: funValue,
			funName:  funName,
			msgType:  funType.In(1).Elem(),
		}
	} else {
		handler = &msgHandler{
			regType:  regType_Msg4,
			funValue: funValue,
			funName:  funName,
			msgType:  funType.In(2).Elem(),
		}
	}

	if msgid == "" && md.RegMsgID != nil {
		// 获取下他的msgid
		msgid = md.RegMsgID(handler.msgType)
	}
	if msgid == "" {
		err := errors.New("msgid is nil")
		log.Error().Err(err).Str("Func", funName).Str("type", handler.msgType.String()).Msg("MsgDispatch RegMsg error")
		return err
	}

	// 保存
	old, ok := md.handlers.Load(msgid)
	if ok {
		err := errors.New("already exist")
		log.Error().Err(err).Str("Exist", old.(*msgHandler).funName).Str("Func", funName).Str("MsgID", msgid).Msg("MsgDispatch RegMsg error")
		return err
	}
	md.handlers.Store(msgid, handler)
	log.Debug().Str("Func", funName).Str("MsgID", msgid).Msg("MsgDispatch RegMsg")
	return nil
}

// 处理消息的函数参数是
// fun的参数支持以下写法
// (ctx context.Context, req *具体消息, resp *具体消息, c *Cr)
// (ctx context.Context, m Mr, req *具体消息, resp *具体消息, c *Cr)
func (md *MsgDispatch[Mr, Cr]) RegReqResp(fun interface{}) error {
	return md.RegReqRespI("", "", fun)
}
func (md *MsgDispatch[Mr, Cr]) RegReqRespI(reqid, respid string, fun interface{}) error {
	md.lazyInit()
	// 获取函数类型和函数名
	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	// 必须是函数
	if funType.Kind() != reflect.Func {
		err := errors.New("param must be function")
		log.Error().Err(err).Str("type", funType.String()).Msg("MsgDispatch RegReqResp error")
		return err
	}
	funName := getFuncName(funValue)

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
		// 第二个参数必须是Mr
		if funType.In(1) != md.mType {
			err := errors.New("the second param must be " + md.cType.String())
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
	// 最后一个参数必须是*Cr
	if funType.In(paramNum-1) != md.cType {
		err := errors.New("the " + ordinalName[paramNum-1] + " param must be " + md.mType.String())
		log.Error().Err(err).Str("Func", funName).Str("type", funType.In(paramNum-1).String()).Msg("MsgDispatch RegReqResp error")
		return err
	}

	var handler *msgHandler
	if paramNum == 4 {
		handler = &msgHandler{
			regType:  regType_ReqResp4,
			funValue: funValue,
			funName:  funName,
			msgType:  funType.In(1).Elem(),
			respType: funType.In(2).Elem(),
		}
	} else {
		handler = &msgHandler{
			regType:  regType_ReqResp5,
			funValue: funValue,
			funName:  funName,
			msgType:  funType.In(2).Elem(),
			respType: funType.In(3).Elem(),
		}
	}

	// 获取下他的reqid
	if reqid == "" && md.RegMsgID != nil {
		// 获取下他的msgid
		reqid = md.RegMsgID(handler.msgType)
	}
	if reqid == "" {
		err := errors.New("reqid is nil")
		log.Error().Err(err).Str("Func", funName).Str("type", handler.msgType.String()).Msg("MsgDispatch RegReqResp error")
		return err
	}

	if respid == "" && md.RegMsgID != nil {
		// 获取下他的msgid
		respid = md.RegMsgID(handler.respType)
	}
	if respid == "" {
		err := errors.New("respid is nil")
		log.Error().Err(err).Str("Func", funName).Str("type", handler.msgType.String()).Msg("MsgDispatch RegReqResp error")
		return err
	}
	handler.respId = respid

	// 保存
	old, ok := md.handlers.Load(reqid)
	if ok {
		err := errors.New("already exist")
		log.Error().Err(err).Str("Exist", old.(*msgHandler).funName).Str("Func", funName).Str("MsgID", reqid).Msg("MsgDispatch RegReqResp error")
		return err
	}
	md.handlers.Store(reqid, handler)
	log.Debug().Str("Func", funName).Msg("MsgDispatch RegReqResp")
	return nil
}

func (r *MsgDispatch[Mr, Cr]) RegHook(f func(ctx context.Context, msgid string, elapsed time.Duration)) {
	r.hook = append(r.hook, f)
}

func (r *MsgDispatch[Mr, Cr]) WaitAllMsgDone(timeout time.Duration) {
	ch := make(chan int)
	go func() {
		r.wg.Wait()
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
func (r *MsgDispatch[Mr, Cr]) Dispatch(ctx context.Context, m Mr, c *Cr) bool {
	msgid := m.MsgID()
	value1, ok1 := r.handlers.Load(msgid)
	if ok1 {
		handler, _ := value1.(*msgHandler)
		msg, err := m.BodyUnMarshal(handler.msgType)
		if err != nil {
			utils.LogCtx(log.Error(), ctx).Err(err).Str("MsgID", msgid).Str("MsgType", handler.msgType.String()).Msg("MsgDispatch Dispatch MsgMarshal error")
			return false
		}
		// 熔断
		if name, ok := ParamConf.Get().IsHystrixMsg(msgid); ok {
			hystrix.DoC(ctx, name, func(ctx context.Context) error {
				r.handle(ctx, handler, m, msgid, msg, c)
				return nil
			}, func(ctx context.Context, err error) error {
				// 出现了熔断
				utils.LogCtx(log.Error(), ctx).Err(err).Str("MsgID", msgid).Str("MsgType", handler.msgType.String()).Msg("MsgDispatch Hystrix")
				// 熔断也会调用回调
				r.callhook(ctx, msgid, 0)
				return err
			})
		} else {
			r.handle(ctx, handler, m, msgid, msg, c)
		}
		return true
	}
	return false
}

func (r *MsgDispatch[Mr, Cr]) handle(ctx context.Context, handler *msgHandler, m Mr, msgid string, msg interface{}, c *Cr) {
	r.wg.Add(1)
	defer r.wg.Done()

	// 日志
	logLevel := ParamConf.Get().MsgLogLevel(msgid)
	if logLevel >= int(log.Logger.GetLevel()) {
		r.log(ctx, handler, m, msg, c, logLevel, "DispatchMsg")
	}

	// 消息函数调用
	entry := time.Now()
	resp := r.callFunc(ctx, handler, m, msg, c)
	elapsed := time.Since(entry)

	// rpc回复
	if resp != nil {
		if r.SendResp != nil {
			r.SendResp(ctx, m, c, handler.respId, resp)
		} else {
			utils.LogCtx(log.Error(), ctx).Str("MsgID", msgid).Interface("Resp", resp).Msg("MsgDispatch Dispatch SendResp is nil")
		}
	}

	r.callhook(ctx, msgid, elapsed)
}

// 如果有Resp，返回Resp消息
func (md *MsgDispatch[Mr, Cr]) callFunc(ctx context.Context, handler *msgHandler, m Mr, msg interface{}, c *Cr) interface{} {
	defer utils.HandlePanic()
	// 消息超时检查
	if ParamConf.Get().TimeOutCheck > 0 {
		// 消息处理超时监控逻辑
		msgDone := make(chan int, 1)
		defer close(msgDone)

		utils.Submit(func() {
			timer := time.NewTimer(time.Duration(ParamConf.Get().TimeOutCheck) * time.Second)
			select {
			case <-msgDone:
				if !timer.Stop() {
					select {
					case <-timer.C: // try to drain the channel
					default:
					}
				}
			case <-timer.C:
				// 消息超时了
				md.log(ctx, handler, m, msg, c, int(zerolog.ErrorLevel), "MsgDispatch TimeOut")
			}
		})
	}

	if handler.regType == regType_Msg3 {
		handler.funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(msg), reflect.ValueOf(c)})
	} else if handler.regType == regType_Msg4 {
		handler.funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(m), reflect.ValueOf(msg), reflect.ValueOf(c)})
	} else if handler.regType == regType_ReqResp4 || handler.regType == regType_ReqResp5 {
		resp := reflect.New(handler.respType).Interface()
		if handler.regType == regType_ReqResp4 {
			handler.funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(msg), reflect.ValueOf(resp), reflect.ValueOf(c)})
		} else {
			handler.funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(m), reflect.ValueOf(msg), reflect.ValueOf(resp), reflect.ValueOf(c)})
		}
		return resp
	}
	return nil
}

func (r *MsgDispatch[Mr, Cr]) callhook(ctx context.Context, msgid string, elapsed time.Duration) {
	defer utils.HandlePanic()
	// 回调
	for _, f := range r.hook {
		f(ctx, msgid, elapsed)
	}
}

func (r *MsgDispatch[Mr, Cr]) log(ctx context.Context, handler *msgHandler, m Mr, msg interface{}, c *Cr, logLevel int, logmsg string) {
	l := log.WithLevel(zerolog.Level(logLevel))
	if l == nil {
		return
	}
	l = utils.LogCtx(l, ctx)
	// 调用对象的ConnName函数，输入消息日志
	cer, _ := any(c).(ConnNameer)
	if cer != nil {
		l.Str("Name", cer.ConnName())
	}
	l.Str("Func", handler.funName).Interface("Msg", m)
	l.Msg(logmsg)
}
