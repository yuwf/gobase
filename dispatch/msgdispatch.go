package dispatch

// https://github.com/yuwf/gobase

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	sync "sync"
	"time"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/yuwf/gobase/utils"
)

type msgHandler struct {
	regType  int           // 表示是用哪个函数注册的
	funValue reflect.Value // 处理函数
	funName  string        // 处理函数名，用来输出日志
	msgType  reflect.Type  // 处理消息的类型
	respType reflect.Type  // 回复消息的类型
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
	SendResp func(ctx context.Context, m Mr, c *Cr, resp interface{})

	handlers sync.Map     // 处理函数  [msgid:*msgHandler]
	mType    reflect.Type // Mr的类型
	cType    reflect.Type // Cr的类型

	wg sync.WaitGroup // 用于所有消息的处理完毕等待

	// 请求处理完后回调 不使用锁，默认要求提前注册好
	hook []func(ctx context.Context, msgid string, elapsed time.Duration)
}

// 注册消息处理函数
// fun的参数支持以下写法
// (ctx context.Context, msg *具体消息, c *Cr)
// (ctx context.Context, m *Mr, msg *具体消息, c *Cr)
func (md *MsgDispatch[Mr, Cr]) RegMsg(fun interface{}) {
	if md.mType == nil {
		mObj := new(Mr)
		md.mType = reflect.TypeOf(mObj).Elem()
	}
	if md.cType == nil {
		cObj := new(Cr)
		md.cType = reflect.TypeOf(cObj)
	}
	// 获取函数类型和函数名
	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	funName := runtime.FuncForPC(funValue.Pointer()).Name()
	slice := strings.Split(funName, "/")
	if len(slice) > 0 {
		funName = slice[len(slice)-1]
		funName = strings.Replace(funName, "-fm", "", -1)
	}

	// 必须是函数
	if funType.Kind() != reflect.Func {
		log.Error().Str("Call", funName).Msg("MsgDispatch RegMsg param must be function")
		return
	}
	// 必须有三个或者四个参数
	paramNum := funType.NumIn()
	if paramNum != 3 && paramNum != 4 {
		log.Error().Str("Call", funName).Msg("MsgDispatch RegMsg param num must be 3 or 4")
		return
	}
	// 第一个参数必须是context.Context
	if funType.In(0).String() != "context.Context" {
		log.Error().Str("Call", funName).Str("type", funType.In(0).String()).Msg("MsgDispatch RegMsg first param must be context.Context")
		return
	}
	if paramNum == 3 {
		// 第二个参数是具体消息类型指针
		if funType.In(1).Kind() != reflect.Ptr {
			log.Error().Str("Call", funName).Str("type", funType.In(1).String()).Msg("MsgDispatch RegMsg second param must be Pointer")
			return
		}
		// 获取下他的msgid
		var msgid string
		if md.RegMsgID != nil {
			msgid = md.RegMsgID(funType.In(1).Elem())
		}
		if msgid == "" {
			log.Error().Str("Call", funName).Str("type", funType.In(1).String()).Msg("MsgDispatch RegMsg second param get msgid is nil")
			return
		}
		// 第三个参数必须是*Cr
		if funType.In(2) != md.cType {
			log.Error().Str("Call", funName).Str("type", funType.In(3).String()).Str("musttype", md.mType.String()).Msg("MsgDispatch RegMsg third param err")
			return
		}
		// 保存
		md.handlers.Store(string(msgid), &msgHandler{
			regType:  3,
			funValue: funValue,
			funName:  funName,
			msgType:  funType.In(1).Elem(),
		})
	} else {
		// 第二个参数必须是Mr
		if funType.In(1) != md.mType {
			log.Error().Str("Call", funName).Str("type", funType.In(1).String()).Str("musttype", md.cType.String()).Msg("MsgDispatch RegMsg second param err")
			return
		}
		// 第三个参数是具体消息类型指针
		if funType.In(2).Kind() != reflect.Ptr {
			log.Error().Str("Call", funName).Str("type", funType.In(2).String()).Msg("MsgDispatch RegMsg third param must be Pointer")
			return
		}
		// 获取下他的msgid
		var msgid string
		if md.RegMsgID != nil {
			msgid = md.RegMsgID(funType.In(2).Elem())
		}
		if msgid == "" {
			log.Error().Str("Call", funName).Str("type", funType.In(2).String()).Msg("MsgDispatch RegMsg third param get msgid is nil")
			return
		}
		// 第四个参数必须是*Cr
		if funType.In(3) != md.cType {
			log.Error().Str("Call", funName).Str("type", funType.In(3).String()).Str("musttype", md.cType.String()).Msg("MsgDispatch RegMsg fourth param err")
			return
		}
		// 保存
		md.handlers.Store(string(msgid), &msgHandler{
			regType:  4,
			funValue: funValue,
			funName:  funName,
			msgType:  funType.In(2).Elem(),
		})
	}
}

// 处理消息的函数参数是
// fun的参数支持以下写法
// (ctx context.Context, req *具体消息, resp *具体消息, c *Cr)
// (ctx context.Context, m Mr, req *具体消息, resp *具体消息, c *Cr)
func (md *MsgDispatch[Mr, Cr]) RegReqResp(fun interface{}) {
	if md.mType == nil {
		mObj := new(Mr)
		md.mType = reflect.TypeOf(mObj).Elem()
	}
	if md.cType == nil {
		cObj := new(Cr)
		md.cType = reflect.TypeOf(cObj)
	}
	// 获取函数类型和函数名
	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	funName := runtime.FuncForPC(funValue.Pointer()).Name()
	slice := strings.Split(funName, "/")
	if len(slice) > 0 {
		funName = slice[len(slice)-1]
		funName = strings.Replace(funName, "-fm", "", -1)
	}

	// 必须是函数
	if funType.Kind() != reflect.Func {
		log.Error().Str("Call", funName).Msg("MsgDispatch RegReqResp param must be function")
		return
	}
	// 必须有四个参数
	paramNum := funType.NumIn()
	if paramNum != 4 && paramNum != 5 {
		log.Error().Str("Call", funName).Msg("MsgDispatch RegReqResp param num must be 4 or 5")
		return
	}
	// 第一个参数必须是context.Context
	if funType.In(0).String() != "context.Context" {
		log.Error().Str("Call", funName).Str("type", funType.In(0).String()).Msg("MsgDispatch RegReqResp first param must be context.Context")
		return
	}
	if paramNum == 4 {
		// 第二个参数是具体消息类型指针
		if funType.In(1).Kind() != reflect.Ptr {
			log.Error().Str("Call", funName).Str("type", funType.In(1).String()).Msg("MsgDispatch RegReqResp second param must be Pointer")
			return
		}
		// 获取下他的msgid
		var msgid string
		if md.RegMsgID != nil {
			msgid = md.RegMsgID(funType.In(1).Elem())
		}
		if msgid == "" {
			log.Error().Str("Call", funName).Str("type", funType.In(1).String()).Msg("MsgDispatch RegReqResp second param get msgid is nil")
			return
		}
		// 第三个参数是具体消息类型指针
		if funType.In(2).Kind() != reflect.Ptr {
			log.Error().Str("Call", funName).Str("type", funType.In(2).String()).Msg("MsgDispatch RegReqResp third param must be Pointer")
			return
		}
		// 第四个参数必须是*Cr
		if funType.In(3) != md.cType {
			log.Error().Str("Call", funName).Str("type", funType.In(3).String()).Str("musttype", md.cType.String()).Msg("MsgDispatch RegReqResp fourth param err")
			return
		}
		// 保存
		md.handlers.Store(msgid, &msgHandler{
			regType:  14,
			funValue: funValue,
			funName:  funName,
			msgType:  funType.In(1).Elem(),
			respType: funType.In(2).Elem(),
		})
	} else {
		// 第二个参数必须是Mr
		if funType.In(1) != md.mType {
			log.Error().Str("Call", funName).Str("type", funType.In(1).String()).Str("musttype", md.cType.String()).Msg("MsgDispatch RegReqResp second param err")
			return
		}
		// 第三个参数是具体消息类型指针
		if funType.In(2).Kind() != reflect.Ptr {
			log.Error().Str("Call", funName).Str("type", funType.In(2).String()).Msg("MsgDispatch RegReqResp third param must be Pointer")
			return
		}
		// 获取下他的msgid
		var msgid string
		if md.RegMsgID != nil {
			msgid = md.RegMsgID(funType.In(2).Elem())
		}
		if msgid == "" {
			log.Error().Str("Call", funName).Str("type", funType.In(2).String()).Msg("MsgDispatch RegReqResp third param get msgid is nil")
			return
		}
		// 第四个参数是具体消息类型指针
		if funType.In(3).Kind() != reflect.Ptr {
			log.Error().Str("Call", funName).Str("type", funType.In(3).String()).Msg("MsgDispatch RegReqResp fourth param must be Pointer")
			return
		}
		// 第五个参数必须是*Cr
		if funType.In(4) != md.cType {
			log.Error().Str("Call", funName).Str("type", funType.In(4).String()).Str("musttype", md.cType.String()).Msg("MsgDispatch RegReqResp fifth param err")
			return
		}
		// 保存
		md.handlers.Store(string(msgid), &msgHandler{
			regType:  15,
			funValue: funValue,
			funName:  funName,
			msgType:  funType.In(2).Elem(),
			respType: funType.In(3).Elem(),
		})
	}

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
			r.SendResp(ctx, m, c, resp)
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

	if handler.regType == 3 {
		handler.funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(msg), reflect.ValueOf(c)})
	} else if handler.regType == 4 {
		handler.funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(m), reflect.ValueOf(msg), reflect.ValueOf(c)})
	} else if handler.regType == 14 || handler.regType == 15 {
		resp := reflect.New(handler.respType).Interface()
		if handler.regType == 14 {
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
	l.Str("Call", handler.funName).Interface("Msg", m)
	l.Msg(logmsg)
}
