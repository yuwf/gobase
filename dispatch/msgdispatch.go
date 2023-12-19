package dispatch

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	sync "sync"
	"time"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/rs/zerolog/log"
	"github.com/yuwf/gobase/alert"
	"github.com/yuwf/gobase/utils"
)

func init() {
	alert.AddErrorLogPrefix("MsgDispatch Reg")
	alert.AddErrorLogPrefix("HandleMsg TimeOut")
}

type msgHandler struct {
	regType  int           // 表示是用哪个函数注册的
	funValue reflect.Value // 处理函数
	msgType  reflect.Type  // 处理消息的类型
	respType reflect.Type  // 回复消息的类型
}

// 消息接口，包括消息头和消息体结构
type Msger interface {
	Head() interface{}                                      // 获取消息头
	MsgID() string                                          // 获取msgid
	MsgUnMarshal(msgType reflect.Type) (interface{}, error) // 根据具体消息类型，解析出消息体
	Resp(resp interface{}) interface{}                      // ReqResp注册函数使用，获取回复的消息对象（包括头和消息体），参数resp表示回复的消息体
}

// 一般为连接对象
type Conner interface {
	ConnName() string              // 连接对象名字，用来输出日志
	SendMsg(msg interface{}) error // ReqResp注册函数使用 用来发送Resp消息
}

// 注册时获取具体消息的msgid 【需要外部来设置】，如果设置了MsgDispatch.RegMsgID会优先调用它
var DefaultRegMsgID func(msgType reflect.Type) string

// MsgDispatch 提供消息注册和分发功能
// Mr表示用来透传的消息类型，必须实现Msger接口
// Cr表示用来透传的连接类型，必须实现Conner接口
type MsgDispatch[Mr any, Cr any] struct {
	// 注册时获取具体消息的msgid 【需要外部来设置】
	RegMsgID func(msgType reflect.Type) string

	handlers sync.Map     // 处理函数  [msgid:*msgHandler]
	mType    reflect.Type // Mr的类型
	cType    reflect.Type // Cr的类型

	wg sync.WaitGroup // 用于所有消息的处理完毕等待
}

// 注册消息处理函数
// fun的参数支持以下写法
// (ctx context.Context, msg *具体消息, c *Cr)
// (ctx context.Context, m *Mr, msg *具体消息, c *Cr)
func (md *MsgDispatch[Mr, Cr]) RegMsg(fun interface{}) {
	if md.mType == nil {
		m := new(Mr)
		md.mType = reflect.TypeOf(m)
	}
	if md.cType == nil {
		c := new(Cr)
		md.cType = reflect.TypeOf(c)
	}

	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	// 必须是函数
	if funType.Kind() != reflect.Func {
		log.Error().Msg("MsgDispatch RegMsg param must be function")
		return
	}
	// 必须有三个或者四个参数
	paramNum := funType.NumIn()
	if paramNum != 3 && paramNum != 4 {
		log.Error().Msg("MsgDispatch RegMsg param num must be 3 or 4")
		return
	}
	// 第一个参数必须是context.Context
	if funType.In(0).String() != "context.Context" {
		log.Error().Str("type", funType.In(0).String()).Msg("MsgDispatch RegMsg first param must be context.Context")
		return
	}
	if paramNum == 3 {
		// 第二个参数是具体消息类型指针
		if funType.In(1).Kind() != reflect.Ptr {
			log.Error().Str("type", funType.In(1).String()).Msg("MsgDispatch RegMsg second param must be Pointer")
			return
		}
		// 获取下他的msgid
		var msgid string
		if md.RegMsgID != nil {
			msgid = md.RegMsgID(funType.In(1).Elem())
		} else if DefaultRegMsgID != nil {
			msgid = DefaultRegMsgID(funType.In(1).Elem())
		}
		if msgid == "" {
			log.Error().Str("type", funType.In(1).String()).Msg("MsgDispatch RegMsg second param get msgid is nil")
			return
		}
		// 第三个参数必须是*Cr
		if funType.In(2) != md.cType {
			log.Error().Str("type", funType.In(3).String()).Str("musttype", md.mType.Name()).Msg("MsgDispatch RegMsg third param err")
			return
		}
		// 保存
		md.handlers.Store(string(msgid), &msgHandler{
			regType:  3,
			funValue: funValue,
			msgType:  funType.In(1).Elem(),
		})
	} else {
		// 第二个参数必须是*Mr
		if funType.In(1) != md.mType {
			log.Error().Str("type", funType.In(1).String()).Str("musttype", md.cType.Name()).Msg("MsgDispatch RegMsg second param err")
			return
		}
		// 第三个参数是具体消息类型指针
		if funType.In(2).Kind() != reflect.Ptr {
			log.Error().Str("type", funType.In(2).String()).Msg("MsgDispatch RegMsg third param must be Pointer")
			return
		}
		// 获取下他的msgid
		var msgid string
		if md.RegMsgID != nil {
			msgid = md.RegMsgID(funType.In(2).Elem())
		} else if DefaultRegMsgID != nil {
			msgid = DefaultRegMsgID(funType.In(1).Elem())
		}
		if msgid == "" {
			log.Error().Str("type", funType.In(2).String()).Msg("MsgDispatch RegMsg third param get msgid is nil")
			return
		}
		// 第四个参数必须是*Cr
		if funType.In(3) != md.cType {
			log.Error().Str("type", funType.In(3).String()).Str("musttype", md.cType.Name()).Msg("MsgDispatch RegMsg fourth param err")
			return
		}
		// 保存
		md.handlers.Store(string(msgid), &msgHandler{
			regType:  4,
			funValue: funValue,
			msgType:  funType.In(2).Elem(),
		})
	}

}

// 处理消息的函数参数是
// fun的参数支持以下写法
// (ctx context.Context, req *具体消息, resp *具体消息, c *Cr)
// (ctx context.Context, m *Mr, req *具体消息, resp *具体消息, c *Cr)
func (md *MsgDispatch[Mr, Cr]) RegReqResp(fun interface{}) {
	if md.mType == nil {
		m := new(Mr)
		md.mType = reflect.TypeOf(m)
	}
	if md.cType == nil {
		c := new(Cr)
		md.cType = reflect.TypeOf(c)
	}

	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	// 必须是函数
	if funType.Kind() != reflect.Func {
		log.Error().Msg("MsgDispatch RegReqResp param must be function")
		return
	}
	// 必须有四个参数
	paramNum := funType.NumIn()
	if paramNum != 4 && paramNum != 5 {
		log.Error().Msg("MsgDispatch RegReqResp param num must be 4 or 5")
		return
	}
	// 第一个参数必须是context.Context
	if funType.In(0).String() != "context.Context" {
		log.Error().Str("type", funType.In(0).String()).Msg("MsgDispatch RegReqResp first param must be context.Context")
		return
	}
	if paramNum == 4 {
		// 第二个参数是具体消息类型指针
		if funType.In(1).Kind() != reflect.Ptr {
			log.Error().Str("type", funType.In(1).String()).Msg("MsgDispatch RegReqResp second param must be Pointer")
			return
		}
		// 获取下他的msgid
		var msgid string
		if md.RegMsgID != nil {
			msgid = md.RegMsgID(funType.In(1).Elem())
		} else if DefaultRegMsgID != nil {
			msgid = DefaultRegMsgID(funType.In(1).Elem())
		}
		if msgid == "" {
			log.Error().Str("type", funType.In(1).String()).Msg("MsgDispatch RegReqResp second param get msgid is nil")
			return
		}
		// 第三个参数是具体消息类型指针
		if funType.In(2).Kind() != reflect.Ptr {
			log.Error().Str("type", funType.In(2).String()).Msg("MsgDispatch RegReqResp third param must be Pointer")
			return
		}
		// 第四个参数必须是*Cr
		if funType.In(3) != md.cType {
			log.Error().Str("type", funType.In(3).String()).Str("musttype", md.cType.Name()).Msg("MsgDispatch RegReqResp fourth param err")
			return
		}
		// 保存
		md.handlers.Store(msgid, &msgHandler{
			regType:  14,
			funValue: funValue,
			msgType:  funType.In(1).Elem(),
			respType: funType.In(2).Elem(),
		})
	} else {
		// 第二个参数必须是*Mr
		if funType.In(1) != md.mType {
			log.Error().Str("type", funType.In(1).String()).Str("musttype", md.cType.Name()).Msg("MsgDispatch RegReqResp second param err")
			return
		}
		// 第三个参数是具体消息类型指针
		if funType.In(2).Kind() != reflect.Ptr {
			log.Error().Str("type", funType.In(2).String()).Msg("MsgDispatch RegReqResp third param must be Pointer")
			return
		}
		// 获取下他的msgid
		var msgid string
		if md.RegMsgID != nil {
			msgid = md.RegMsgID(funType.In(2).Elem())
		} else if DefaultRegMsgID != nil {
			msgid = DefaultRegMsgID(funType.In(1).Elem())
		}
		if msgid == "" {
			log.Error().Str("type", funType.In(2).String()).Msg("MsgDispatch RegReqResp third param get msgid is nil")
			return
		}
		// 第四个参数是具体消息类型指针
		if funType.In(3).Kind() != reflect.Ptr {
			log.Error().Str("type", funType.In(3).String()).Msg("MsgDispatch RegReqResp fourth param must be Pointer")
			return
		}
		// 第五个参数必须是*Cr
		if funType.In(4) != md.cType {
			log.Error().Str("type", funType.In(5).String()).Str("musttype", md.cType.Name()).Msg("MsgDispatch RegReqResp fifth param err")
			return
		}
		// 保存
		md.handlers.Store(string(msgid), &msgHandler{
			regType:  15,
			funValue: funValue,
			msgType:  funType.In(2).Elem(),
			respType: funType.In(3).Elem(),
		})
	}

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
func (r *MsgDispatch[Mr, Cr]) Dispatch(ctx context.Context, m *Mr, c *Cr) bool {
	mer, ok := any(m).(Msger)
	if !ok {
		log.Error().Str("type", reflect.TypeOf(m).Name()).Msg("MsgDispatch Dispatch not implement Msger")
		return false
	}
	value1, ok1 := r.handlers.Load(mer.MsgID())
	if ok1 {
		handler, _ := value1.(*msgHandler)
		msg, err := mer.MsgUnMarshal(handler.msgType)
		if err != nil {
			log.Error().Err(err).Str("MsgID", mer.MsgID()).Str("MsgType", handler.msgType.String()).Msg("MsgDispatch Dispatch MsgMarshal error")
			return false
		}
		// 熔断
		if name, ok := ParamConf.Get().IsHystrixMsg(mer.MsgID()); ok {
			hystrix.DoC(ctx, name, func(ctx context.Context) error {
				r.handle(ctx, handler, m, msg, c)
				return nil
			}, func(ctx context.Context, err error) error {
				// 出现了熔断
				return err
			})
		} else {
			r.handle(ctx, handler, m, msg, c)
		}
		return true
	}
	return false
}

func (r *MsgDispatch[Mr, Cr]) handle(ctx context.Context, handler *msgHandler, m *Mr, msg interface{}, c *Cr) {
	r.wg.Add(1)
	defer r.wg.Done()
	mer, _ := any(m).(Msger)

	// 消息日志
	if !ParamConf.Get().IsIgnoreMsg(mer.MsgID()) {
		l := log.Debug()
		// 调用对象的ConnName函数，输入消息日志
		cer, ok := any(c).(Conner)
		if ok {
			l.Str("Name", cer.ConnName())
		}
		funname := runtime.FuncForPC(handler.funValue.Pointer()).Name()
		slice := strings.Split(funname, "/")
		if len(slice) > 0 {
			funname = slice[len(slice)-1]
		}
		l.Str("Call", funname).Interface("Head", mer.Head()).Interface("Body", msg)
		l.Msg("DispatchMsg")
	}

	// 消息函数调用
	respMsg := r.callFunc(ctx, handler, m, msg, c)

	// rpc回复
	if respMsg != nil {
		resp := mer.Resp(respMsg)
		if resp != nil {
			// 查找对象的SendMsg函数，发送回复消息
			cer, ok := any(c).(Conner)
			if ok {
				cer.SendMsg(resp)
			}
		}
	}
}

// 如果有Resp，返回Resp消息
func (md *MsgDispatch[Mr, Cr]) callFunc(ctx context.Context, handler *msgHandler, m *Mr, msg interface{}, c *Cr) interface{} {
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
				l := log.Error()
				cer, ok := any(c).(Conner)
				if ok {
					l.Str("Name", cer.ConnName())
				}
				mer, _ := any(m).(Msger)
				funname := runtime.FuncForPC(handler.funValue.Pointer()).Name()
				slice := strings.Split(funname, "/")
				if len(slice) > 0 {
					funname = slice[len(slice)-1]
				}
				l.Str("Call", funname).Interface("Head", mer.Head()).Interface("Body", msg)
				l.Msg("HandleMsg TimeOut")
			}
		})
	}

	if handler.regType == 3 {
		handler.funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(msg), reflect.ValueOf(c)})
	} else if handler.regType == 4 {
		handler.funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(m), reflect.ValueOf(msg), reflect.ValueOf(c)})
	} else if handler.regType == 14 || handler.regType == 15 {
		respMsg := reflect.New(handler.respType).Interface()
		if handler.regType == 14 {
			handler.funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(msg), reflect.ValueOf(respMsg), reflect.ValueOf(c)})
		} else {
			handler.funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(m), reflect.ValueOf(msg), reflect.ValueOf(respMsg), reflect.ValueOf(c)})
		}
		return respMsg
	}
	return nil
}
