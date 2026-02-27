package msger

// https://github.com/yuwf/gobase

import (
	"errors"
	"fmt"
	"reflect"
	sync "sync"
)

var zeroErr reflect.Value = reflect.Zero(reflect.TypeOf((*error)(nil)).Elem())

// 优化SendAsyncRPCMsg函数使用的回调函数(允许为空)，函数要符合以下写法:
// (resp RecvMsger, respBody *具体消息, err error)
// (resp RecvMsger, err error)
// (respBody *具体消息, err error)
type AsyncRPCCallback struct {
	*AsyncRPCCallbackType
	CallbackValue reflect.Value
}

func (cb *AsyncRPCCallback) Call(resp RecvMsger, body interface{}, err error) {
	args := make([]reflect.Value, cb.paramNum)
	if cb.paramNum == 2 {
		// 两个参数时，依据是否有RespType来判断是填充resp还是body
		if cb.respElemType != nil {
			if resp != nil {
				args[0] = reflect.ValueOf(resp)
			} else {
				args[0] = cb.zeroRespValue
			}
		} else {
			if body != nil {
				args[0] = reflect.ValueOf(body)
			} else {
				args[0] = cb.zeroRespBodyValue
			}
		}
	} else if cb.paramNum == 3 {
		if resp != nil {
			args[0] = reflect.ValueOf(resp)
		} else {
			args[0] = cb.zeroRespValue
		}
		if body != nil {
			args[1] = reflect.ValueOf(body)
		} else {
			args[1] = cb.zeroRespBodyValue
		}
	}
	if err != nil {
		args[len(args)-1] = reflect.ValueOf(err)
	} else {
		args[len(args)-1] = zeroErr
	}
	cb.CallbackValue.Call(args)
}

// 异步回调函数类型，有缓存
type AsyncRPCCallbackType struct {
	paramNum     int
	callbackType reflect.Type

	// 移除了*的只Value
	respElemType     reflect.Type
	respBodyElemType reflect.Type

	// 零值
	zeroRespValue     reflect.Value
	zeroRespBodyValue reflect.Value
}

func (cb *AsyncRPCCallbackType) CallbackType() reflect.Type {
	return cb.callbackType
}

func (cb *AsyncRPCCallbackType) RespBodyElemType() reflect.Type {
	return cb.respBodyElemType
}

// 函数优化缓存
var asyncRPCCallbackTypeCache sync.Map // key: callbackType, value: *AsyncRPCCallbackType

// callback为空时，返回nil, 不返回错误
// callback不为空时，会严格判断回调函数的签名是否符合要求
func GetAsyncCallback(callback interface{}) (*AsyncRPCCallback, error) {
	if callback == nil {
		return nil, nil
	}
	// 获取函数类型
	funType := reflect.TypeOf(callback)
	funValue := reflect.ValueOf(callback)

	// 缓存是不是有
	if cbType, ok := asyncRPCCallbackTypeCache.Load(funType); ok {
		return &AsyncRPCCallback{AsyncRPCCallbackType: cbType.(*AsyncRPCCallbackType), CallbackValue: funValue}, nil
	}

	// 必须是函数
	if funType.Kind() != reflect.Func {
		err := errors.New("callback must be function")
		return nil, err
	}
	// 必须有两个参数或者三个参数
	paramNum := funType.NumIn()
	var respType reflect.Type
	var bodyType reflect.Type
	if paramNum != 2 && paramNum != 3 {
		err := errors.New("callback param num must be 2 or 3")
		return nil, err
	}
	if paramNum == 2 {
		// 第一个参数没有实现RecvMsger接口的指针，就表示具体的消息类型
		if funType.In(0).Kind() == reflect.Ptr && funType.In(0).Implements(reflect.TypeOf((*RecvMsger)(nil)).Elem()) {
			respType = funType.In(0)
		} else if funType.In(0).Kind() == reflect.Ptr && funType.In(0).Elem().Kind() == reflect.Struct {
			bodyType = funType.In(0)
		} else {
			err := errors.New(fmt.Sprintf("the first param must be RecvMsger Pointer or Struct Pointer, but %s", funType.In(0).String()))
			return nil, err
		}
	} else if paramNum == 3 {
		// 第一个参数必须是实现了RecvMsger接口的指针
		if funType.In(0).Kind() != reflect.Ptr || !funType.In(0).Implements(reflect.TypeOf((*RecvMsger)(nil)).Elem()) {
			err := errors.New(fmt.Sprintf("the first param must be RecvMsger Pointer, but %s", funType.In(0).String()))
			return nil, err
		}
		// 第二个参数必须是具体消息类型指针
		if funType.In(1).Kind() != reflect.Ptr || funType.In(1).Elem().Kind() != reflect.Struct {
			err := errors.New(fmt.Sprintf("the second param must be Struct Pointer, but %s", funType.In(1).String()))
			return nil, err
		}
		respType = funType.In(0)
		bodyType = funType.In(1)
	}
	// 最后一个参数必须是error
	if !funType.In(paramNum - 1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		err := errors.New(fmt.Sprintf("the last param must be error, but %s", funType.In(paramNum-1).String()))
		return nil, err
	}

	t := &AsyncRPCCallbackType{
		paramNum:     paramNum,
		callbackType: funType,
	}
	if respType != nil {
		t.respElemType = respType.Elem()
		t.zeroRespValue = reflect.Zero(respType)
	}
	if bodyType != nil {
		t.respBodyElemType = bodyType.Elem()
		t.zeroRespBodyValue = reflect.Zero(bodyType)
	}
	asyncRPCCallbackTypeCache.LoadOrStore(funType, t)
	return &AsyncRPCCallback{AsyncRPCCallbackType: t, CallbackValue: funValue}, nil
}
