package msger

// https://github.com/yuwf/gobase

import (
	"reflect"
	"runtime"
	"strings"
)

const (
	RegType_Invalid   = iota
	RegType_Msg3      // 3个参数 (ctx context.Context, msg *具体消息, t *Termianl)
	RegType_Msg4      // 4个参数 (ctx context.Context, m *Msg, msg *具体消息, t *Termianl)
	RegType_ReqResp4  // 4个参数 (ctx context.Context, req *具体消息, resp *具体消息, t *Termianl)
	RegType_ReqResp5  // 5个参数 (ctx context.Context, m *Msg, req *具体消息, resp *具体消息, t *Termianl)
	RegType_ReqReply4 // 4个参数 (ctx context.Context, req *具体消息, reply *ReplyResp[具体消息], t *Termianl)
	RegType_ReqReply5 // 5个参数 (ctx context.Context, m *Msg, req *具体消息, reply *ReplyResp[具体消息], t *Termianl)
)

var ordinalName = []string{"first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth", "tenth"}

type MsgHandler struct {
	RegType      int           // RegType类型
	FunValue     reflect.Value // 处理函数
	FunName      string        // 处理函数名，用来输出日志，不稳定不要用来做逻辑
	FunNameShort string
	MsgType      reflect.Type // 处理消息的类型
	RespType     reflect.Type // 回复消息的类型
	RespId       string       // 回复消息的id
}

// 获取函数名
func getFuncName(fun reflect.Value) (string, string) {
	funName := runtime.FuncForPC(fun.Pointer()).Name()
	funName = strings.Replace(funName, "-fm", "", -1)
	funNameShort := funName

	slice := strings.Split(funName, "/")
	shortSlice := strings.Split(funName, ".")
	if len(slice) > 0 {
		funName = slice[len(slice)-1]
	}
	if len(shortSlice) > 0 {
		funNameShort = shortSlice[len(shortSlice)-1]
	}
	return funName, funNameShort
}
