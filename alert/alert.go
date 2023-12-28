package alert

// https://github.com/yuwf/gobase

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	// 日志钩子
	hook logHook

	errorLogPrefixLock sync.RWMutex
	// 报警日志 Msg前缀部分
	errorLogPrefix = map[string]interface{}{
		"Panic":               nil,
		"Consul":              nil,
		"Redis":               nil,
		"MySQL":               nil,
		"RandParamErr":        nil,
		"HttpRequest Err":     nil,
		"HttpRequest TimeOut": nil,
	}
	// 报警日志 Msg前缀部分
	// 一个错误多个节点可能都会报警 会先调用 LogAlertCheck 判断是否报警节点
	// 外部可重新 LogAlertCheck 函数
	errorLogPrefix2 = map[string]interface{}{
		"JsonLoader": nil,
		"StrLoader":  nil,
	}
	// 检查报警函数，外部可赋值重定义
	LogAlertCheck = func(prefix string) bool {
		return true
	}
)

// InitAlert 需要在日志初始化完毕之后调用
// 内部开启一个时钟，在时钟的范围内记录最近的错误日志
// 返回的fun需要外层defer调用， 参数表示如果还在时钟范围内，是否发送启动错误日志
func InitAlert() func(sendAlert bool) {
	// 设置日志钩子
	log.Logger = log.Logger.Hook(&hook)

	hook.bootChecking = true
	// 开启超时关闭检查
	go func() {
		ticker := time.Tick(15 * time.Second)
		<-ticker
		hook.bootChecking = false
	}()
	// 外层回调
	return func(sendAlert bool) {
		if hook.bootChecking {
			hook.bootChecking = false
			// 把最近的日志alert出来
			if sendAlert {
				hook.sendLastLogs()
			}
		}
	}
}

// 报警日志前缀添加
func AddErrorLogPrefix(prefix string) {
	errorLogPrefixLock.Lock()
	defer errorLogPrefixLock.Unlock()
	errorLogPrefix[prefix] = nil
}

func RemoveErrorLogPrefix(prefix string) {
	errorLogPrefixLock.Lock()
	defer errorLogPrefixLock.Unlock()
	delete(errorLogPrefix, prefix)
}

// 报警日志前缀添加 多节点唯一性报警
func AddErrorLogPrefix2(prefix string) {
	errorLogPrefixLock.Lock()
	defer errorLogPrefixLock.Unlock()
	errorLogPrefix2[prefix] = nil
}

func RemoveErrorLogPrefix2(prefix string) {
	errorLogPrefixLock.Lock()
	defer errorLogPrefixLock.Unlock()
	delete(errorLogPrefix2, prefix)
}

func checkSendAlert(msg string) bool {
	errorLogPrefixLock.RLock()
	defer errorLogPrefixLock.RUnlock()

	alert := false
	for k, _ := range errorLogPrefix2 {
		l := len(k)
		if len(msg) >= l && msg[0:l] == k {
			if LogAlertCheck != nil && LogAlertCheck(k) {
				alert = true
			}
			break
		}
	}
	if !alert {
		for k, _ := range errorLogPrefix {
			l := len(k)
			if len(msg) >= l && msg[0:l] == k {
				alert = true
				break
			}
		}
	}
	return alert
}

// 监控所有的Fatal日志
// 监控错误日志 以Redis、ConfigErr开头的日志
type logHook struct {
	// 程序启动检查
	bootChecking bool // 不要求严格行无需用锁
	// 记录最近几条错误日志 需要用到锁
	lastLock      sync.Mutex
	lastLogs      [5]string
	lastLogsIndex int // 最后一次写入lastLogs的索引

	// 要报警的日志采样 key表示报警位置
	samplingLock sync.Mutex
	samplingLogs map[string]*logSampling
}

// 日志采样
type logSampling struct {
	// 不修改
	pos  string
	msg  string
	info string
	// 原子操作
	count int32 // 上次报警到本次报警，出现的次数
	// 只有开启的协程访问
	totalCount    int32 // 累计次数
	lastAlertTime int64 // 上次报警时间
}

func (h *logHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if level == zerolog.FatalLevel {
		vo := reflect.ValueOf(e).Elem()
		buf := vo.FieldByName("buf")
		SendFeiShuAlert2("Fatal %s\n%s}", msg, string(buf.Bytes()))
	} else if level == zerolog.PanicLevel {
		vo := reflect.ValueOf(e).Elem()
		buf := vo.FieldByName("buf")
		SendFeiShuAlert2("%s\n%s}", msg, string(buf.Bytes()))
	} else if level == zerolog.ErrorLevel {
		if checkSendAlert(msg) {
			h.addAlertLog(e, &msg)
		}

		if hook.bootChecking {
			h.saveLastLog(e, &msg)
		}
	}
}

func (h *logHook) saveLastLog(e *zerolog.Event, msg *string) {
	hook.lastLock.Lock()
	defer hook.lastLock.Unlock()
	vo := reflect.ValueOf(e).Elem()
	buf := vo.FieldByName("buf")
	h.lastLogs[h.lastLogsIndex] = fmt.Sprintf("%s\t%s}", *msg, string(buf.Bytes()))
	h.lastLogsIndex++
	if h.lastLogsIndex >= len(h.lastLogs) {
		h.lastLogsIndex = 0
	}
}

func (h *logHook) sendLastLogs() {
	hook.lastLock.Lock()
	defer hook.lastLock.Unlock()
	send := "程序启动失败"
	for i := 0; i < len(h.lastLogs); i++ {
		index := (h.lastLogsIndex + i) % len(h.lastLogs)
		if len(h.lastLogs[index]) > 0 {
			send = send + "\n\n" + h.lastLogs[index]
		}
	}
	SendFeiShuAlert2(send)
}

func (h *logHook) addAlertLog(e *zerolog.Event, msg *string) {
	hook.samplingLock.Lock()
	if h.samplingLogs == nil {
		h.samplingLogs = map[string]*logSampling{}
	}
	// 算出报警位置
	_, file, line, ok := runtime.Caller(4)
	if !ok {
		hook.samplingLock.Unlock()
		return
	}
	pos := fmt.Sprintf("%s%d", file, line)

	a, ok := h.samplingLogs[pos]
	if !ok {
		a = &logSampling{
			lastAlertTime: time.Now().UnixNano(), // 第一次会先报警一次 记录里当前时间
		}
		h.samplingLogs[pos] = a
	}
	hook.samplingLock.Unlock()

	if ok {
		atomic.AddInt32(&a.count, 1)
		return
	}

	eVO := reflect.ValueOf(e).Elem()
	buf := eVO.FieldByName("buf")
	a.pos = pos
	a.msg = *msg
	a.info = string(buf.Bytes())
	a.totalCount = 1
	// 先报警一次
	if a.msg == "Panic" {
		SendFeiShuAlert2("%s\n\n%s}", a.msg, a.info)
	} else {
		SendFeiShuAlert("Error %s\n\n%s}", a.msg, a.info)
	}

	// 开启协程 检查该报警
	go func() {
		for {
			time.Sleep(time.Minute)
			count := atomic.LoadInt32(&a.count)
			if count > 0 {
				atomic.AddInt32(&a.count, -count)
				a.totalCount += count
				SendFeiShuAlert("Error %s\n\nCount:%d\nTotalCount:%d\n\n%s}", a.msg, count, a.totalCount, a.info)
			} else {
				hook.samplingLock.Lock()
				delete(hook.samplingLogs, a.pos)
				hook.samplingLock.Unlock()
				break // 退出
			}
		}
	}()
}
