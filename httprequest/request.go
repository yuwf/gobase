package httprequest

// https://github.com/yuwf/gobase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/afex/hystrix-go/hystrix"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type HttpRequest struct {
	Method string // 请求方法 GET POST ..
	Addr   string // 请求地址
	URL    *url.URL
	Header http.Header //请求头
	Data   []byte      // 请求数据

	Err    error  // 内部错误
	errPos string // 错误的地方 引起错误的点太多，记录详情位置，内部日志使用

	Resp     *http.Response // 请求返回对象 Err==nil时有效
	RespData []byte         // 请求返回数据 Err==nil时有效

	entry   time.Time
	Elapsed time.Duration // 请求耗时

	Body     interface{} // 根据请求接口来填充原始对象
	RespBody interface{} // 根据请求接口来填充原始对象
}

// 请求处理完后回调 不使用锁，默认要求提前注册好
var httpHook []func(ctx context.Context, call *HttpRequest)

func RegHook(f func(ctx context.Context, call *HttpRequest)) {
	httpHook = append(httpHook, f)
}

// Request http请求 返回值StatusCode 内容 错误码
func Request(ctx context.Context, method, addr string, body []byte, headers map[string]string) (int, []byte, error) {
	call := &HttpRequest{
		entry:  time.Now(),
		Method: method,
		Addr:   addr,
		Header: make(http.Header),
		Data:   body,
	}
	for k, v := range headers {
		call.Header.Set(k, v)
	}
	defer call.done(ctx)

	// URL解析
	call.URL, call.Err = url.Parse(addr)
	if call.Err != nil {
		call.errPos = "Parse Addr error"
		return 0, nil, call.Err
	}

	call.call(ctx)
	if call.Err != nil {
		return 0, nil, call.Err
	}
	return call.Resp.StatusCode, call.RespData, nil
}

// body和返回值都是可以转Json的对象
func JsonRequest[T any](ctx context.Context, method, addr string, body interface{}, headers map[string]string) (int, *T, error) {
	call := &HttpRequest{
		entry:  time.Now(),
		Method: method,
		Addr:   addr,
		Header: make(http.Header),
	}
	for k, v := range headers {
		call.Header.Set(k, v)
	}
	defer call.done(ctx)

	// URL解析
	call.URL, call.Err = url.Parse(addr)
	if call.Err != nil {
		call.errPos = "Parse Addr error"
		return 0, nil, call.Err
	}

	// 数据格式化
	switch data := body.(type) {
	case []byte:
		call.Data = data
	case string:
		call.Data = []byte(data)
	default:
		call.Body = body
		call.Data, call.Err = json.Marshal(body)
		if call.Err != nil {
			call.errPos = "Marshal error"
			return 0, nil, call.Err
		}
	}

	// 添加上json格式头
	call.Header.Set("Content-Type", "application/json")
	call.Header.Set("Accept", "application/json")

	call.call(ctx)
	if call.Err != nil {
		return 0, nil, call.Err
	}

	// 解析返回值
	t := new(T)
	call.Err = json.Unmarshal(call.RespData, t)
	if call.Err != nil {
		if call.Resp != nil && call.Resp.StatusCode != http.StatusOK {
			call.Err = errors.New(call.Resp.Status)
			return call.Resp.StatusCode, nil, call.Err
		}
		call.errPos = "Unmarshal error"
		return call.Resp.StatusCode, nil, call.Err
	}
	call.RespBody = t
	return call.Resp.StatusCode, t, nil
}

// 专业接口
func Request2(ctx context.Context, method, addr string, body []byte, header http.Header) *HttpRequest {
	call := &HttpRequest{
		entry:  time.Now(),
		Method: method,
		Addr:   addr,
		Header: header,
		Data:   body,
	}
	defer call.done(ctx)

	// URL解析
	call.URL, call.Err = url.Parse(addr)
	if call.Err != nil {
		call.errPos = "Parse Addr error"
		return call
	}

	call.call(ctx)
	return call
}

// 专业接口
// body和返回值都是可以转Json的对象
func JsonRequest2[T any](ctx context.Context, method, addr string, body interface{}, header http.Header) (*T, *HttpRequest) {
	call := &HttpRequest{
		entry:  time.Now(),
		Method: method,
		Addr:   addr,
		Header: header,
	}
	defer call.done(ctx)

	// URL解析
	call.URL, call.Err = url.Parse(addr)
	if call.Err != nil {
		call.errPos = "Parse Addr error"
		return nil, call
	}

	// 数据格式化
	switch data := body.(type) {
	case []byte:
		call.Data = data
	case string:
		call.Data = []byte(data)
	default:
		call.Body = body
		call.Data, call.Err = json.Marshal(body)
		if call.Err != nil {
			call.errPos = "Marshal error"
			return nil, call
		}
	}

	// 添加上json格式头
	call.Header.Set("Content-Type", "application/json")
	call.Header.Set("Accept", "application/json")

	call.call(ctx)
	if call.Err != nil {
		return nil, call
	}

	// 解析返回值
	t := new(T)
	call.Err = json.Unmarshal(call.RespData, t)
	if call.Err != nil {
		call.errPos = "Unmarshal error"
		return nil, call
	}
	call.RespBody = t
	return t, call
}

func (h *HttpRequest) call(ctx context.Context) {
	// 熔断检查
	if name, ok := ParamConf.Get().IsHystrixURL(h.Addr); ok {
		hystrix.DoC(ctx, name, func(ctx context.Context) error {
			h.call_(ctx)
			return h.Err
		}, func(ctx context.Context, err error) error {
			// 出现了熔断
			h.Err = err
			return err
		})
	} else {
		h.call_(ctx)
	}
}
func (h *HttpRequest) call_(ctx context.Context) {

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
				h.log(ctx, int(zerolog.ErrorLevel), time.Duration(ParamConf.Get().TimeOutCheck), "HttpRequest TimeOut")
			}
		})
	}

	// 准备body和header
	reqBody := bytes.NewBuffer(h.Data)
	var request *http.Request
	request, h.Err = http.NewRequestWithContext(ctx, h.Method, h.Addr, reqBody)
	if h.Err != nil {
		h.errPos = "NewRequest fail"
		return
	}
	if h.Header != nil {
		request.Header = h.Header
	}
	// traceId透传
	if traceId := ctx.Value(utils.CtxKey_traceId); traceId != nil {
		request.Header.Set(utils.HttpTraceIdHeader, fmt.Sprintf("%d", traceId.(int64)))
	}

	// 请求
	var client *http.Client
	if ParamConf.Get().Pool {
		client = getHttpClient(h.URL.Host)
	} else {
		client = &http.Client{
			Timeout: time.Second * 8,
		}
	}

	h.Resp, h.Err = client.Do(request)
	if h.Err != nil {
		h.errPos = "Request Do error"
		return
	}

	// 读取返回值
	h.RespData, h.Err = io.ReadAll(h.Resp.Body)
	defer h.Resp.Body.Close()
	if h.Err != nil {
		h.errPos = "Response ReadAll error"
		return
	}
}

// 连接池
var lock sync.RWMutex
var httpPool map[string]*http.Client = make(map[string]*http.Client)

func getHttpClient(host string) *http.Client {
	lock.RLock()
	if client, ok := httpPool[host]; ok {
		lock.RUnlock()
		return client
	}
	lock.RUnlock()

	client := &http.Client{
		Transport: &http.Transport{
			Dial: func(network, addr string) (net.Conn, error) {
				c, err := net.DialTimeout(network, addr, time.Second*3)
				if err != nil {
					return nil, err
				}
				return c, nil
			},
			MaxIdleConnsPerHost:   128,
			MaxIdleConns:          2048,
			IdleConnTimeout:       time.Second * 90,
			ExpectContinueTimeout: time.Second * 15,
			DisableKeepAlives:     true,
		},
		Timeout: time.Second * 8,
	}
	lock.Lock()
	//double check
	if _, ok := httpPool[host]; !ok {
		httpPool[host] = client
	}
	lock.Unlock()
	return client
}

func (h *HttpRequest) done(ctx context.Context) {
	h.Elapsed = time.Since(h.entry)

	// 日志
	if h.URL != nil {
		if !utils.CtxHasNolog(ctx) && zerolog.DebugLevel >= log.Logger.GetLevel() {
			if h.Err != nil {
				h.log(ctx, int(zerolog.ErrorLevel), h.Elapsed, "HttpRequest Err")
			} else {
				logLevel := ParamConf.Get().GetLogLevel(h.URL.Path, h.URL.Host)
				if logLevel >= int(log.Logger.GetLevel()) {
					h.log(ctx, logLevel, h.Elapsed, "HttpRequest")
				}
			}
		}
	}

	// 回调
	h.callhook(ctx)
}

func (h *HttpRequest) callhook(ctx context.Context) {
	defer utils.HandlePanic()
	// 回调
	for _, f := range httpHook {
		f(ctx, h)
	}
}

func (h *HttpRequest) log(ctx context.Context, logLevel int, elapsed time.Duration, msg string) {
	l := log.WithLevel(zerolog.Level(logLevel))
	if l == nil {
		return
	}

	logHeadOut := true
	if h.URL != nil {
		logLevelHead := ParamConf.Get().GetLogLevelHead(h.URL.Path, h.URL.Host)
		if logLevelHead == int(zerolog.Disabled) || logLevelHead < logLevel {
			logHeadOut = false
		}
	}

	l = utils.LogCtx(l, ctx)

	if h.Err != nil {
		l = l.Err(h.Err)
	}
	if len(h.errPos) > 0 {
		l = l.Str("errPos", h.errPos)
	}
	l = l.Int32("elapsed", int32(h.Elapsed/time.Millisecond)).Str("addr", h.Addr)
	if logHeadOut {
		l = l.Interface("reqheader", h.Header)
	}
	if h.Body != nil {
		l = utils.LogHttpInterface(l, h.Header, "req", h.Body, ParamConf.Get().LogMaxLimit)
	} else if len(h.Data) > 0 {
		l = utils.LogHttpBody(l, h.Header, "req", h.Data, ParamConf.Get().LogMaxLimit)
	}
	if h.Resp != nil {
		l.Int("status", h.Resp.StatusCode)
		if logHeadOut {
			l = l.Interface("respheader", h.Resp.Header)
		}
		if h.RespBody != nil {
			l = utils.LogHttpInterface(l, h.Resp.Header, "resp", h.RespBody, ParamConf.Get().LogMaxLimit)
		} else if len(h.RespData) > 0 {
			l = utils.LogHttpBody(l, h.Resp.Header, "resp", h.RespData, ParamConf.Get().LogMaxLimit)
		}
	}
	l.Msg(msg)
}
