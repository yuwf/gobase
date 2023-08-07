package httprequest

// https://github.com/yuwf

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/yuwf/gobase/loader"
)

const CtxKey_nolog = "nolog" // 不打印掉应该能日志，错误日志还会打印 值：不受限制 一般写1

// 参数配置
type ParamConfig struct {
	IgnoreHost []string `json:"ignorehost,omitempty"` // 日志忽略Host
	IgnorePath []string `json:"ignorepath,omitempty"` // 日志忽略Path
}

var ParamConf loader.JsonLoader[ParamConfig]

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
	_, h.Err = url.Parse(h.Addr)
	if h.Err != nil {
		h.errPos = "Parse Addr error"
		return
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

	// 请求
	client := http.Client{
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
		},
		Timeout: time.Second * 8,
	}
	h.Resp, h.Err = client.Do(request)
	if h.Err != nil {
		h.errPos = "Request Do error"
		return
	}

	// 读取返回值
	h.RespData, h.Err = ioutil.ReadAll(h.Resp.Body)
	defer h.Resp.Body.Close()
	if h.Err != nil {
		h.errPos = "Response ReadAll error"
		return
	}
}

func (h *HttpRequest) done(ctx context.Context) {
	h.Elapsed = time.Since(h.entry)

	// 日志是否输出
	logOut := true
	if ctx != nil && ctx.Value(CtxKey_nolog) != nil {
		logOut = false
	}

	// 忽略的路径
	if logOut && h.URL != nil {
		for _, v := range ParamConf.Get().IgnoreHost {
			if h.URL.Host == v {
				logOut = false
			}
		}
		if logOut {
			for _, v := range ParamConf.Get().IgnorePath {
				if h.URL.Path == v {
					logOut = false
				}
			}
		}
	}

	if logOut {
		// 日志输出
		var l *zerolog.Event
		if h.Err != nil {
			l = log.Error().Err(h.Err).Int32("elapsed", int32(h.Elapsed/time.Millisecond))
		} else {
			l = log.Info().Int32("elapsed", int32(h.Elapsed/time.Millisecond))
		}
		l = l.Str("addr", h.Addr).Interface("header", h.Header)
		if h.Body != nil {
			l = l.Interface("body", h.Body)
		} else if len(h.Data) > 0 {
			l = l.Bytes("body", h.Data)
		}
		if h.Resp != nil {
			l.Int("status", h.Resp.StatusCode)
			if h.RespBody != nil {
				l = l.Interface("resp", h.RespBody)
			} else if len(h.RespData) > 0 {
				l = l.Bytes("resp", h.RespData)
			}
		}
		if len(h.errPos) > 0 {
			l.Msgf("HttpRequest %s", h.errPos)
		} else {
			l.Msg("HttpRequest")
		}
	}

	// 回调
	for _, f := range httpHook {
		f(ctx, h)
	}
}
