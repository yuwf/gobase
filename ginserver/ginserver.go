package ginserver

// https://github.com/yuwf

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/yuwf/gobase/loader"
	"github.com/yuwf/gobase/utils"
)

// 参数配置
type ParamConfig struct {
	IgnorePath []string `json:"ignorepath,omitempty"` // handle函数日志忽略Path
}

var ParamConf loader.JsonLoader[ParamConfig]

// 请求参数绑定错误的回复，外部也可以定义定
var JsonParamBindError = map[string]interface{}{"errCode": 1, "errDesc": "Param Error"}

type GinServer struct {
	engine *gin.Engine
	server *http.Server
	state  int32 // 运行状态 0:未运行 1：开启监听

	// 请求处理完后回调 不使用锁，默认要求提前注册好
	hook []func(ctx context.Context, c *gin.Context, elapsed time.Duration)
}

func NewGinServer(port int) *GinServer {
	engine := gin.New()
	gs := &GinServer{
		engine: engine,
		server: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: engine,
		},
	}

	gs.engine.Use(
		gs.cors,
		gs.handle,
	)
	return gs
}

// Engine 暴露原始对象
func (gs *GinServer) Engine() *gin.Engine {
	return gs.engine
}

func (gs *GinServer) Start() error {
	if !atomic.CompareAndSwapInt32(&gs.state, 0, 1) {
		log.Error().Str("Addr", gs.server.Addr).Msg("GinServer already Start")
		return nil
	}

	ln, err := net.Listen("tcp", gs.server.Addr)
	if err != nil {
		log.Error().Err(err).Str("Addr", gs.server.Addr).Msg("GinServer Start err")
		return err
	}
	// 开启监听 gnet.Serve会阻塞
	go func() {
		err := gs.server.Serve(ln)
		if err == nil || err == http.ErrServerClosed {
			log.Info().Str("Addr", gs.server.Addr).Msg("GinServer Exit")
		} else if err != nil {
			atomic.StoreInt32(&gs.state, 0)
			log.Error().Err(err).Str("Addr", gs.server.Addr).Msg("GinServer Start Error")
		}
	}()

	log.Info().Str("Addr", gs.server.Addr).Msg("GinServer Start")
	return nil
}

func (gs *GinServer) StartTLS(certFile, keyFile string) error {
	if !atomic.CompareAndSwapInt32(&gs.state, 0, 1) {
		return nil
	}
	log.Info().Str("Addr", gs.server.Addr).Msg("GinServer StartTLS")
	// 开启监听 gnet.Serve会阻塞
	go func() {
		err := gs.server.ListenAndServeTLS(certFile, keyFile)
		if err == nil || err == http.ErrServerClosed {
			log.Info().Str("Addr", gs.server.Addr).Msg("GinServer TLS Exit")
		} else if err != nil {
			atomic.StoreInt32(&gs.state, 0)
			log.Error().Err(err).Str("Addr", gs.server.Addr).Msg("GinServer StartTLS Error")
		}
	}()
	return nil
}

func (gs *GinServer) Stop() error {
	if !atomic.CompareAndSwapInt32(&gs.state, 1, 0) {
		log.Error().Str("Addr", gs.server.Addr).Msg("GinServer already Close")
		return nil
	}
	log.Info().Str("Addr", gs.server.Addr).Msg("GinServer Closeing")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := gs.server.Shutdown(ctx)
	if err != nil {
		log.Error().Err(err).Str("Addr", gs.server.Addr).Msgf("GinServer Close error")
	}
	return nil
}

// 注册回调，需要fun调用回复
// method为空表示注册所有方法
// fun的参数支持以下写法
// (ctx context.Context, c *gin.Context)
// optionsHandlers会先执行 再执行fun调用
func (gs *GinServer) RegHandler(method, path string, fun interface{}, optionsHandlers ...gin.HandlerFunc) {
	if fun == nil {
		return
	}
	defer utils.HandlePanic()

	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	// 必须是函数
	if funType.Kind() != reflect.Func {
		log.Error().Msg("GinServer RegHandler, param must be function")
		return
	}
	// 必须有两个参数
	if funType.NumIn() != 2 {
		log.Error().Int("num", funType.NumIn()).Msg("GinServer RegHandler, fun param num must be 2")
		return
	}
	// 第一个参数必须是context.Context
	if funType.In(0).String() != "context.Context" {
		log.Error().Str("type", funType.In(0).String()).Msg("GinServer RegHandler, frist param num must be *context.Context")
		return
	}
	// 第二个参数必须是*gin.Context
	if funType.In(1).String() != "*gin.Context" {
		log.Error().Str("type", funType.In(1).String()).Msg("GinServer RegHandler, second param num must be *gin.Context")
		return
	}

	ginFun := func(c *gin.Context) {
		var ctx context.Context
		ctxv, ok := c.Get("ctx")
		if ok {
			ctx, _ = ctxv.(context.Context)
		}
		if ctx == nil {
			ctx = context.TODO()
		}
		// 注册函数调用
		funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(c)})
	}

	// 将函数加到最后
	optionsHandlers = append(optionsHandlers, ginFun)
	if len(method) == 0 {
		gs.engine.Any(path, optionsHandlers...)
	} else {
		gs.engine.Handle(method, path, optionsHandlers...)
	}
}

// 注册Json数据结构的回调，不需要fun调用回复
// method为空表示注册所有方法
// fun的参数支持以下写法
// (ctx context.Context, c *gin.Context, resp *RespStruct)
// (ctx context.Context, c *gin.Context, req *ReqStruct, resp *RespStruct)
// ReqStruct结构的格式可以是gin支持绑定的任意格式，需要在请求头中Content-Type指定具体格式，然后ReqStruct对应的写tag即可
// RespStruct必须是支持json格式化的结构
// optionsHandlers会先执行 再执行fun调用
func (gs *GinServer) RegJsonHandler(method, path string, fun interface{}, optionsHandlers ...gin.HandlerFunc) {
	if fun == nil {
		return
	}
	defer utils.HandlePanic()

	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	// 必须是函数
	if funType.Kind() != reflect.Func {
		log.Error().Msg("GinServer RegHttpHandler, param must be function")
		return
	}
	// 必须有三个或者四个参数
	if funType.NumIn() != 3 && funType.NumIn() != 4 {
		log.Error().Int("num", funType.NumIn()).Msg("GinServer RegHttpHandler, fun param num must be 3 or 4")
		return
	}
	// 第一个参数必须是context.Context
	if funType.In(0).String() != "context.Context" {
		log.Error().Str("type", funType.In(0).String()).Msg("GinServer RegHttpHandler, frist param num must be *context.Context")
		return
	}
	// 第二个参数必须是*gin.Context
	if funType.In(1).String() != "*gin.Context" {
		log.Error().Str("type", funType.In(1).String()).Msg("GinServer RegHttpHandler, second param num must be *gin.Context")
		return
	}
	// 第三和第四个参数必须是结构指针
	if funType.In(2).Kind() != reflect.Ptr || funType.In(2).Elem().Kind() != reflect.Struct {
		log.Error().Str("type", funType.In(2).String()).Msg("GinServer RegHttpHandler, third param num must be *Struct")
		return
	}
	if funType.NumIn() == 4 {
		// 第三个参数必须是结构指针
		if funType.In(3).Kind() != reflect.Ptr || funType.In(2).Elem().Kind() != reflect.Struct {
			log.Error().Str("type", funType.In(3).String()).Msg("GinServer RegHttpHandler, fourth param num must be *Struct")
			return
		}
	}

	ginFun := func(c *gin.Context) {
		var ctx context.Context
		ctxv, ok := c.Get("ctx")
		if ok {
			ctx, _ = ctxv.(context.Context)
		}
		if ctx == nil {
			ctx = context.TODO()
		}
		if funType.NumIn() == 4 {
			// 先解析传入的参数
			reqVal := reflect.New(funType.In(2).Elem())
			respVal := reflect.New(funType.In(3).Elem())
			c.Set("req", reqVal.Interface()) // 日志使用
			if funType.In(2).Elem().NumField() > 0 {
				err := c.ShouldBind(reqVal.Interface())
				if err != nil {
					c.JSON(http.StatusOK, JsonParamBindError)
					c.Set("err", err)
					c.Set("resp", JsonParamBindError) // 日志使用
					return
				}
			}
			// 注册函数调用
			funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(c), reqVal, respVal})
			// 回复
			c.JSON(http.StatusOK, respVal.Interface())
		} else {
			respVal := reflect.New(funType.In(2).Elem())
			c.Set("resp", respVal.Interface()) // 日志使用
			// 注册函数调用
			funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(c), respVal})
			// 回复
			c.JSON(http.StatusOK, respVal.Interface())
		}
	}

	// 将函数加到最后
	optionsHandlers = append(optionsHandlers, ginFun)
	if len(method) == 0 {
		gs.engine.Any(path, optionsHandlers...)
	} else {
		gs.engine.Handle(method, path, optionsHandlers...)
	}
}

// 注册无路由的处理逻辑
// fun的参数支持以下写法
// (ctx context.Context, c *gin.Context)
// optionsHandlers会先执行 再执行fun调用
func (gs *GinServer) RegNoRouteHandler(fun interface{}, optionsHandlers ...gin.HandlerFunc) {
	if fun == nil {
		return
	}
	defer utils.HandlePanic()

	funType := reflect.TypeOf(fun)
	funValue := reflect.ValueOf(fun)
	// 必须是函数
	if funType.Kind() != reflect.Func {
		log.Error().Msg("GinServer RegNoRouteHandler, param must be function")
		return
	}
	// 必须有两个参数
	if funType.NumIn() != 2 {
		log.Error().Int("num", funType.NumIn()).Msg("GinServer RegNoRouteHandler, fun param num must be 2")
		return
	}
	// 第一个参数必须是context.Context
	if funType.In(0).String() != "context.Context" {
		log.Error().Str("type", funType.In(0).String()).Msg("GinServer RegNoRouteHandler, frist param num must be *context.Context")
		return
	}
	// 第二个参数必须是*gin.Context
	if funType.In(1).String() != "*gin.Context" {
		log.Error().Str("type", funType.In(1).String()).Msg("GinServer RegNoRouteHandler, second param num must be *gin.Context")
		return
	}

	ginFun := func(c *gin.Context) {
		var ctx context.Context
		ctxv, ok := c.Get("ctx")
		if ok {
			ctx, _ = ctxv.(context.Context)
		}
		if ctx == nil {
			ctx = context.TODO()
		}
		// 注册函数调用
		funValue.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(c)})
	}
	// 将函数加到最后
	optionsHandlers = append(optionsHandlers, ginFun)
	gs.engine.NoRoute(optionsHandlers...)
}

func (gs *GinServer) RegHook(f func(ctx context.Context, c *gin.Context, elapsed time.Duration)) {
	gs.hook = append(gs.hook, f)
}

func (gs *GinServer) handle(c *gin.Context) {
	defer utils.HandlePanic()

	// 忽略的路径
	blog := true
	path := c.Request.URL.Path
	for _, v := range ParamConf.Get().IgnorePath {
		if path == v {
			blog = false
		}
	}
	entry := time.Now()
	// 日志 使用defer 保证崩溃了也能输出来
	logFun := func(elapsed time.Duration) {
		blog = false // 标记不用在输出了
		l := log.Info().Str("clientIP", c.ClientIP()).
			Str("method", c.Request.Method).
			Str("path", path).
			Interface("head", c.Request.Header)
		err, ok := c.Get("err")
		if ok {
			l = l.Interface("err", err)
		}
		req, ok := c.Get("req")
		if ok {
			l = l.Interface("body", req)
		} else if len(c.Request.URL.RawQuery) > 0 {
			l = l.Str("body", c.Request.URL.RawQuery)
		} else {
			data, _ := c.GetRawData()
			if len(data) > 0 {
				l = l.Bytes("body", data)
			}
		}
		l = l.Int("status", c.Writer.Status()).Int("elapsed", int(elapsed/time.Millisecond))
		resp, ok := c.Get("resp")
		if ok {
			l = l.Interface("resp", resp)
		} else {
			l = l.Int("respsize", c.Writer.Size())
		}
		l.Msg("GinServer handle")
	}
	defer func() {
		if blog { // 说明日志还没有调用 应该是蹦了
			logFun(time.Since(entry))
		}
	}()

	ctx := context.TODO()
	c.Set("ctx", ctx)

	c.Next()

	elapsed := time.Since(entry)
	if blog {
		logFun(elapsed)
	}

	// 回调
	for _, f := range gs.hook {
		f(ctx, c, elapsed)
	}
}

// CORS 跨域处理
// 官方提供了一个cors库 参考说明 https://mp.weixin.qq.com/s/GFX3sAgZqrxsGamJT-s4-g
func (gs *GinServer) cors(c *gin.Context) {
	c.Header("Access-Control-Allow-Origin", "*")
	if c.Request.Method == "OPTIONS" {
		//服务器支持的所有跨域请求的方法,为了避免浏览次请求的多次'预检'请求
		c.Header("Access-Control-Allow-Methods", "POST,GET,OPTIONS,PUT,DELETE,UPDATE")
		// header的类型
		allowHeaders := "content-type,x-qp-appid,x-qp-appversion,x-qp-clienttype,x-qp-gid,x-qp-nonce,x-qp-os,x-qp-osversion,x-qp-resversion,x-qp-signature,x-qp-timestamp,x-qp-token"
		reqHeader := c.Request.Header.Get("Access-Control-Request-Headers")
		if len(reqHeader) > 0 {
			allowHeaders = reqHeader
		}
		c.Header("Access-Control-Allow-Headers", allowHeaders)
		// 缓存请求信息 单位为秒
		c.Header("Access-Control-Max-Age", "86400")

		c.AbortWithStatus(http.StatusNoContent)
		return
	}
}
