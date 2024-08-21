package gnetserver

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/panjf2000/gnet"

	"github.com/rs/zerolog/log"
)

// GNetServer
// ClientId客户端ID类型
// ClientInfo是和业务相关的客户端信息结构类型
type GNetServer[ClientId any, ClientInfo any] struct {
	*gnet.EventServer
	// 不可需改
	Address string // 监听地址
	Scheme  string // scheme支持tcp和ws，为空表示tcp

	event GNetEvent[ClientInfo] //event
	state int32                 // 运行状态 0:未运行 1：开启监听

	//所有的连接的客户端 [gnet.Conn:*gClient]
	connMap *sync.Map

	//外层添加的用户映射 [ClientId:*gClient]
	clientMap *sync.Map

	// 请求处理完后回调 不使用锁，默认要求提前注册好
	hook []GNetHook[ClientInfo]
}

// GNetClient过渡对象，便于保存id，减少GNetClient的复杂度
type gClient[ClientId any, ClientInfo any] struct {
	gc *GNetClient[ClientInfo]
	id ClientId // 调用GNetServer.AddClient设置的id 目前无锁 不存在复杂使用
	// 回调修改，目前认为回调对单个连接对象是协程安全的
	lastActiveTime time.Time // 最近一次活跃的时间
}

// 创建服务器
// scheme支持tcp和ws，为空表示tcp
func NewGNetServer[ClientId any, ClientInfo any](port int, event GNetEvent[ClientInfo]) *GNetServer[ClientId, ClientInfo] {
	s := &GNetServer[ClientId, ClientInfo]{
		Address:     fmt.Sprintf("tcp://:%d", port),
		Scheme:      "tcp",
		event:       event,
		state:       0,
		EventServer: &gnet.EventServer{},
		connMap:     new(sync.Map),
		clientMap:   new(sync.Map),
	}
	return s
}

func NewGNetServerWS[ClientId any, ClientInfo any](port int, event GNetEvent[ClientInfo]) *GNetServer[ClientId, ClientInfo] {
	s := &GNetServer[ClientId, ClientInfo]{
		Address:     fmt.Sprintf("tcp://:%d", port),
		Scheme:      "ws",
		event:       event,
		state:       0,
		EventServer: &gnet.EventServer{},
		connMap:     new(sync.Map),
		clientMap:   new(sync.Map),
	}
	return s
}

// 开启监听
func (s *GNetServer[ClientId, ClientInfo]) Start() error {
	if !atomic.CompareAndSwapInt32(&s.state, 0, 1) {
		log.Error().Str("Addr", s.Address).Msg("GNetServer already Start")
		return nil
	}
	log.Info().Str("Addr", s.Address).Msg("GNetServer Starting")
	// 开启监听 gnet.Serve会阻塞
	go func() {
		err := gnet.Serve(s, s.Address,
			gnet.WithMulticore(true),
			gnet.WithTCPKeepAlive(time.Minute*2),
			gnet.WithCodec(s),
			gnet.WithReusePort(true),
			gnet.WithTicker(true))

		if err == nil {
			log.Info().Str("Addr", s.Address).Msg("GNetServer Exist")
		} else {
			atomic.StoreInt32(&s.state, 0)
			log.Error().Err(err).Str("Addr", s.Address).Msg("GNetServer Start error")
		}
	}()
	return nil
}

func (s *GNetServer[ClientId, ClientInfo]) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.state, 1, 0) {
		log.Error().Str("Addr", s.Address).Msg("GNetServer already Stop")
		return nil
	}
	log.Info().Str("Addr", s.Address).Msg("GNetServer Stoping")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := gnet.Stop(ctx, s.Address) // 会回调所有连接的的OnClosed 和 OnShutdown
	if err != nil {
		log.Error().Err(err).Str("Addr", s.Address).Msg("GNetServer Stop error")
	}
	return nil
}

// 添加用户映射
func (s *GNetServer[ClientId, ClientInfo]) AddClient(id ClientId, gc *GNetClient[ClientInfo]) {
	// 先检查下是否存在连接
	client, ok := s.connMap.Load(gc.conn)
	if !ok {
		return
	}
	client.(*gClient[ClientId, ClientInfo]).id = id
	s.clientMap.Store(id, client)

	// 回调回调hook
	for _, h := range s.hook {
		h.OnAddClient(gc)
	}
}

func (s *GNetServer[ClientId, ClientInfo]) GetClient(id ClientId) *GNetClient[ClientInfo] {
	client, ok := s.clientMap.Load(id)
	if ok {
		return client.(*gClient[ClientId, ClientInfo]).gc
	}
	return nil
}

func (s *GNetServer[ClientId, ClientInfo]) RemoveClient(id ClientId) *GNetClient[ClientInfo] {
	client, ok := s.clientMap.Load(id)
	if ok {
		s.clientMap.Delete(id)
		gc := client.(*gClient[ClientId, ClientInfo]).gc

		// 回调hook
		for _, h := range s.hook {
			h.OnRemoveClient(gc)
		}
		return gc
	}
	return nil
}

// 主动关闭 不会回调事件的OnDisConnect
// 使用GNetClient.Close会回调事件的OnDisConnect
func (s *GNetServer[ClientId, ClientInfo]) CloseClient(id ClientId) {
	client, ok := s.clientMap.Load(id)
	if ok {
		gc := client.(*gClient[ClientId, ClientInfo]).gc
		log.Info().Str("Name", gc.ConnName()).Msg("OnClosed CloseClient") // 日志为GNetServer OnClosed 便于和下面的OnClosed统一查找
		s.connMap.Delete(gc.conn)
		s.clientMap.Delete(id)
		gc.Close(nil) // 会回调GNetServer的OnClosed 所以上面先删除对象

		// 回调
		for _, h := range s.hook {
			h.OnDisConnect(gc, true, nil)
		}
	}
}

// 遍历Client f函数返回false 停止遍历
func (s *GNetServer[ClientId, ClientInfo]) RangeClient(f func(gc *GNetClient[ClientInfo]) bool) {
	s.connMap.Range(func(key, value interface{}) bool {
		gclient := value.(*gClient[ClientId, ClientInfo])
		gc := gclient.gc
		return f(gc)
	})
}

func (s *GNetServer[ClientId, ClientInfo]) Send(ctx context.Context, id ClientId, data []byte) error {
	client, ok := s.clientMap.Load(id)
	if ok {
		return client.(*gClient[ClientId, ClientInfo]).gc.Send(ctx, data)
	}
	err := fmt.Errorf("not exist client %v", id)
	utils.LogCtx(log.Debug(), ctx).Err(err).Int("Size", len(data)).Msg("Send error")
	return err
}

func (s *GNetServer[ClientId, ClientInfo]) SendMsg(ctx context.Context, id ClientId, msg utils.SendMsger) error {
	client, ok := s.clientMap.Load(id)
	if ok {
		return client.(*gClient[ClientId, ClientInfo]).gc.SendMsg(ctx, msg)
	}
	err := fmt.Errorf("not exist client %v", id)
	utils.LogCtx(log.Debug(), ctx).Err(err).Interface("Msg", msg).Msg("SendMsg error")
	return err
}

func (s *GNetServer[ClientId, ClientInfo]) ConnCount() (int, int) {
	count := 0
	handshakecount := 0
	s.connMap.Range(func(key, value interface{}) bool {
		count++
		gc := value.(*gClient[ClientId, ClientInfo]).gc
		if gc.wsh != nil && gc.wsh.upgrade {
			handshakecount++
		}
		return true
	})
	return count, handshakecount
}

func (s *GNetServer[ClientId, ClientInfo]) ClientCount() int {
	count := 0
	s.clientMap.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// 注册hook
func (s *GNetServer[ClientId, ClientInfo]) RegHook(h GNetHook[ClientInfo]) {
	s.hook = append(s.hook, h)
}

// 编解码原样返回
func (s *GNetServer[ClientId, ClientInfo]) Encode(c gnet.Conn, buf []byte) ([]byte, error) {
	// 回调
	client, ok := s.connMap.Load(c)
	if ok {
		gc := client.(*gClient[ClientId, ClientInfo]).gc
		for _, h := range s.hook {
			h.OnSend(gc, len(buf))
		}
	}
	return buf, nil
}

// 返回值[]byte 就是React的packet
func (s *GNetServer[ClientId, ClientInfo]) Decode(c gnet.Conn) ([]byte, error) {
	buf := c.Read()
	if len(buf) == 0 {
		return nil, nil
	}

	// 回调
	client, ok := s.connMap.Load(c)
	if ok {
		gc := client.(*gClient[ClientId, ClientInfo]).gc
		for _, h := range s.hook {
			h.OnRecv(gc, len(buf))
		}
	}
	return buf, nil
}

func (s *GNetServer[ClientId, ClientInfo]) OnInitComplete(server gnet.Server) (action gnet.Action) {
	log.Info().Str("Addr", server.Addr.String()).Msg("GNetServer InitComplete")
	return
}

func (s *GNetServer[ClientId, ClientInfo]) OnShutdown(server gnet.Server) {
	log.Info().Str("Addr", server.Addr.String()).Msg("GNetServer Shutdown")
}

func (s *GNetServer[ClientId, ClientInfo]) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	logOut := !ParamConf.Get().IsIgnoreIp(c.RemoteAddr().String())
	if logOut {
		log.Info().Str("RemoveAddr", c.RemoteAddr().String()).Str("LocalAddr", c.LocalAddr().String()).Msg("OnOpened")
	}

	gc := newGNetClient(c, s.event, s.hook)
	if s.Scheme == "ws" {
		gc.ctx = context.WithValue(gc.ctx, CtxKey_WS, 1)
		gc.wsh = newGNetWSHandler(gc)
	}
	client := &gClient[ClientId, ClientInfo]{
		gc:             gc,
		lastActiveTime: time.Now(),
	}
	// 如果clientName还为空 就用client里面的id来表示
	if client.gc.connName == nil {
		gc.connName = func() string {
			name := fmt.Sprintf("%v", client.id)
			if len(name) == 0 || name == "0" {
				return gc.removeAddr.String()
			}
			return name
		}
	}
	s.connMap.Store(c, client)
	if s.event != nil {
		s.event.OnConnected(gc.ctx, gc)
	}
	// 回调
	for _, h := range s.hook {
		h.OnConnected(gc)
	}
	return
}

func (s *GNetServer[ClientId, ClientInfo]) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	client, ok := s.connMap.Load(c)
	if ok {
		gc := client.(*gClient[ClientId, ClientInfo]).gc
		logOut := !ParamConf.Get().IsIgnoreIp(gc.removeAddr.String())
		if logOut {
			if gc.closeReason != nil {
				err = gc.closeReason
			}
			log.Info().Err(err).Str("Name", gc.ConnName()).Str("RemoveAddr", gc.removeAddr.String()).Msg("OnClosed")
		}
		s.connMap.Delete(c)
		_, delClient := s.clientMap.LoadAndDelete(client.(*gClient[ClientId, ClientInfo]).id)
		if s.event != nil {
			s.event.OnDisConnect(gc.ctx, gc)
		}

		// 回调
		for _, h := range s.hook {
			h.OnDisConnect(gc, delClient, err)
		}
	}
	return
}

func (s *GNetServer[ClientId, ClientInfo]) React(packet []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	if c.BufferLength() == 0 {
		return
	}
	client, ok := s.connMap.Load(c)
	if ok {
		gclient := client.(*gClient[ClientId, ClientInfo])
		gc := gclient.gc
		gclient.lastActiveTime = time.Now()
		// 是否websock
		if gc.wsh != nil {
			len, handshake, err := gc.wsh.recv(c.Read())
			if err != nil {
				c.ResetBuffer() // 先调用下，防止gnet.Conn有数据还会继续调用
				gc.Close(err)
				return
			}

			if handshake {
				// 查找真正的ip
				addr := utils.ClientTCPIPHeader(gc.wsh.Header)
				if addr != nil {
					gc.removeAddr = *addr
				}
				log.Info().Str("RemoveAddr", gc.removeAddr.String()+"("+gc.conn.RemoteAddr().String()+")").Interface("Header", gc.wsh.Header).Msg("HandShake")

				// 回调
				for _, h := range s.hook {
					h.OnWSHandShake(gc)
				}
			}
			c.ShiftN(len)
		} else {
			len, err := gc.recv(gc.ctx, c.Read())
			if err != nil {
				c.ResetBuffer() // 先调用下，防止gnet.Conn有数据还会继续调用
				gc.Close(err)
				return
			}
			c.ShiftN(len)
		}
	}
	return
}

func (s *GNetServer[ClientId, ClientInfo]) Tick() (delay time.Duration, action gnet.Action) {
	delay = time.Second
	if s.event != nil {
		s.connMap.Range(func(key, value interface{}) bool {
			gclient := value.(*gClient[ClientId, ClientInfo])
			gc := gclient.gc
			timeout := 0
			if ParamConf.Get().ActiveTimeout > 0 {
				timeout = ParamConf.Get().ActiveTimeout
			}
			if timeout > 0 && time.Since(gclient.lastActiveTime) > time.Second*time.Duration(timeout) {
				gc.Close(errors.New("activetimeout"))
			} else {
				ctx := context.WithValue(gc.ctx, utils.CtxKey_traceId, utils.GenTraceID())
				ctx = context.WithValue(ctx, utils.CtxKey_msgId, "_tick_")
				if ParamConf.Get().MsgSeq {
					gc.seq.Submit(func() {
						s.event.OnTick(ctx, gc)
					})
				} else {
					utils.Submit(func() {
						s.event.OnTick(ctx, gc)
					})
				}
			}
			return true
		})
	}
	return
}
