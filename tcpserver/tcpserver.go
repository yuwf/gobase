package tcpserver

// https://github.com/yuwf/gobase

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"gobase/tcp"
	"gobase/utils"

	"github.com/rs/zerolog/log"
)

// TCPServer
// ClientId客户端ID类型
// ClientInfo是和业务相关的客户端信息结构类型
type TCPServer[ClientId any, ClientInfo any] struct {
	*tcp.TCPConnEvenHandle
	// 不可需改
	Address string // 监听地址
	Scheme  string // scheme支持tcp和ws，为空表示tcp

	event TCPEvent[ClientInfo] // event
	state int32                // 运行状态 0:未运行 1：开启监听

	// 监听对象
	listener *tcp.TCPListener

	//所有的连接的客户端 [tcp.TCPConn:*gClient]
	connMap *sync.Map

	//外层添加的用户映射 [ClientId:*gClient]
	clientMap *sync.Map

	// 请求处理完后回调 不使用锁，默认要求提前注册好
	hook []TCPHook[ClientInfo]

	// 外部要求退出
	quit chan int // 退出chan 外部写 内部读
}

// TCPClient过渡对象，便于保存id，减少TCPClient的复杂度
type tClient[ClientId any, ClientInfo any] struct {
	tc *TCPClient[ClientInfo]
	id ClientId // 调用GNetServer.AddClient设置的id 目前无锁 不存在复杂使用
	// 回调修改，目前认为回调对单个连接对象是协程安全的
	lastActiveTime time.Time // 最近一次活跃的时间
}

// 创建服务器
func NewTCPServer[ClientId any, ClientInfo any](port int, event TCPEvent[ClientInfo]) (*TCPServer[ClientId, ClientInfo], error) {
	s := &TCPServer[ClientId, ClientInfo]{
		TCPConnEvenHandle: new(tcp.TCPConnEvenHandle),
		Address:           fmt.Sprintf(":%d", port),
		event:             event,
		state:             0,
		connMap:           new(sync.Map),
		clientMap:         new(sync.Map),
		quit:              make(chan int),
	}

	var err error
	s.listener, err = tcp.NewTCPListener(s.Address, s)
	if err != nil {
		log.Error().Err(err).Str("Addr", s.Address).Msg("NewTCPServer error")
		return nil, err
	}
	// 开启tick协程
	go s.loopTick()
	return s, nil
}

func NewTCPServerWithWS[ClientId any, ClientInfo any](port int, event TCPEvent[ClientInfo], certFile, keyFile string) (*TCPServer[ClientId, ClientInfo], error) {
	s := &TCPServer[ClientId, ClientInfo]{
		TCPConnEvenHandle: new(tcp.TCPConnEvenHandle),
		Address:           fmt.Sprintf(":%d", port),
		Scheme:            "ws",
		event:             event,
		state:             0,
		connMap:           new(sync.Map),
		clientMap:         new(sync.Map),
		quit:              make(chan int),
	}

	if certFile != "" || keyFile != "" {
		var err error
		config := &tls.Config{}
		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Error().Err(err).Str("Addr", s.Address).Msg("NewTCPServerWithWS error")
			return nil, err
		}
		s.listener, err = tcp.NewTCPListenerTLSConfig(s.Address, s, config)
		if err != nil {
			log.Error().Err(err).Str("Addr", s.Address).Msg("NewTCPServerWithWS error")
			return nil, err
		}
	} else {
		var err error
		s.listener, err = tcp.NewTCPListener(s.Address, s)
		if err != nil {
			log.Error().Err(err).Str("Addr", s.Address).Msg("NewTCPServerWithWS error")
			return nil, err
		}
	}
	// 开启tick协程
	go s.loopTick()
	return s, nil
}

func NewTCPServerWithWS2[ClientId any, ClientInfo any](port int, event TCPEvent[ClientInfo], certPEMBlock, keyPEMBlock []byte) (*TCPServer[ClientId, ClientInfo], error) {
	s := &TCPServer[ClientId, ClientInfo]{
		TCPConnEvenHandle: new(tcp.TCPConnEvenHandle),
		Address:           fmt.Sprintf(":%d", port),
		Scheme:            "ws",
		event:             event,
		state:             0,
		connMap:           new(sync.Map),
		clientMap:         new(sync.Map),
		quit:              make(chan int),
	}

	if len(certPEMBlock) > 0 || len(keyPEMBlock) > 0 {
		var err error
		config := &tls.Config{}
		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates[0], err = tls.X509KeyPair(certPEMBlock, keyPEMBlock)
		if err != nil {
			log.Error().Err(err).Str("Addr", s.Address).Msg("NewTCPServerWithWS2 error")
			return nil, err
		}
		s.listener, err = tcp.NewTCPListenerTLSConfig(s.Address, s, config)
		if err != nil {
			log.Error().Err(err).Str("Addr", s.Address).Msg("NewTCPServerWithWS error")
			return nil, err
		}
	} else {
		var err error
		s.listener, err = tcp.NewTCPListener(s.Address, s)
		if err != nil {
			log.Error().Err(err).Str("Addr", s.Address).Msg("NewTCPServerWithWS error")
			return nil, err
		}
	}
	// 开启tick协程
	go s.loopTick()
	return s, nil
}

// 开启监听
func (s *TCPServer[ClientId, ClientInfo]) Start(reusePort bool) error {
	if s.listener == nil {
		err := errors.New("listener is nil")
		log.Error().Err(err).Str("Addr", s.Address).Msg("TCPServer Start error")
		return err
	}

	if !atomic.CompareAndSwapInt32(&s.state, 0, 1) {
		log.Error().Str("Addr", s.Address).Msg("TCPServer already Start")
		return nil
	}

	err := s.listener.Start(reusePort)
	if err != nil {
		atomic.StoreInt32(&s.state, 0)
		log.Error().Str("Addr", s.Address).Msg("TCPServer already Start")
		return err
	}

	log.Info().Str("Addr", s.Address).Msg("TCPServer Start")

	return nil
}

// 关闭监听
func (s *TCPServer[ClientId, ClientInfo]) Stop() error {
	if !atomic.CompareAndSwapInt32(&s.state, 1, 0) {
		log.Error().Str("Addr", s.Address).Msg("TCPServer already Stop")
		return nil
	}

	// 关闭监听
	err := s.listener.Close()
	if err != nil {
		log.Error().Err(err).Str("Addr", s.Address).Msg("TCPServer Stop error")
	}

	log.Info().Str("Addr", s.Address).Msg("TCPServer Stop")
	return nil
}

// 添加用户映射
func (s *TCPServer[ClientId, ClientInfo]) AddClient(id ClientId, tc *TCPClient[ClientInfo]) {
	// 先检查下是否存在连接
	client, ok := s.connMap.Load(tc.conn)
	if !ok {
		return
	}
	client.(*tClient[ClientId, ClientInfo]).id = id
	s.clientMap.Store(id, client)

	// 回调回调hook
	for _, h := range s.hook {
		h.OnAddClient(tc)
	}
}

func (s *TCPServer[ClientId, ClientInfo]) GetClient(id ClientId) *TCPClient[ClientInfo] {
	client, ok := s.clientMap.Load(id)
	if ok {
		return client.(*tClient[ClientId, ClientInfo]).tc
	}
	return nil
}

func (s *TCPServer[ClientId, ClientInfo]) RemoveClient(id ClientId) *TCPClient[ClientInfo] {
	client, ok := s.clientMap.Load(id)
	if ok {
		s.clientMap.Delete(id)
		tc := client.(*tClient[ClientId, ClientInfo]).tc

		// 回调hook
		for _, h := range s.hook {
			h.OnRemoveClient(tc)
		}
		return tc
	}
	return nil
}

// 主动关闭 不会回调event的OnDisConnect
// 使用TCPClient.Close会回调OnDisConnect
func (s *TCPServer[ClientId, ClientInfo]) CloseClient(id ClientId) {
	client, ok := s.clientMap.Load(id)
	if ok {
		tc := client.(*tClient[ClientId, ClientInfo]).tc
		log.Info().Str("Name", tc.ConnName()).Msg("OnClosed CloseClient") // 日志为OnClosed 便于和下面的OnClosed统一查找
		s.connMap.Delete(tc.conn)
		s.clientMap.Delete(id)
		tc.Close(nil) // 会回调TCPServer的OnClosed 所以上面先删除对象
		tc.clear()

		// 回调
		for _, h := range s.hook {
			h.OnDisConnect(tc, true, nil)
		}
	}
}

// 遍历Client f函数返回false 停止遍历
func (s *TCPServer[ClientId, ClientInfo]) RangeClient(f func(tc *TCPClient[ClientInfo]) bool) {
	s.connMap.Range(func(key, value interface{}) bool {
		tclient := value.(*tClient[ClientId, ClientInfo])
		tc := tclient.tc
		return f(tc)
	})
}

func (s *TCPServer[ClientId, ClientInfo]) Send(id ClientId, data []byte) error {
	client, ok := s.clientMap.Load(id)
	if ok {
		return client.(*tClient[ClientId, ClientInfo]).tc.Send(data)
	}
	err := fmt.Errorf("not exist client %v", id)
	log.Debug().Err(err).Int("Size", len(data)).Msg("Send error")
	return err
}

func (s *TCPServer[ClientId, ClientInfo]) SendMsg(id ClientId, msg interface{}) error {
	client, ok := s.clientMap.Load(id)
	if ok {
		return client.(*tClient[ClientId, ClientInfo]).tc.SendMsg(msg)
	}
	err := fmt.Errorf("not exist client %v", id)
	log.Debug().Err(err).Interface("Msg", msg).Msg("SendMsg error")
	return err
}

func (s *TCPServer[ClientId, ClientInfo]) ConnCount() (int, int) {
	count := 0
	s.connMap.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count, 0
}

func (s *TCPServer[ClientId, ClientInfo]) ClientCount() int {
	count := 0
	s.clientMap.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// 注册hook
func (s *TCPServer[ClientId, ClientInfo]) RegHook(h TCPHook[ClientInfo]) {
	s.hook = append(s.hook, h)
}

func (s *TCPServer[ClientId, ClientInfo]) OnShutdown() {
	log.Info().Str("addr", s.Address).Msg("Shutdown")
}

func (s *TCPServer[ClientId, ClientInfo]) OnAccept(c net.Conn) {
	logOut := !ParamConf.Get().IsIgnoreIp(c.RemoteAddr().String())
	if logOut {
		log.Info().Str("RemoveAddr", c.RemoteAddr().String()).Msg("OnAccept")
	}

	conn, _ := tcp.NewTCPConned(c, s)
	tc := newTCPClient(conn, s.event)
	if s.Scheme == "ws" {
		tc.ctx = context.WithValue(tc.ctx, CtxKey_WS, 1)
		tc.wsh = newTCPWSHandler(tc)
	}
	client := &tClient[ClientId, ClientInfo]{
		tc:             tc,
		lastActiveTime: time.Now(),
	}
	// 如果clientName还为空 就用client里面的id来表示
	if client.tc.connName == nil {
		tc.connName = func() string {
			name := fmt.Sprintf("%v", client.id)
			if len(name) == 0 || name == "0" {
				return tc.removeAddr.String()
			}
			return name
		}
	}
	s.connMap.Store(conn, client)
	if s.event != nil {
		s.event.OnConnected(tc.ctx, tc)
	}

	// 回调
	for _, h := range s.hook {
		h.OnConnected(tc)
	}
}

func (s *TCPServer[ClientId, ClientInfo]) OnDisConnect(err error, c *tcp.TCPConn) error {
	client, ok := s.connMap.Load(c)
	if ok {
		tc := client.(*tClient[ClientId, ClientInfo]).tc
		logOut := !ParamConf.Get().IsIgnoreIp(tc.removeAddr.String())
		if logOut {
			if tc.closeReason != nil {
				err = tc.closeReason
			}
			log.Info().Err(err).Str("Name", tc.ConnName()).Str("RemoveAddr", tc.removeAddr.String()).Msg("OnDisConnect")
		}
		s.connMap.Delete(c)
		_, delClient := s.clientMap.LoadAndDelete(client.(*tClient[ClientId, ClientInfo]).id)
		tc.clear()
		if s.event != nil {
			s.event.OnDisConnect(tc.ctx, tc)
		}

		// 回调
		for _, h := range s.hook {
			h.OnDisConnect(tc, delClient, err)
		}
	}
	return nil
}

func (s *TCPServer[ClientId, ClientInfo]) OnClose(c *tcp.TCPConn) {
	client, ok := s.connMap.Load(c)
	if ok {
		tc := client.(*tClient[ClientId, ClientInfo]).tc
		logOut := !ParamConf.Get().IsIgnoreIp(tc.removeAddr.String())
		if logOut {
			log.Info().Err(tc.closeReason).Str("Name", tc.ConnName()).Str("RemoveAddr", tc.removeAddr.String()).Msg("OnClosed")
		}
		s.connMap.Delete(c)
		_, delClient := s.clientMap.LoadAndDelete(client.(*tClient[ClientId, ClientInfo]).id)
		tc.clear()
		if s.event != nil {
			s.event.OnDisConnect(tc.ctx, tc)
		}

		// 回调
		for _, h := range s.hook {
			h.OnDisConnect(tc, delClient, tc.closeReason)
		}
	}
}

func (s *TCPServer[ClientId, ClientInfo]) OnRecv(data []byte, c *tcp.TCPConn) (int, error) {
	client, ok := s.connMap.Load(c)
	if ok {
		tclient := client.(*tClient[ClientId, ClientInfo])
		tc := tclient.tc
		tclient.lastActiveTime = time.Now()
		// 回调
		for _, h := range s.hook {
			h.OnRecv(tc, len(data))
		}

		// 是否websock
		if tc.wsh != nil {
			len, handshake, err := tc.wsh.recv(data)
			if handshake {
				// 查找真正的ip
				addr := utils.ClientTCPIPHeader(tc.wsh.Header)
				if addr != nil {
					tc.removeAddr = *addr
				}
				log.Info().Str("RemoveAddr", tc.removeAddr.String()+"("+tc.conn.RemoteAddr().String()+")").Interface("Header", tc.wsh.Header).Msg("HandShake")

				// 回调
				for _, h := range s.hook {
					h.OnWSHandShake(tc)
				}
			}
			return len, err // 返回err后会关闭连接，并调用OnDisConnect
		} else {
			len, err := tc.recv(tc.ctx, data)
			return len, err // 返回err后会关闭连接，并调用OnDisConnect
		}
	}
	return len(data), nil
}

func (s *TCPServer[ClientId, ClientInfo]) OnSend(data []byte, c *tcp.TCPConn) ([]byte, error) {
	// 回调
	client, ok := s.connMap.Load(c)
	if ok {
		tc := client.(*tClient[ClientId, ClientInfo]).tc
		for _, h := range s.hook {
			h.OnSend(tc, len(data))
		}
	}
	return data, nil
}

func (s *TCPServer[ClientId, ClientInfo]) loopTick() {
	for {
		// 每秒tick下
		timer := time.NewTimer(time.Second)
		select {
		case <-s.quit:
			if !timer.Stop() {
				select {
				case <-timer.C: // try to drain the channel
				default:
				}
			}
			s.quit <- 1 // 反写让Close退出
			return
		case <-timer.C:
		}
		if s.event != nil {
			s.connMap.Range(func(key, value interface{}) bool {
				tclient := value.(*tClient[ClientId, ClientInfo])
				tc := tclient.tc
				timeout := 0
				if ParamConf.Get().ActiveTimeout > 0 {
					timeout = ParamConf.Get().ActiveTimeout
				}
				if timeout > 0 && time.Since(tclient.lastActiveTime) > time.Second*time.Duration(timeout) {
					tc.Close(errors.New("activetimeout"))
				} else {
					if tc.seq != nil {
						tc.seq.Submit(func() {
							s.event.OnTick(tc.ctx, tc)
						})
					} else {
						utils.Submit(func() {
							s.event.OnTick(tc.ctx, tc)
						})
					}
				}
				return true
			})
		}
	}
}
