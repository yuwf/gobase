package tcp

// https://github.com/yuwf/gobase

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"reflect"
	"sync/atomic"
	"syscall"
	"time"
)

// TCPListener 事件回调接口
type TCPListenerEvent interface {
	// 监听关闭
	OnShutdown()
	// 收到连接对象
	OnAccept(conn net.Conn)
}

type TCPListener struct {
	// 不可修改
	ListenAddr net.TCPAddr      //
	event      TCPListenerEvent // 事件回调接口
	TLSConfig  *tls.Config

	listener net.Listener

	state int32 // 注册状态 原子操作 0：未开启监听 1：已开启监听
}

func NewTCPListener(address string, event TCPListenerEvent) (*TCPListener, error) {
	// 检查下地址格式合法性
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}
	tl := &TCPListener{
		ListenAddr: *tcpAddr,
		event:      event,
	}
	return tl, nil
}

func NewTCPListenerTLSConfig(address string, event TCPListenerEvent, config *tls.Config) (*TCPListener, error) {
	// 检查下地址格式合法性
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}

	tl := &TCPListener{
		ListenAddr: *tcpAddr,
		event:      event,
		TLSConfig:  config,
	}
	return tl, nil
}

func (tl *TCPListener) Start(reuse bool) error {
	if !atomic.CompareAndSwapInt32(&tl.state, 0, 1) {
		return nil
	}

	l := &net.ListenConfig{}
	if reuse {
		l = &net.ListenConfig{Control: ReusePortControl}
	}
	listener, err := l.Listen(context.Background(), "tcp", tl.ListenAddr.String())
	if err != nil || listener == nil {
		return err
	}
	if tl.TLSConfig != nil {
		listener = tls.NewListener(listener, tl.TLSConfig)
	}
	tl.listener = listener

	// 开启循环
	go tl.loop()
	return nil
}

func (tl *TCPListener) Close() error {
	if !atomic.CompareAndSwapInt32(&tl.state, 1, 0) {
		return nil
	}

	return tl.listener.Close()
}

func (tl *TCPListener) Listening() bool {
	return atomic.LoadInt32(&tl.state) == 1
}

func (tl *TCPListener) loop() {
	//以下借鉴 net/http 区分停服和其他错误
	var tempDelay time.Duration // how long to sleep on accept failure
	for {
		c, err := tl.listener.Accept()
		if err != nil {
			if atomic.LoadInt32(&tl.state) == 0 {
				break
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				//log.Error().Err(err).Str("Addr", tl.listenAddr.String()).Msg("TCPListener Accept error, retrying")
				//延时调用
				time.Sleep(tempDelay)
				continue
			}
			break
		}
		if tl.event != nil {
			tl.event.OnAccept(c)
		}
	}
	if tl.event != nil {
		tl.event.OnShutdown()
	}
}

func ReusePortControl(network, address string, c syscall.RawConn) error {
	var opErr error
	err := c.Control(func(fd uintptr) {
		funType := reflect.TypeOf(syscall.SetsockoptInt)
		funValue := reflect.ValueOf(syscall.SetsockoptInt)
		v := reflect.New(funType.In(0))
		if v.Elem().Type().Kind() == reflect.Uintptr {
			v.Elem().SetUint(uint64(fd))
		} else if v.Elem().Type().Kind() == reflect.Int {
			v.Elem().SetInt(int64(fd))
		} else {
			panic(fmt.Sprintf("ReusePortControl error, %s", v.Elem().Type().Kind().String()))
		}
		r := funValue.Call([]reflect.Value{v.Elem(), reflect.ValueOf(syscall.SOL_SOCKET), reflect.ValueOf(syscall.SO_REUSEADDR), reflect.ValueOf(1)})
		if r[0].Interface() != nil {
			opErr = r[0].Interface().(error)
		}
	})
	if err != nil {
		return err
	}
	return opErr
}
