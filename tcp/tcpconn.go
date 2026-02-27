package tcp

// https://github.com/yuwf/gobase

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/yuwf/gobase/utils"
)

const (
	TCPStateInvalid    = 0 // 无效
	TCPStateConnecting = 1 // 连接中
	TCPStateConnected  = 2 // 连接成功
	TCPStateStop       = 3 // 外部要求退出了
	TCPStateRWExit     = 4 // 读写异常退出
)

// TCPConn 事件回调接口
type TCPConnEvent interface {
	// OnDialFail 连接失败，等待下次连接 拨号模式调用, 返回nil会再次自动重连，否则不重连
	OnDialFail(err error, tc *TCPConn) error
	// OnDialSuccess 连接成功 拨号模式调用
	OnDialSuccess(tc *TCPConn)

	// OnDisConnect 失去连接，主动调用Close也会调用，拨号模式返回nil会自动重连，否则不自动重连，调用后会清空还未发送的消息队列
	OnDisConnect(err error, tc *TCPConn) error
	// OnRecv 收到数据，处理层返回处理数据的长度，返回error将失去连接
	OnRecv(data []byte, tc *TCPConn) (int, error)
	// OnSend 发送数据，返回error将失去连接
	OnSend(data []byte, tc *TCPConn) ([]byte, error)
}

// TCPConnEvenHandle TCPConnEvent的内置实现
// 如果不想实现TCPConnEvent的所有接口，可以继承它实现部分方法
type TCPConnEvenHandle struct {
}

func (*TCPConnEvenHandle) OnDialFail(err error, tc *TCPConn) error {
	return nil
}
func (*TCPConnEvenHandle) OnDialSuccess(tc *TCPConn) {
}
func (*TCPConnEvenHandle) OnDisConnect(err error, tc *TCPConn) error {
	return nil
}
func (*TCPConnEvenHandle) OnRecv(data []byte, tc *TCPConn) (int, error) {
	return len(data), nil
}
func (*TCPConnEvenHandle) OnSend(data []byte, tc *TCPConn) ([]byte, error) {
	return data, nil
}

// TCPConn tcp连接对象 协程安全
type TCPConn struct {
	// 不可修改
	dialMode   bool // 拨号模式 主动连接对象使用
	removeAddr net.TCPAddr
	localAddr  net.TCPAddr  // 拨号模式 非协程安全，待解决，实际情况是修改localAddr时，一般外部没有回调，不会访问localAddr
	event      TCPConnEvent // 事件回调接口

	// 只有run协程负责修改
	state int32    // 连接状态 原子操作，只有loop协程负责修改
	conn  net.Conn // 连接对象

	mq chan []byte // 写消息队列

	// 外部要求退出
	quit      chan struct{} // 退出chan 外部写 内部读
	quitState int32         // 标记是否退出，原子操作, <0内部退出 >0外部Close 0：未退出 1：表示等待退出 2：表示不等待退出
	closed    chan struct{} // 关闭chan 内部写 外部读
	reconn    chan struct{} // 重新连接 外部写 内部读 只有在拨号模式下才有效
}

// NewTCPConn 创建TCP网络主动连接对象，拨号模式，address格式 host:port (客户端)
func NewTCPConn(address string, event TCPConnEvent) (*TCPConn, error) {
	// 检查下地址格式合法性
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}
	tc := &TCPConn{
		dialMode:   true,
		removeAddr: *tcpAddr,
		event:      event,
		state:      TCPStateInvalid,
		quit:       make(chan struct{}),
		closed:     make(chan struct{}),
		reconn:     make(chan struct{}, 1), // 开一个缓存即可
		mq:         make(chan []byte, 10000),
	}
	// 开启循环
	go tc.loop()
	return tc, nil
}

// NewTCPConned 创建TCP网络被动连接对象(监听端)
func NewTCPConned(conn net.Conn, event TCPConnEvent) (*TCPConn, error) {
	if conn == nil {
		return nil, errors.New("conn is nil")
	}

	switch conn.(type) {
	case *net.TCPConn:
	case *tls.Conn:
	default:
		return nil, fmt.Errorf("conn type not support %T", conn)
	}
	t := &TCPConn{
		dialMode:   false,
		removeAddr: *conn.RemoteAddr().(*net.TCPAddr),
		localAddr:  *conn.LocalAddr().(*net.TCPAddr),
		event:      event,
		state:      TCPStateConnected, // 默认连接成功
		conn:       conn,
		quit:       make(chan struct{}),
		closed:     make(chan struct{}),
		mq:         make(chan []byte, 10000),
	}
	// 开启循环
	go t.loop()
	return t, nil
}

func (tc *TCPConn) RemoteAddr() *net.TCPAddr {
	return &tc.removeAddr
}

func (tc *TCPConn) LocalAddr() *net.TCPAddr {
	return &tc.localAddr
}

func (tc *TCPConn) Connected() bool {
	return atomic.LoadInt32(&tc.state) == TCPStateConnected
}

func (tc *TCPConn) Send(buf []byte) error {
	if len(buf) == 0 {
		return errors.New("send buf is empty")
	}
	if atomic.LoadInt32(&tc.state) != TCPStateConnected {
		return errors.New("net not connect")
	}
	// 做一个写超时
	timer := time.NewTimer(4 * time.Second)
	select {
	case tc.mq <- buf:
	case <-timer.C:
		return errors.New("send timeout")
	}
	if !timer.Stop() {
		select {
		case <-timer.C: // try to drain the channel
		default:
		}
	}
	return nil
}

// 还未发送的消息的长度
func (tc *TCPConn) MQLen() int {
	return len(tc.mq)
}

// 清空未发送的消息
func (tc *TCPConn) MQClear() {
	for {
		select {
		case <-tc.mq:
		default:
			return
		}
	}
}

// Close 关闭连接
// waitClose 是否等待关闭完成，true：等待 false：不等待，允许在event的回调函数中调用
func (tc *TCPConn) Close(waitClose bool) {
	quitState := 1
	if !waitClose {
		quitState = 2
	}
	// 判断外部是否能调用关闭
	if atomic.CompareAndSwapInt32(&tc.quitState, 0, int32(quitState)) {
		close(tc.quit)
		if waitClose {
			<-tc.closed
		}
	}
}

// Recon 重新连接，只有主动连接的才支持
func (tc *TCPConn) Reconn() {
	if !tc.dialMode {
		return
	}
	select {
	case tc.reconn <- struct{}{}:
	default:
	}
}

func (tc *TCPConn) loop() {
	for {
		// 拨号模式 先拨号
		if tc.dialMode {
			atomic.StoreInt32(&tc.state, TCPStateConnecting)

			// 不需要等待loopDial退出，内部已经处理了合理的退出，否则会导致死锁(在OnDialSuccess的回调中调用了Close(true))
			lexit := make(chan error, 1)
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				tc.loopDial(ctx, lexit)
			}()

			select {
			case <-tc.quit:
				atomic.StoreInt32(&tc.state, TCPStateStop)
				cancel() // 取消连接
			case exitErr := <-lexit:
				if exitErr == nil {
					// loopDial填充nil 表示连接成功
					atomic.StoreInt32(&tc.state, TCPStateConnected)
				} else {
					atomic.CompareAndSwapInt32(&tc.quitState, 0, -1)
					atomic.StoreInt32(&tc.state, TCPStateStop)
				}
			}

			// 要求退出了 直接退出
			if atomic.LoadInt32(&tc.quitState) != 0 {
				break
			}
		}

		// 开启器读写
		// 不需要等待loopRead 和 loopWrite退出，内部已经处理了合理的退出，否则会导致死锁(在OnRecv的回调中调用了Close(true))
		rexit := make(chan error, 1) // 读内部退出
		wexit := make(chan error, 1) // 写内部退出
		conn := tc.conn              // 保存连接对象 防止在读写循环还没开始，下面的就给关闭了
		go func() {
			tc.loopRead(conn, rexit)
		}()
		go func() {
			tc.loopWrite(conn, wexit)
		}()

		// 监听外部退出和读写退出
		var exitErr error
		reconn := false
		select {
		case <-tc.quit:
			atomic.StoreInt32(&tc.state, TCPStateStop)
		case exitErr = <-rexit:
			atomic.StoreInt32(&tc.state, TCPStateRWExit)
		case exitErr = <-wexit:
			atomic.StoreInt32(&tc.state, TCPStateRWExit)
		case <-tc.reconn:
			reconn = true
			exitErr = errors.New("reconn")
			atomic.StoreInt32(&tc.state, TCPStateInvalid)
		}

		// 关闭socket
		tc.conn.Close()
		tc.conn = nil

		if tc.event != nil {
			var err error
			func() {
				defer utils.HandlePanic()
				err = tc.event.OnDisConnect(exitErr, tc)
			}()
			if err != nil {
				atomic.CompareAndSwapInt32(&tc.quitState, 0, -1)
				break
			}
		}
		// 清空待发送的消息
		tc.MQClear()

		// 如果不是拨号模式 直接退出
		if !tc.dialMode {
			atomic.CompareAndSwapInt32(&tc.quitState, 0, -1)
		}

		// 要求退出了 直接退出
		if atomic.LoadInt32(&tc.quitState) != 0 {
			break
		}

		// 如果是要求重连，等待1秒后继续重连
		if reconn {
			timer := time.NewTimer(time.Second)
			select {
			case <-tc.quit:
			case <-timer.C:
				continue
			}
			if !timer.Stop() {
				select {
				case <-timer.C: // try to drain the channel
				default:
				}
			}

			// 再次检查是否退出了
			if atomic.LoadInt32(&tc.quitState) != 0 {
				break
			}
		}
	}

	close(tc.closed)
}

// 内部会一直尝试连接 直到外部要求退出或者连接成功
// 成功是 lexit 发送 nil, 外部要求退出是 lexit 发送错误
func (tc *TCPConn) loopDial(ctx context.Context, exit chan error) {
	// 开始连接
	var conn net.Conn
	for {
		var err error
		// 连接
		d := net.Dialer{Timeout: time.Second * 30}
		conn, err = d.DialContext(ctx, "tcp", tc.removeAddr.String())

		// 先检查下连接状态
		if atomic.LoadInt32(&tc.state) != TCPStateConnecting {
			exit <- fmt.Errorf("Dial state is not Connecting")
			return
		}

		// 连接成功
		if err == nil && conn != nil {
			break
		}

		if tc.event != nil {
			func() {
				defer utils.HandlePanic()
				err = tc.event.OnDialFail(err, tc)
			}()

			if err != nil {
				exit <- err // 外部要求不在连接了
				return
			}
		}
		// 连接失败 1秒后继续连
		timer := time.NewTimer(time.Second)
		select {
		case <-tc.quit:
		case <-timer.C:
			continue
		}
		if !timer.Stop() {
			select {
			case <-timer.C: // try to drain the channel
			default:
			}
		}

		// 再次检查下连接状态
		if atomic.LoadInt32(&tc.state) != TCPStateConnecting {
			exit <- fmt.Errorf("Dial state is not Connecting")
			return
		}
	}
	// 连接成功
	tc.conn = conn
	tc.localAddr = *conn.LocalAddr().(*net.TCPAddr) // 写下本地地址
	if tc.event != nil {
		func() {
			defer utils.HandlePanic()
			tc.event.OnDialSuccess(tc)
		}()
	}
	exit <- nil
}

func (tc *TCPConn) loopRead(conn net.Conn, exit chan error) {
	readbuf := new(bytes.Buffer)
	buf := make([]byte, 1024*16)
	for {
		// 先检查下连接状态
		if atomic.LoadInt32(&tc.state) != TCPStateConnected {
			exit <- fmt.Errorf("Read state is not Connected")
			return
		}
		conn.SetReadDeadline(time.Now().Add(time.Second))
		n, err := conn.Read(buf)
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				continue
			}
			exit <- fmt.Errorf("Read %s", err.Error())
			return
		}
		if n == 0 {
			continue
		}
		if tc.event != nil {
			readbuf.Write(buf[0:n]) // 需要有event对象才保存数据，否则没有保存的意义
			for {
				len, err := tc.event.OnRecv(readbuf.Bytes(), tc)
				if err != nil {
					exit <- fmt.Errorf("OnRecv %s", err.Error())
					return
				}
				if len < 0 || len > readbuf.Len() {
					exit <- fmt.Errorf("OnRecv len is %d, readbuf len is %d", len, readbuf.Len())
					return
				}
				if len > 0 {
					readbuf.Next(len)
				}
				if len == 0 || readbuf.Len() == 0 {
					break // 继续读取
				}
			}
		}
	}
}

func (tc *TCPConn) loopWrite(conn net.Conn, exit chan error) {
	for {
		// 先检查下连接状态
		if atomic.LoadInt32(&tc.state) != TCPStateConnected {
			exit <- fmt.Errorf("Write state is not Connected")
			return
		}
		exitFlag := false
		timer := time.NewTimer(time.Second)
		select {
		case <-tc.quit:
			exitFlag = true
		case <-timer.C:
			continue
		case buf := <-tc.mq:
			if tc.event != nil {
				var err error
				func() {
					defer utils.HandlePanic()
					buf, err = tc.event.OnSend(buf, tc)
				}()
				if err != nil {
					exit <- fmt.Errorf("OnSend %s", err.Error())
					exitFlag = true
					break
				}
			}
			_, err := conn.Write(buf)
			if err != nil {
				exit <- fmt.Errorf("Write %s", err.Error())
				exitFlag = true
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C: // try to drain the channel
			default:
			}
		}
		if exitFlag {
			break
		}
	}
}
