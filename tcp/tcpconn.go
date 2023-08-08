package tcp

// https://github.com/yuwf

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TCPStateInvalid    = 0 // 无效
	TCPStateConnecting = 1 // 连接中
	TCPStateConnected  = 2 // 连接成功
	TCPStateStop       = 3 // 外部要求退出了
	TCPStateRWExit     = 4 // 读写异常退出
)

// TCPConn 事件回调接口
// 在OnDialFail OnDialSuccess OnDisConnect回调函数中 不可调用TCPConn.Close函数
type TCPConnEvent interface {
	// OnDialFail 连接失败，等待下次连接 拨号模式调用, 返回nil会再次自动重连，否则不重连
	OnDialFail(err error, tc *TCPConn) error
	// OnDialSuccess 连接成功 拨号模式调用
	OnDialSuccess(tc *TCPConn)

	// OnDisConnect 失去连接，外部Close不会调用，拨号模式返回nil会自动重连，否则不自动重连，调用后会清空还未发送的消息队列
	OnDisConnect(err error, tc *TCPConn) error
	// OnClose 关闭了本Conn，外部Close才会调用
	OnClose(tc *TCPConn)
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
func (*TCPConnEvenHandle) OnClose(tc *TCPConn) {
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
	state int32    // 连接状态 原子操作
	conn  net.Conn // 连接对象

	mq chan []byte // 写消息队列

	// 外部要求退出
	quit     chan int // 退出chan 外部写 内部读
	quitFlag int32    // 标记是否退出，原子操作, <0内部退出 >0外部Close 0：未退出 1：表示等待退出 2：表示不等待退出
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
		quit:       make(chan int),
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
		quit:       make(chan int),
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

// Close 关闭连接 在event的回调函数中不可调用等待关闭
func (tc *TCPConn) Close(waitClose bool) {
	quitFlag := 1
	if !waitClose {
		quitFlag = 2
	}
	// 判断外部是否能调用关闭
	if atomic.CompareAndSwapInt32(&tc.quitFlag, 0, int32(quitFlag)) {
		if waitClose {
			tc.quit <- int(atomic.LoadInt32(&tc.quitFlag))
			if quitFlag == 1 {
				<-tc.quit
			}
		} else {
			go func() {
				tc.quit <- int(atomic.LoadInt32(&tc.quitFlag))
			}()
		}
	}
}

func (tc *TCPConn) loop() {
	for {
		// 拨号模式 先拨号
		if tc.dialMode {
			if !tc.loopDial() {
				atomic.CompareAndSwapInt32(&tc.quitFlag, 0, -1)
				break
			}
		}

		// 开启器读写
		wg := &sync.WaitGroup{} // 用来等待读写退出
		wg.Add(2)
		rexit := make(chan error, 1) // 读内部退出
		wexit := make(chan error, 1) // 写内部退出
		go func() {
			tc.loopRead(rexit)
			wg.Done()
		}()
		go func() {
			tc.loopWrite(wexit)
			wg.Done()
		}()

		// 监听外部退出和读写退出
		var exitErr error
		select {
		case <-tc.quit:
			atomic.StoreInt32(&tc.state, TCPStateStop)
		case exitErr = <-rexit:
			atomic.StoreInt32(&tc.state, TCPStateRWExit)
		case exitErr = <-wexit:
			atomic.StoreInt32(&tc.state, TCPStateRWExit)
		}

		// 等待读写退出
		wg.Wait()
		close(rexit)
		close(wexit)

		tc.conn.Close()
		tc.conn = nil

		// 如果是外部要求退出 直接退出
		if atomic.LoadInt32(&tc.state) == TCPStateStop {
			break
		}
		if tc.event != nil {
			if tc.event.OnDisConnect(exitErr, tc) != nil {
				atomic.CompareAndSwapInt32(&tc.quitFlag, 0, -1)
				break
			}
		}
		// 清空待发送的消息
		tc.MQClear()
		// 如果不是拨号模式 直接退出
		if !tc.dialMode {
			atomic.CompareAndSwapInt32(&tc.quitFlag, 0, -1)
			break
		}
	}

	quitFlag := atomic.LoadInt32(&tc.quitFlag)
	if quitFlag > 0 && tc.event != nil {
		tc.event.OnClose(tc)
	}
	if quitFlag == 1 {
		// 清空下quit，防止内部close途中，外部调用Close
		select {
		case <-tc.quit:
		default:
		}
		tc.quit <- int(atomic.LoadInt32(&tc.quitFlag)) // 反写让Close退出
	}
}

func (tc *TCPConn) loopDial() bool {
	// 开始连接
	atomic.StoreInt32(&tc.state, TCPStateConnecting)
	for {
		conn, err := net.DialTimeout("tcp", tc.removeAddr.String(), time.Second*30)
		if err != nil || conn == nil {
			if tc.event != nil {
				if tc.event.OnDialFail(err, tc) != nil {
					atomic.StoreInt32(&tc.state, TCPStateStop)
					return false
				}
			}
			// 连接失败 1秒后继续连
			timer := time.NewTimer(time.Second)
			select {
			case <-tc.quit:
				atomic.StoreInt32(&tc.state, TCPStateStop)
			case <-timer.C:
				continue
			}
			if !timer.Stop() {
				select {
				case <-timer.C: // try to drain the channel
				default:
				}
			}
			// 如果是外部要求退出 直接退出
			if atomic.LoadInt32(&tc.state) == TCPStateStop {
				return false
			}
			continue
		}
		tc.conn = conn
		break
	}
	// 连接成功
	atomic.StoreInt32(&tc.state, TCPStateConnected)
	tc.localAddr = *tc.conn.LocalAddr().(*net.TCPAddr) // 写下本地地址
	if tc.event != nil {
		tc.event.OnDialSuccess(tc)
	}
	return true
}

func (tc *TCPConn) loopRead(exit chan error) {
	readbuf := new(bytes.Buffer)
	buf := make([]byte, 1024*16)
	for {
		// 先检查下连接状态
		if atomic.LoadInt32(&tc.state) != TCPStateConnected {
			return
		}
		tc.conn.SetReadDeadline(time.Now().Add(time.Second))
		n, err := tc.conn.Read(buf)
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

func (tc *TCPConn) loopWrite(exit chan error) {
	for {
		// 先检查下连接状态
		if atomic.LoadInt32(&tc.state) != TCPStateConnected {
			break
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-timer.C:
			continue
		case buf := <-tc.mq:
			if tc.event != nil {
				var err error
				buf, err = tc.event.OnSend(buf, tc)
				if err != nil {
					exit <- fmt.Errorf("OnSend %s", err.Error())
					return
				}
			}
			_, err := tc.conn.Write(buf)
			if err != nil {
				exit <- fmt.Errorf("Write %s", err.Error())
				return
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C: // try to drain the channel
			default:
			}
		}
	}
}
