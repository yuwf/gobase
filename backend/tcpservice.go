package backend

// https://github.com/yuwf

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/yuwf/gobase/tcp"
	"github.com/yuwf/gobase/utils"
)

// 后端连接对象 协程安全对象
// T是和业务相关的客户端信息结构
type TcpService[T any] struct {
	// 不可修改
	g           *TcpGroup[T]    // 上层对象
	conf        *ServiceConfig  // 服务器发现的配置
	confDestroy int32           // 表示将配置是否销毁了 原子操作
	address     string          // 地址
	seq         *utils.Sequence // 消息顺序处理工具 协程安全
	info        *T              // 客户端信息，内容修改需要外层加锁控制
	conn        *tcp.TCPConn    // 连接对象，协程安全

	ctx context.Context // 本连接的上下文

	//RPC消息使用 [rpcid:chan respmsg]
	rpc *sync.Map

	// 外部要求退出
	quit     chan int // 退出chan 外部写 内部读
	quitFlag int32    // 标记是否退出，原子操作

	// 标记服务器发现逻辑配置已经不存在了

}

func NewTcpService[T any](conf *ServiceConfig, g *TcpGroup[T]) (*TcpService[T], error) {
	ts := &TcpService[T]{
		g:           g,
		conf:        conf,
		confDestroy: 0,
		address:     fmt.Sprintf("%s:%d", conf.ServiceAddr, conf.ServicePort),
		info:        new(T),
		ctx:         context.WithValue(context.TODO(), "scheme", "tcp"),
		rpc:         new(sync.Map),
		quit:        make(chan int),
		quitFlag:    0,
	}
	if TcpParamConf.Get().MsgSeq {
		ts.seq = &utils.Sequence{}
	}
	conn, err := tcp.NewTCPConn(ts.address, ts)
	if err != nil {
		log.Error().Str("ServiceName", conf.ServiceName).Str("ServiceId", conf.ServiceId).Err(err).Str("addr", ts.address).Msg("NewTcpService")
		return nil, err
	}
	ts.conn = conn
	// 开启tick协程
	go ts.loopTick()
	return ts, nil
}

// 重连或者关闭连接时调用
func (tc *TcpService[T]) clear() {
	// 清空下rpc
	tc.rpc.Range(func(key, value interface{}) bool {
		ch := value.(chan interface{})
		ch <- nil // 写一个空的
		return true
	})
}

// 获取配置 获取后外层要求只读
func (ts *TcpService[T]) Conf() *ServiceConfig {
	return ts.conf
}

func (ts *TcpService[T]) ServiceName() string {
	return ts.conf.ServiceName
}

func (ts *TcpService[T]) ServiceId() string {
	return ts.conf.ServiceId
}

func (ts *TcpService[T]) Info() *T {
	return ts.info
}

// 获取连接对象
func (ts *TcpService[T]) Conn() *tcp.TCPConn {
	return ts.conn
}

func (ts *TcpService[T]) ConnName() string {
	return ts.conf.ServiceName + ":" + ts.conf.ServiceId
}

func (ts *TcpService[T]) Send(data []byte) error {
	if ts.g.tb.event != nil {
		msgLog := zerolog.Dict()
		buf, err := ts.g.tb.event.Encode(data, ts, msgLog)
		if err != nil {
			log.Error().Str("Name", ts.ConnName()).Err(err).Dict("Msg", msgLog).Msg("Send error")
			return err
		}
		err = ts.conn.Send(buf)
		if err != nil {
			log.Error().Str("Name", ts.ConnName()).Err(err).Dict("Msg", msgLog).Msg("Send error")
			return err
		}
		logVo := reflect.ValueOf(msgLog).Elem()
		logBuf := logVo.FieldByName("buf")
		if len(logBuf.Bytes()) > 1 {
			log.Debug().Str("Name", ts.ConnName()).Dict("Msg", msgLog).Msg("Send")
		}
		return nil
	}
	err := errors.New("encode is empty")
	log.Error().Str("Name", ts.ConnName()).Err(err).Int("Size", len(data)).Msg("Send error")
	return err
}

func (ts *TcpService[T]) SendMsg(msg interface{}) error {
	if ts.g.tb.event != nil {
		msgLog := zerolog.Dict()
		buf, err := ts.g.tb.event.EncodeMsg(msg, ts, msgLog)
		if err != nil {
			log.Error().Str("Name", ts.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendMsg error")
			return err
		}
		err = ts.conn.Send(buf)
		if err != nil {
			log.Error().Str("Name", ts.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendMsg error")
			return err
		}
		logVo := reflect.ValueOf(msgLog).Elem()
		logBuf := logVo.FieldByName("buf")
		if len(logBuf.Bytes()) > 1 {
			log.Debug().Str("Name", ts.ConnName()).Dict("Msg", msgLog).Msg("SendMsg")
		}
		return nil
	}
	err := errors.New("encode is empty")
	log.Error().Str("Name", ts.ConnName()).Err(err).Interface("Msg", msg).Msg("SendMsg error")
	return err
}

func (ts *TcpService[T]) SendRPCMsg(rpcId interface{}, msg interface{}, timeout time.Duration) (interface{}, error) {
	if ts.g.tb.event != nil {
		_, ok := ts.rpc.Load(rpcId)
		if ok {
			err := errors.New("rpcId exist")
			log.Error().Str("Name", ts.ConnName()).Err(err).Interface("rpcID", rpcId).Interface("Msg", msg).Msg("SendRPCMsg error")
			return nil, err
		}
		msgLog := zerolog.Dict()
		buf, err := ts.g.tb.event.EncodeMsg(msg, ts, msgLog)
		if err != nil {
			log.Error().Str("Name", ts.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendRPCMsg error")
			return nil, err
		}
		err = ts.conn.Send(buf)
		if err != nil {
			log.Error().Str("Name", ts.ConnName()).Err(err).Dict("Msg", msgLog).Msg("SendRPCMsg error")
			return nil, err
		}
		logVo := reflect.ValueOf(msgLog).Elem()
		logBuf := logVo.FieldByName("buf")
		if len(logBuf.Bytes()) > 1 {
			log.Debug().Str("Name", ts.ConnName()).Dict("Msg", msgLog).Msg("SendRPCMsg")
		}
		ch := make(chan interface{})
		ts.rpc.Store(rpcId, ch)
		// 协程让出，等待消息回复或超时
		runtime.Gosched()
		timer := time.NewTimer(timeout)
		var resp interface{}
		select {
		case resp = <-ch:
			if !timer.Stop() {
				select {
				case <-timer.C: // try to drain the channel
				default:
				}
			}
		case <-timer.C:
			err = errors.New("timeout")
		}
		ts.rpc.Delete(rpcId)
		close(ch)
		if resp == nil && err == nil { // clear函数的调用会触发此情况
			err = errors.New("close")
		}
		if err != nil {
			log.Error().Str("Name", ts.ConnName()).Err(err).Interface("Msg", msg).Msg("SendRPCMsg error")
		}
		return resp, err
	}
	err := errors.New("encode is empty")
	log.Error().Str("Name", ts.ConnName()).Err(err).Interface("Msg", msg).Msg("SendRPCMsg error")
	return nil, err
}

func (ts *TcpService[T]) loopTick() {
	for {
		// 每秒tick下
		timer := time.NewTimer(time.Second)
		select {
		case <-ts.quit:
			if !timer.Stop() {
				select {
				case <-timer.C: // try to drain the channel
				default:
				}
			}
		case <-timer.C:
		}
		if atomic.LoadInt32(&ts.quitFlag) != 0 {
			break
		}
		if ts.g.tb.event != nil {
			if ts.seq != nil {
				ts.seq.Submit(func() {
					ts.g.tb.event.OnTick(ts.ctx, ts)
				})
			} else {
				utils.Submit(func() {
					ts.g.tb.event.OnTick(ts.ctx, ts)
				})
			}
		}
	}
	ts.quit <- 1 // 反写让Close退出
}

func (ts *TcpService[T]) close() {
	// 先从哈希环中移除
	ts.g.hashring.Remove(ts.conf.ServiceId)
	// 关闭网络
	ts.conn.Close(true)
	// 退出循环
	if atomic.CompareAndSwapInt32(&ts.quitFlag, 0, 1) {
		ts.quit <- 1
		<-ts.quit
	}
}

func (ts *TcpService[T]) OnDialFail(err error, t *tcp.TCPConn) error {
	log.Error().Str("Name", ts.ConnName()).Err(err).Str("DialAddr", ts.address).Int32("ConfDestroy", atomic.LoadInt32(&ts.confDestroy)).Msg("Connect fail")
	if atomic.LoadInt32(&ts.confDestroy) == 1 {
		// 服务器发现配置已经不存在了，停止loop，直接从group中删除
		ts.g.removeSevice(ts.conf.ServiceId) // 先删
		if atomic.CompareAndSwapInt32(&ts.quitFlag, 0, 1) {
			ts.quit <- 1
			<-ts.quit
		}
		return errors.New("config destroy")
	}
	return nil
}

func (ts *TcpService[T]) OnDialSuccess(t *tcp.TCPConn) {
	log.Info().Str("Name", ts.ConnName()).Str("RemoteAddr", t.RemoteAddr().String()).Str("LocalAddr", t.LocalAddr().String()).Msg("Connect success")

	// 添加到哈希环中
	ts.g.hashring.Add(ts.conf.ServiceId)

	if ts.g.tb.event != nil {
		ctx := context.TODO()
		ts.g.tb.event.OnConnected(ctx, ts)
	}

	// 回调
	for _, h := range ts.g.tb.hook {
		h.OnConnected(ts)
	}
}

func (ts *TcpService[T]) OnDisConnect(err error, t *tcp.TCPConn) error {
	log.Error().Str("Name", ts.ConnName()).Err(err).Str("Addr", ts.address).Int32("ConfDestroy", atomic.LoadInt32(&ts.confDestroy)).Msg("Disconnect")
	// 从哈希环中移除
	ts.g.hashring.Remove(ts.conf.ServiceId)

	ts.clear()

	if atomic.LoadInt32(&ts.confDestroy) == 1 {
		// 服务器发现配置已经不存在了，停止loop，直接从group中删除
		ts.g.removeSevice(ts.conf.ServiceId) // 先删
		if atomic.CompareAndSwapInt32(&ts.quitFlag, 0, 1) {
			ts.quit <- 1
			<-ts.quit
		}
		return errors.New("config destroy")
	}
	if ts.g.tb.event != nil {
		ts.g.tb.event.OnDisConnect(ts.ctx, ts)
	}

	// 回调
	for _, h := range ts.g.tb.hook {
		h.OnDisConnect(ts)
	}
	return nil
}

func (ts *TcpService[T]) OnClose(tc *tcp.TCPConn) {
	// 调用close主动关闭的
	log.Info().Str("Name", ts.ConnName()).Str("addr", ts.address).Msg("Onlose")

	ts.clear()

	// 回调
	for _, h := range ts.g.tb.hook {
		h.OnDisConnect(ts)
	}
}

func (ts *TcpService[T]) OnRecv(data []byte, tc *tcp.TCPConn) (int, error) {
	// 回调
	for _, h := range ts.g.tb.hook {
		h.OnRecv(ts, len(data))
	}
	len, err := ts.recv(data)
	//if err != nil {
	//	tc.Close(err) // 不需要关，tcpconn会根据err关闭掉，并调用OnDisConnect
	//}
	return len, err
}

func (ts *TcpService[T]) OnSend(data []byte, tc *tcp.TCPConn) ([]byte, error) {
	// 回调
	for _, h := range ts.g.tb.hook {
		h.OnSend(ts, len(data))
	}
	return data, nil
}

func (ts *TcpService[T]) recv(data []byte) (int, error) {
	if ts.g.tb.event == nil {
		return len(data), nil
	}
	decodeLen := 0
	for {
		msg, l, err := ts.g.tb.event.DecodeMsg(ts.ctx, data[decodeLen:], ts)
		if err != nil {
			return 0, err
		}
		if l < 0 || l > len(data)-decodeLen {
			err = fmt.Errorf("decode return len is %d, readbuf len is %d", l, len(data)-decodeLen)
			return 0, err
		}
		decodeLen += l
		if l == 0 || msg == nil {
			break
		}
		// rpc消息检查
		rpcId := ts.g.tb.event.CheckRPCResp(msg)
		if rpcId != nil {
			// rpc
			rpc, ok := ts.rpc.Load(rpcId)
			if ok {
				ch := rpc.(chan interface{})
				ch <- msg
			} else {
				// 没找到可能是超时了也可能是CheckRPCResp出错了 也交给OnMsg执行
				if ts.seq != nil {
					ts.seq.Submit(func() {
						ts.g.tb.event.OnMsg(ts.ctx, msg, ts)
					})
				} else {
					utils.Submit(func() {
						ts.g.tb.event.OnMsg(ts.ctx, msg, ts)
					})
				}
			}
		} else {
			// 消息放入协程池中
			if ts.seq != nil {
				ts.seq.Submit(func() {
					ts.g.tb.event.OnMsg(ts.ctx, msg, ts)
				})
			} else {
				utils.Submit(func() {
					ts.g.tb.event.OnMsg(ts.ctx, msg, ts)
				})
			}
		}
		if decodeLen >= len(data) {
			break // 不需要继续读取了
		}
	}
	return decodeLen, nil
}
