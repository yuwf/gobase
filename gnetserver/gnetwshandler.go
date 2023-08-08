package gnetserver

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"io"
	"net/http"
	"runtime"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

// gnet支持websocket类

// 握手时给客户端回复的头，外部可修改
var WSHeader = http.Header{
	"GOVersion": []string{runtime.Version()},
	"GOOS":      []string{runtime.GOOS},
	"GOARCH":    []string{runtime.GOARCH},
}

type gnetWSHandler[ClientInfo any] struct {
	gc      *GNetClient[ClientInfo] // gnet连接对象
	upgrade bool                    // 是否已经升级为websocket
	ugrader ws.Upgrader             // 协议升级处理类
	Header  http.Header             // 请求头

	buf     []byte // 当前要读取的buf
	readlen int    // 读取的长度
}

func newGNetWSHandler[ClientInfo any](gc *GNetClient[ClientInfo]) *gnetWSHandler[ClientInfo] {
	wsh := &gnetWSHandler[ClientInfo]{
		gc:      gc,
		upgrade: false,
		Header:  make(http.Header),
	}

	wsh.ugrader = ws.Upgrader{
		OnHost: func(host []byte) error {
			return nil
		},
		OnHeader: func(key, value []byte) error {
			wsh.Header.Set(string(key), string(value))
			return nil
		},
		OnBeforeUpgrade: func() (ws.HandshakeHeader, error) {
			head := WSHeader
			for k, vs := range ParamConf.Get().WSHeader {
				for _, v := range vs {
					head.Add(k, v)
				}
			}
			return ws.HandshakeHeaderHTTP(head), nil
		},
		OnRequest: func(uri []byte) error {
			return nil
		},
	}
	return wsh
}

func (wsh *gnetWSHandler[ClientInfo]) Read(b []byte) (n int, err error) {
	targetLength := len(b)
	if targetLength < 1 {
		return 0, nil
	}

	if wsh.readlen >= len(wsh.buf) {
		return 0, io.EOF
	}
	if len(wsh.buf)-wsh.readlen >= targetLength {
		copy(b, wsh.buf[wsh.readlen:]) //数据拷贝
		n = targetLength
		wsh.readlen += targetLength
	} else {
		//buffer 中数据不够 数据已全部读出来
		copy(b, wsh.buf[wsh.readlen:]) //数据拷贝
		n = len(wsh.buf) - wsh.readlen
		wsh.readlen += n
	}
	return n, nil
}

func (wsh *gnetWSHandler[ClientInfo]) Write(b []byte) (int, error) {
	err := wsh.gc.conn.AsyncWrite(b)
	return len(b), err
}

func (wsh *gnetWSHandler[ClientInfo]) recv(buf []byte) (int, bool, error) {
	wsh.buf = buf
	wsh.readlen = 0
	validlen := 0 //读取后有效，能正确解析出来的后长度

	if !wsh.upgrade {
		_, err := wsh.ugrader.Upgrade(wsh)
		if err != nil {
			return 0, false, err // 返回err后会关闭连接，并调用OnDisConnect
		}
		wsh.upgrade = true
		validlen = wsh.readlen

		return validlen, true, nil
	}

	for {
		messages, err := wsutil.ReadClientMessage(wsh, nil)
		if err == io.EOF {
			break
		}
		if err != nil {
			return validlen, false, err // 返回err后会关闭连接，并调用OnDisConnect
		}
		validlen = wsh.readlen
		message := messages[0]
		switch message.OpCode {
		case ws.OpText:
			ctx := context.WithValue(wsh.gc.ctx, CtxKey_Text, 1)
			_, err := wsh.gc.recv(ctx, message.Payload)
			if err != nil {
				return 0, false, err // 返回err后会关闭连接，并调用OnDisConnect
			}
		case ws.OpBinary:
			_, err := wsh.gc.recv(wsh.gc.ctx, message.Payload)
			if err != nil {
				return 0, false, err // 返回err后会关闭连接，并调用OnDisConnect
			}
		case ws.OpClose: // 客户端要求断开
			return validlen, false, errors.New("ws.OpClose")
		case ws.OpPing:
			wsutil.WriteServerMessage(wsh, ws.OpPong, nil)
		case ws.OpPong:
			// 不需要操作
		default:
		}
	}
	return validlen, false, nil
}
