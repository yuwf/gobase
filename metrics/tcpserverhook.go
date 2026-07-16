package metrics

// https://github.com/yuwf/gobase

import (
	"sync"
	"time"

	"github.com/yuwf/gobase/msger"
	"github.com/yuwf/gobase/tcpserver"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// TCPServer
	tcpServerOnce              sync.Once
	tcpServerConningCount      *prometheus.GaugeVec
	tcpServerClientingCount    *prometheus.GaugeVec
	tcpServerHandShakeingCount *prometheus.GaugeVec
	tcpServerConnCloseReason   *prometheus.CounterVec

	tcpServerConnCount      *prometheus.CounterVec
	tcpServerHandShakeCount *prometheus.CounterVec
	tcpServerDisConnCount   *prometheus.CounterVec

	// 每个连接的信息，有开关控制，指标名包含remote地址，不可用对象的ConnName，他不稳定
	tcpServerConnSendDataSize *prometheus.CounterVec
	tcpServerConnRecvDataSize *prometheus.CounterVec
	tcpServerConnSendMsgCount *prometheus.CounterVec // 所有的发送
	tcpServerConnRecvMsgCount *prometheus.CounterVec
	tcpServerConnRecvSeqCount *prometheus.GaugeVec

	tcpServerSendDataSize *prometheus.CounterVec
	tcpServerRecvDataSize *prometheus.CounterVec
	tcpServerRecvSeqCount *prometheus.GaugeVec

	tcpServerSendCount *prometheus.CounterVec
	tcpServerSendSize  *prometheus.CounterVec

	tcpServerSendMsgCount *prometheus.CounterVec
	tcpServerSendMsgSize  *prometheus.CounterVec

	tcpServerSendTextCount *prometheus.CounterVec
	tcpServerSendTextSize  *prometheus.CounterVec

	tcpServerSendRPCMsgCount *prometheus.CounterVec
	tcpServerSendRPCMsgSize  *prometheus.CounterVec
	tcpServerSendRPCMsgTime  *prometheus.CounterVec

	tcpServerRecvMsgCount *prometheus.CounterVec
	tcpServerRecvMsgSize  *prometheus.CounterVec
)

type tcpServerHook[ClientInfo any] struct {
	addr   string
	server msger.ServerTermianl
}

func (h *tcpServerHook[ClientInfo]) init() {
	tcpServerOnce.Do(func() {
		tcpServerConningCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "tcpserver_conning_count"}, []string{"addr"})
		tcpServerClientingCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "tcpserver_clienting_count"}, []string{"addr"})
		tcpServerHandShakeingCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "tcpserver_handshakeing_count"}, []string{"addr"})
		tcpServerConnCloseReason = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_conn_close_reason"}, []string{"addr", "err"})

		tcpServerConnCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_conn_count"}, []string{"addr"})
		tcpServerHandShakeCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_handshake_count"}, []string{"addr"})
		tcpServerDisConnCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_disconn_count"}, []string{"addr"})

		//
		if TCPServerConn {
			tcpServerConnSendDataSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_conn_senddata_size"}, []string{"addr", "remote"})
			tcpServerConnRecvDataSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_conn_recvdata_size"}, []string{"addr", "remote"})
			tcpServerConnSendMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_conn_sendmsg_count"}, []string{"addr", "remote"})
			tcpServerConnRecvMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_conn_recvmsg_count"}, []string{"addr", "remote"})
			tcpServerConnRecvSeqCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "tcpserver_conn_recvseqmsg_count"}, []string{"addr", "remote"})
		}

		tcpServerSendDataSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_senddata_size"}, []string{"addr"})
		tcpServerRecvDataSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_recvdata_size"}, []string{"addr"})
		tcpServerRecvSeqCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "tcpserver_recvseqmsg_count"}, []string{"addr"})

		tcpServerSendCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_send_count"}, []string{"addr"})
		tcpServerSendSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_send_size"}, []string{"addr"})

		tcpServerSendMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_sendmsg_count"}, []string{"addr", "name"})
		tcpServerSendMsgSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_sendmsg_size"}, []string{"addr", "name"})

		tcpServerSendTextCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_sendtext_count"}, []string{"addr"})
		tcpServerSendTextSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_sendtext_size"}, []string{"addr"})

		tcpServerSendRPCMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_sendrpcmsg_count"}, []string{"addr", "name"})
		tcpServerSendRPCMsgSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_sendrpcmsg_size"}, []string{"addr", "name"})
		tcpServerSendRPCMsgTime = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_sendrpcmsg_time"}, []string{"addr", "name"})

		tcpServerRecvMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_recvmsg_count"}, []string{"addr", "name"})
		tcpServerRecvMsgSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_recvmsg_size"}, []string{"addr", "name"})
	})
}

func (h *tcpServerHook[ClientInfo]) OnConnected(tc *tcpserver.TCPClient[ClientInfo]) {
	h.init()
	count, _ := h.server.ConnCount()
	tcpServerConningCount.WithLabelValues(h.addr).Set(float64(count))
	tcpServerConnCount.WithLabelValues(h.addr).Add(1)
}

func (h *tcpServerHook[ClientInfo]) OnWSHandShake(tc *tcpserver.TCPClient[ClientInfo]) {
	h.init()
	_, handshakecount := h.server.ConnCount()
	tcpServerHandShakeCount.WithLabelValues(h.addr).Add(1)
	tcpServerHandShakeingCount.WithLabelValues(h.addr).Set(float64(handshakecount))
}

func (h *tcpServerHook[ClientInfo]) OnDisConnect(tc *tcpserver.TCPClient[ClientInfo], removeClient bool, closeReason error) {
	h.init()
	count, handshakecount := h.server.ConnCount()
	tcpServerConningCount.WithLabelValues(h.addr).Set(float64(count))
	tcpServerHandShakeingCount.WithLabelValues(h.addr).Set(float64(handshakecount))
	tcpServerDisConnCount.WithLabelValues(h.addr).Add(1)
	if removeClient {
		tcpServerClientingCount.WithLabelValues(h.addr).Set(float64(h.server.ClientCount()))
	}

	errDesc := "nil"
	if closeReason != nil {
		errDesc = closeReason.Error()
		for _, exp := range connCloseReasonRegexp {
			k, err := exp.Replace(errDesc, "*", 0, -1)
			if err == nil {
				errDesc = k
			}
		}
	}
	tcpServerConnCloseReason.WithLabelValues(h.addr, errDesc).Add(1)

	remote := tc.RemoteAddr()
	if tcpServerConnSendDataSize != nil {
		tcpServerConnSendDataSize.DeleteLabelValues(h.addr, remote.String())
	}
	if tcpServerConnRecvDataSize != nil {
		tcpServerConnRecvDataSize.DeleteLabelValues(h.addr, remote.String())
	}
	if tcpServerConnSendMsgCount != nil {
		tcpServerConnSendMsgCount.DeleteLabelValues(h.addr, remote.String())
	}
	if tcpServerConnRecvMsgCount != nil {
		tcpServerConnRecvMsgCount.DeleteLabelValues(h.addr, remote.String())
	}
	if tcpServerConnRecvSeqCount != nil {
		tcpServerConnRecvSeqCount.DeleteLabelValues(h.addr, remote.String())
	}
}

func (h *tcpServerHook[ClientInfo]) OnAddClient(tc *tcpserver.TCPClient[ClientInfo]) {
	h.init()
	tcpServerClientingCount.WithLabelValues(h.addr).Set(float64(h.server.ClientCount()))
}

func (h *tcpServerHook[ClientInfo]) OnRemoveClient(tc *tcpserver.TCPClient[ClientInfo]) {
	h.init()
	tcpServerClientingCount.WithLabelValues(h.addr).Set(float64(h.server.ClientCount()))
}

func (h *tcpServerHook[ClientInfo]) OnSendData(tc *tcpserver.TCPClient[ClientInfo], len int) {
	h.init()
	tcpServerSendDataSize.WithLabelValues(h.addr).Add(float64(len))
	if tcpServerConnSendDataSize != nil {
		addr := tc.RemoteAddr()
		tcpServerConnSendDataSize.WithLabelValues(h.addr, addr.String()).Add(float64(len))
	}
}

func (h *tcpServerHook[ClientInfo]) OnRecvData(tc *tcpserver.TCPClient[ClientInfo], len int) {
	h.init()
	tcpServerRecvDataSize.WithLabelValues(h.addr).Add(float64(len))
	if tcpServerConnRecvDataSize != nil {
		remote := tc.RemoteAddr()
		tcpServerConnRecvDataSize.WithLabelValues(h.addr, remote.String()).Add(float64(len))
	}
}

func (h *tcpServerHook[ClientInfo]) OnSend(tc *tcpserver.TCPClient[ClientInfo], len_ int) {
	h.init()
	if tcpServerConnSendMsgCount != nil {
		addr := tc.RemoteAddr()
		tcpServerConnSendMsgCount.WithLabelValues(h.addr, addr.String()).Inc()
	}
	tcpServerSendCount.WithLabelValues(h.addr).Inc()
	tcpServerSendSize.WithLabelValues(h.addr).Add(float64(len_))
}

func (h *tcpServerHook[ClientInfo]) OnSendMsg(tc *tcpserver.TCPClient[ClientInfo], mr msger.Msger, len_ int) {
	h.init()
	if tcpServerConnSendMsgCount != nil {
		remote := tc.RemoteAddr()
		tcpServerConnSendMsgCount.WithLabelValues(h.addr, remote.String()).Inc()
	}
	if mner, _ := any(mr).(msger.MsgerName); mner != nil {
		tcpServerSendMsgCount.WithLabelValues(h.addr, mner.MsgName()).Inc()
		tcpServerSendMsgSize.WithLabelValues(h.addr, mner.MsgName()).Add(float64(len_))
	} else {
		tcpServerSendMsgCount.WithLabelValues(h.addr, mr.MsgID()).Inc()
		tcpServerSendMsgSize.WithLabelValues(h.addr, mr.MsgID()).Add(float64(len_))
	}
}

func (h *tcpServerHook[ClientInfo]) OnSendText(tc *tcpserver.TCPClient[ClientInfo], len_ int) {
	h.init()
	if tcpServerConnSendMsgCount != nil {
		remote := tc.RemoteAddr()
		tcpServerConnSendMsgCount.WithLabelValues(h.addr, remote.String()).Inc()
	}
	tcpServerSendTextCount.WithLabelValues(h.addr).Inc()
	tcpServerSendTextSize.WithLabelValues(h.addr).Add(float64(len_))
}

func (h *tcpServerHook[ClientInfo]) OnSendRPCMsg(tc *tcpserver.TCPClient[ClientInfo], rpcId interface{}, mr msger.Msger, elapsed time.Duration, len_ int) {
	h.init()
	if tcpServerConnSendMsgCount != nil {
		remote := tc.RemoteAddr()
		tcpServerConnSendMsgCount.WithLabelValues(h.addr, remote.String()).Inc()
	}
	if mner, _ := any(mr).(msger.MsgerName); mner != nil {
		tcpServerSendRPCMsgCount.WithLabelValues(h.addr, mner.MsgName()).Inc()
		tcpServerSendRPCMsgSize.WithLabelValues(h.addr, mner.MsgName()).Add(float64(len_))
		tcpServerSendRPCMsgTime.WithLabelValues(h.addr, mner.MsgName()).Add(float64(elapsed.Nanoseconds()))
	} else {
		tcpServerSendRPCMsgCount.WithLabelValues(h.addr, mr.MsgID()).Inc()
		tcpServerSendRPCMsgSize.WithLabelValues(h.addr, mr.MsgID()).Add(float64(len_))
		tcpServerSendRPCMsgTime.WithLabelValues(h.addr, mr.MsgID()).Add(float64(elapsed.Nanoseconds()))
	}
}

func (h *tcpServerHook[ClientInfo]) OnRecvMsg(tc *tcpserver.TCPClient[ClientInfo], mr msger.RecvMsger, len_ int) {
	h.init()
	if tcpServerConnRecvMsgCount != nil {
		remote := tc.RemoteAddr()
		tcpServerConnRecvMsgCount.WithLabelValues(h.addr, remote.String()).Inc()
	}
	if mner, _ := any(mr).(msger.MsgerName); mner != nil {
		tcpServerRecvMsgCount.WithLabelValues(h.addr, mner.MsgName()).Inc()
		tcpServerRecvMsgSize.WithLabelValues(h.addr, mner.MsgName()).Add(float64(len_))
	} else {
		tcpServerRecvMsgCount.WithLabelValues(h.addr, mr.MsgID()).Inc()
		tcpServerRecvMsgSize.WithLabelValues(h.addr, mr.MsgID()).Add(float64(len_))
	}
}

func (h *tcpServerHook[ClientInfo]) OnTick() {
	h.init()
	seqs := h.server.RecvSeqCount()
	num := 0
	for i, l := range seqs {
		remote := i.(*tcpserver.TCPClient[ClientInfo]).RemoteAddr()
		num += l
		if tcpServerConnRecvSeqCount != nil {
			tcpServerConnRecvSeqCount.WithLabelValues(h.addr, remote.String()).Set(float64(num))
		}
	}
	tcpServerRecvSeqCount.WithLabelValues(h.addr).Set(float64(num))

}
