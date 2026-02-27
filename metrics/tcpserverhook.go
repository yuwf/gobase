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

	// 每个连接的信息，有开关控制
	tcpServerConnSendDataSize *prometheus.CounterVec
	tcpServerConnRecvDataSize *prometheus.CounterVec
	tcpServerConnSendMsgCount *prometheus.CounterVec // 所有的发送
	tcpServerConnRecvMsgCount *prometheus.CounterVec
	tcpServerConnRecvSeqCount *prometheus.GaugeVec

	tcpServerSendDataSize *prometheus.CounterVec
	tcpServerRecvDataSize *prometheus.CounterVec
	tcpServerRecvSeqCount *prometheus.GaugeVec

	tcpServerSendCount prometheus.Counter
	tcpServerSendSize  prometheus.Counter

	tcpServerSendMsgCount *prometheus.CounterVec
	tcpServerSendMsgSize  *prometheus.CounterVec

	tcpServerSendTextCount prometheus.Counter
	tcpServerSendTextSize  prometheus.Counter

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
			tcpServerConnSendDataSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_conn_senddata_size"}, []string{"addr", "connname"})
			tcpServerConnRecvDataSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_conn_recvdata_size"}, []string{"addr", "connname"})
			tcpServerConnSendMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_conn_sendmsg_count"}, []string{"addr", "connname"})
			tcpServerConnRecvMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_conn_recvmsg_count"}, []string{"addr", "connname"})
			tcpServerConnRecvSeqCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "tcpserver_conn_recvseqmsg_count"}, []string{"addr", "connname"})
		}

		tcpServerSendDataSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_senddata_size"}, []string{"addr"})
		tcpServerRecvDataSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_recvdata_size"}, []string{"addr"})
		tcpServerRecvSeqCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "tcpserver_recvseqmsg_count"}, []string{"addr"})

		tcpServerSendCount = DefaultReg().NewCounter(prometheus.CounterOpts{Name: "tcpserver_send_count"})
		tcpServerSendSize = DefaultReg().NewCounter(prometheus.CounterOpts{Name: "tcpserver_send_size"})

		tcpServerSendMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_sendmsg_count"}, []string{"name"})
		tcpServerSendMsgSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_sendmsg_size"}, []string{"name"})

		tcpServerSendTextCount = DefaultReg().NewCounter(prometheus.CounterOpts{Name: "tcpserver_sendtext_count"})
		tcpServerSendTextSize = DefaultReg().NewCounter(prometheus.CounterOpts{Name: "tcpserver_sendtext_size"})

		tcpServerSendRPCMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_sendrpcmsg_count"}, []string{"name"})
		tcpServerSendRPCMsgSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_sendrpcmsg_size"}, []string{"name"})
		tcpServerSendRPCMsgTime = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_sendrpcmsg_time"}, []string{"name"})

		tcpServerRecvMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_recvmsg_count"}, []string{"name"})
		tcpServerRecvMsgSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_recvmsg_size"}, []string{"name"})
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

	if tcpServerConnSendDataSize != nil {
		tcpServerConnSendDataSize.DeleteLabelValues(h.addr, tc.ConnName())
	}
	if tcpServerConnRecvDataSize != nil {
		tcpServerConnRecvDataSize.DeleteLabelValues(h.addr, tc.ConnName())
	}
	if tcpServerConnSendMsgCount != nil {
		tcpServerConnSendMsgCount.DeleteLabelValues(h.addr, tc.ConnName())
	}
	if tcpServerConnRecvMsgCount != nil {
		tcpServerConnRecvMsgCount.DeleteLabelValues(h.addr, tc.ConnName())
	}
	if tcpServerConnRecvSeqCount != nil {
		tcpServerConnRecvSeqCount.DeleteLabelValues(h.addr, tc.ConnName())
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
		tcpServerConnSendDataSize.WithLabelValues(h.addr, tc.ConnName()).Add(float64(len))
	}
}

func (h *tcpServerHook[ClientInfo]) OnRecvData(tc *tcpserver.TCPClient[ClientInfo], len int) {
	h.init()
	tcpServerRecvDataSize.WithLabelValues(h.addr).Add(float64(len))
	if tcpServerConnRecvDataSize != nil {
		tcpServerConnRecvDataSize.WithLabelValues(h.addr, tc.ConnName()).Add(float64(len))
	}
}

func (h *tcpServerHook[ClientInfo]) OnSend(tc *tcpserver.TCPClient[ClientInfo], len_ int) {
	h.init()
	if tcpServerConnSendMsgCount != nil {
		tcpServerConnSendMsgCount.WithLabelValues(h.addr, tc.ConnName()).Inc()
	}
	tcpServerSendCount.Inc()
	tcpServerSendSize.Add(float64(len_))
}

func (h *tcpServerHook[ClientInfo]) OnSendMsg(tc *tcpserver.TCPClient[ClientInfo], mr msger.Msger, len_ int) {
	h.init()
	if tcpServerConnSendMsgCount != nil {
		tcpServerConnSendMsgCount.WithLabelValues(h.addr, tc.ConnName()).Inc()
	}
	if mner, _ := any(mr).(msger.MsgerName); mner != nil {
		tcpServerSendMsgCount.WithLabelValues(mner.MsgName()).Inc()
		tcpServerSendMsgSize.WithLabelValues(mner.MsgName()).Add(float64(len_))
	} else {
		tcpServerSendMsgCount.WithLabelValues(mr.MsgID()).Inc()
		tcpServerSendMsgSize.WithLabelValues(mr.MsgID()).Add(float64(len_))
	}
}

func (h *tcpServerHook[ClientInfo]) OnSendText(tc *tcpserver.TCPClient[ClientInfo], len_ int) {
	h.init()
	if tcpServerConnSendMsgCount != nil {
		tcpServerConnSendMsgCount.WithLabelValues(h.addr, tc.ConnName()).Inc()
	}
	tcpServerSendTextCount.Inc()
	tcpServerSendTextSize.Add(float64(len_))
}

func (h *tcpServerHook[ClientInfo]) OnSendRPCMsg(tc *tcpserver.TCPClient[ClientInfo], rpcId interface{}, mr msger.Msger, elapsed time.Duration, len_ int) {
	h.init()
	if tcpServerConnSendMsgCount != nil {
		tcpServerConnSendMsgCount.WithLabelValues(h.addr, tc.ConnName()).Inc()
	}
	if mner, _ := any(mr).(msger.MsgerName); mner != nil {
		tcpServerSendRPCMsgCount.WithLabelValues(mner.MsgName()).Inc()
		tcpServerSendRPCMsgSize.WithLabelValues(mner.MsgName()).Add(float64(len_))
		tcpServerSendRPCMsgTime.WithLabelValues(mner.MsgName()).Add(float64(elapsed.Nanoseconds()))
	} else {
		tcpServerSendRPCMsgCount.WithLabelValues(mr.MsgID()).Inc()
		tcpServerSendRPCMsgSize.WithLabelValues(mr.MsgID()).Add(float64(len_))
		tcpServerSendRPCMsgTime.WithLabelValues(mr.MsgID()).Add(float64(elapsed.Nanoseconds()))
	}
}

func (h *tcpServerHook[ClientInfo]) OnRecvMsg(tc *tcpserver.TCPClient[ClientInfo], mr msger.RecvMsger, len_ int) {
	h.init()
	if tcpServerConnRecvMsgCount != nil {
		tcpServerConnRecvMsgCount.WithLabelValues(h.addr, tc.ConnName()).Inc()
	}
	if mner, _ := any(mr).(msger.MsgerName); mner != nil {
		tcpServerRecvMsgCount.WithLabelValues(mner.MsgName()).Inc()
		tcpServerRecvMsgSize.WithLabelValues(mner.MsgName()).Add(float64(len_))
	} else {
		tcpServerRecvMsgCount.WithLabelValues(mr.MsgID()).Inc()
		tcpServerRecvMsgSize.WithLabelValues(mr.MsgID()).Add(float64(len_))
	}
}

func (h *tcpServerHook[ClientInfo]) OnTick() {
	h.init()
	seqs := h.server.RecvSeqCount()
	num := 0
	for connName, l := range seqs {
		num += l
		if tcpServerConnRecvSeqCount != nil {
			tcpServerConnRecvSeqCount.WithLabelValues(h.addr, connName).Set(float64(num))
		}
	}
	tcpServerRecvSeqCount.WithLabelValues(h.addr).Set(float64(num))

}
