package metrics

// https://github.com/yuwf/gobase

import (
	"sync"
	"time"

	"github.com/yuwf/gobase/backend"
	"github.com/yuwf/gobase/msger"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// TCPBackend
	tcpBackendOnce   sync.Once
	tcpBackendServer *prometheus.GaugeVec
	tcpBackendConned *prometheus.GaugeVec

	tcpBackendConnSendDataSize *prometheus.CounterVec
	tcpBackendConnRecvDataSize *prometheus.CounterVec
	tcpBackendConnSendMsgCount *prometheus.CounterVec
	tcpBackendConnRecvMsgCount *prometheus.CounterVec
	tcpBackendConnRecvSeqCount *prometheus.GaugeVec

	tcpBackendSendCount prometheus.Counter
	tcpBackendSendSize  prometheus.Counter

	tcpBackendSendMsgCount *prometheus.CounterVec
	tcpBackendSendMsgSize  *prometheus.CounterVec

	tcpBackendSendRPCMsgCount *prometheus.CounterVec
	tcpBackendSendRPCMsgSize  *prometheus.CounterVec
	tcpBackendSendRPCMsgTime  *prometheus.CounterVec

	tcpBackendRecvMsgCount *prometheus.CounterVec
	tcpBackendRecvMsgSize  *prometheus.CounterVec
)

type tcpBackendHook[ServiceInfo any] struct {
}

func (h *tcpBackendHook[ServiceInfo]) init() {
	tcpBackendOnce.Do(func() {
		tcpBackendServer = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "tcpbackend_server"}, []string{"connname"})
		tcpBackendConned = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "tcpbackend_conned"}, []string{"connname"})

		tcpBackendConnSendDataSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpbackend_conn_senddata_size"}, []string{"connname"})
		tcpBackendConnRecvDataSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpbackend_conn_recvdata_size"}, []string{"connname"})
		tcpBackendConnSendMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpbackend_conn_sendmsg_count"}, []string{"connname"})
		tcpBackendConnRecvMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpbackend_conn_sendmsg_size"}, []string{"connname"})
		tcpBackendConnRecvSeqCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "tcpbackend_conn_recvseq_count"}, []string{"connname"})

		tcpBackendSendCount = DefaultReg().NewCounter(prometheus.CounterOpts{Name: "tcpbackend_send_count"})
		tcpBackendSendSize = DefaultReg().NewCounter(prometheus.CounterOpts{Name: "tcpbackend_send_size"})

		tcpBackendSendMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpbackend_sendmsg_count"}, []string{"name"})
		tcpBackendSendMsgSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpbackend_sendmsg_size"}, []string{"name"})

		tcpBackendSendRPCMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpbackend_sendrpcmsg_count"}, []string{"name"})
		tcpBackendSendRPCMsgSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpbackend_sendrpcmsg_size"}, []string{"name"})
		tcpBackendSendRPCMsgTime = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpbackend_sendrpcmsg_time"}, []string{"name"})

		tcpBackendRecvMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpbackend_recvmsg_count"}, []string{"name"})
		tcpBackendRecvMsgSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "tcpbackend_recvmsg_size"}, []string{"name"})
	})
}

func (h *tcpBackendHook[ServiceInfo]) OnAdd(ts *backend.TcpService[ServiceInfo]) {
	h.init()
	tcpBackendServer.WithLabelValues(ts.ConnName()).Set(1)
}

func (h *tcpBackendHook[ServiceInfo]) OnRemove(ts *backend.TcpService[ServiceInfo]) {
	h.init()
	tcpBackendServer.DeleteLabelValues(ts.ConnName())
	tcpBackendConned.DeleteLabelValues(ts.ConnName())
	tcpBackendConnSendDataSize.DeleteLabelValues(ts.ConnName())
	tcpBackendConnRecvDataSize.DeleteLabelValues(ts.ConnName())

	tcpBackendConnSendDataSize.DeleteLabelValues(ts.ConnName())
	tcpBackendConnRecvDataSize.DeleteLabelValues(ts.ConnName())
	tcpBackendConnSendMsgCount.DeleteLabelValues(ts.ConnName())
	tcpBackendConnRecvMsgCount.DeleteLabelValues(ts.ConnName())
	tcpBackendConnRecvSeqCount.DeleteLabelValues(ts.ConnName())
}

func (h *tcpBackendHook[ServiceInfo]) OnConnected(ts *backend.TcpService[ServiceInfo]) {
	h.init()
	tcpBackendConned.WithLabelValues(ts.ConnName()).Set(1)
}

func (h *tcpBackendHook[ServiceInfo]) OnDisConnect(ts *backend.TcpService[ServiceInfo]) {
	h.init()
	tcpBackendConned.DeleteLabelValues(ts.ConnName())
}

func (h *tcpBackendHook[ServiceInfo]) OnSendData(ts *backend.TcpService[ServiceInfo], len int) {
	h.init()
	tcpBackendConnSendDataSize.WithLabelValues(ts.ConnName()).Add(float64(len))
}

func (h *tcpBackendHook[ServiceInfo]) OnRecvData(ts *backend.TcpService[ServiceInfo], len int) {
	h.init()
	tcpBackendConnRecvDataSize.WithLabelValues(ts.ConnName()).Add(float64(len))
	tcpBackendConnRecvSeqCount.WithLabelValues(ts.ConnName()).Set(float64(ts.RecvSeqCount()))
}

func (h *tcpBackendHook[ServiceInfo]) OnSend(ts *backend.TcpService[ServiceInfo], len_ int) {
	h.init()
	tcpBackendConnSendMsgCount.WithLabelValues(ts.ConnName()).Inc()
	tcpBackendSendCount.Inc()
	tcpBackendSendSize.Add(float64(len_))
}

func (h *tcpBackendHook[ServiceInfo]) OnSendMsg(ts *backend.TcpService[ServiceInfo], mr msger.Msger, len_ int) {
	h.init()
	tcpBackendConnSendMsgCount.WithLabelValues(ts.ConnName()).Inc()
	if mner, _ := any(mr).(msger.MsgerName); mner != nil {
		tcpBackendSendMsgCount.WithLabelValues(mner.MsgName()).Inc()
		tcpBackendSendMsgSize.WithLabelValues(mner.MsgName()).Add(float64(len_))
	} else {
		tcpBackendSendMsgCount.WithLabelValues(mr.MsgID()).Inc()
		tcpBackendSendMsgSize.WithLabelValues(mr.MsgID()).Add(float64(len_))
	}
}

func (h *tcpBackendHook[ServiceInfo]) OnSendRPCMsg(ts *backend.TcpService[ServiceInfo], rpcId interface{}, mr msger.Msger, elapsed time.Duration, len_ int) {
	tcpBackendConnSendMsgCount.WithLabelValues(ts.ConnName()).Inc()
	h.init()
	if mner, _ := any(mr).(msger.MsgerName); mner != nil {
		tcpBackendSendRPCMsgCount.WithLabelValues(mner.MsgName()).Inc()
		tcpBackendSendRPCMsgSize.WithLabelValues(mner.MsgName()).Add(float64(len_))
		tcpBackendSendRPCMsgTime.WithLabelValues(mner.MsgName()).Add(float64(elapsed.Nanoseconds()))
	} else {
		tcpBackendSendRPCMsgCount.WithLabelValues(mr.MsgID()).Inc()
		tcpBackendSendRPCMsgSize.WithLabelValues(mr.MsgID()).Add(float64(len_))
		tcpBackendSendRPCMsgTime.WithLabelValues(mr.MsgID()).Add(float64(elapsed.Nanoseconds()))
	}
}

func (h *tcpBackendHook[ServiceInfo]) OnRecvMsg(ts *backend.TcpService[ServiceInfo], mr msger.RecvMsger, len_ int) {
	h.init()
	tcpBackendConnRecvMsgCount.WithLabelValues(ts.ConnName()).Inc()
	if mner, _ := any(mr).(msger.MsgerName); mner != nil {
		tcpBackendRecvMsgCount.WithLabelValues(mner.MsgName()).Inc()
		tcpBackendRecvMsgSize.WithLabelValues(mner.MsgName()).Add(float64(len_))
	} else {
		tcpBackendRecvMsgCount.WithLabelValues(mr.MsgID()).Inc()
		tcpBackendRecvMsgSize.WithLabelValues(mr.MsgID()).Add(float64(len_))
	}
}
