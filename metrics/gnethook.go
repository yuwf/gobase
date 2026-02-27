package metrics

// https://github.com/yuwf/gobase

import (
	"sync"

	"github.com/yuwf/gobase/gnetserver"
	"github.com/yuwf/gobase/msger"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// gnet
	gnetOnce              sync.Once
	gnetConningCount      *prometheus.GaugeVec
	gnetClientingCount    *prometheus.GaugeVec
	gnetHandShakeingCount *prometheus.GaugeVec
	gnetConnCloseReason   *prometheus.CounterVec
	gnetConnCount         *prometheus.CounterVec
	gnetHandShakeCount    *prometheus.CounterVec
	gnetDisConnCount      *prometheus.CounterVec

	gnetSendDataSize *prometheus.CounterVec
	gnetRecvDataSize *prometheus.CounterVec
	gnetRecvSeqCount *prometheus.GaugeVec

	gnetSendCount prometheus.Counter
	gnetSendSize  prometheus.Counter

	gnetSendMsgCount *prometheus.CounterVec
	gnetSendMsgSize  *prometheus.CounterVec

	gnetSendTextCount prometheus.Counter
	gnetSendTextSize  prometheus.Counter

	gnetRecvMsgCount *prometheus.CounterVec
	gnetRecvMsgSize  *prometheus.CounterVec
)

type gNetHook[ClientInfo any] struct {
	addr   string // 监听地址
	server msger.ServerTermianl
}

func (h *gNetHook[ClientInfo]) init() {
	gnetOnce.Do(func() {
		gnetConningCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "gnet_conning_count"}, []string{"addr"})
		gnetClientingCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "gnet_clienting_count"}, []string{"addr"})
		gnetHandShakeingCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "gnet_handshakeing_count"}, []string{"addr"})
		gnetConnCloseReason = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "gnet_conn_close_reason"}, []string{"addr", "err"})
		gnetConnCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "gnet_conn_count"}, []string{"addr"})
		gnetHandShakeCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "gnet_handshake_count"}, []string{"addr"})
		gnetDisConnCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "gnet_disconn_count"}, []string{"addr"})

		gnetSendDataSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "gnet_senddata_size"}, []string{"addr"})
		gnetRecvDataSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "gnet_recvdata_size"}, []string{"addr"})
		gnetRecvSeqCount = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "gnet_recvseq_count"}, []string{"addr"})

		gnetSendCount = DefaultReg().NewCounter(prometheus.CounterOpts{Name: "gnet_send_count"})
		gnetSendSize = DefaultReg().NewCounter(prometheus.CounterOpts{Name: "gnet_send_size"})

		gnetSendMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "gnet_sendmsg_count"}, []string{"name"})
		gnetSendMsgSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "gnet_sendmsg_size"}, []string{"name"})

		gnetSendTextCount = DefaultReg().NewCounter(prometheus.CounterOpts{Name: "gnet_sendtext_count"})
		gnetSendTextSize = DefaultReg().NewCounter(prometheus.CounterOpts{Name: "gnet_sendtext_size"})

		gnetRecvMsgCount = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "gnet_recvmsg_count"}, []string{"name"})
		gnetRecvMsgSize = DefaultReg().NewCounterVec(prometheus.CounterOpts{Name: "gnet_recvmsg_size"}, []string{"name"})
	})
}

func (h *gNetHook[ClientInfo]) OnConnected(gc *gnetserver.GNetClient[ClientInfo]) {
	h.init()
	count, _ := h.server.ConnCount()
	gnetConningCount.WithLabelValues(h.addr).Set(float64(count))
	gnetConnCount.WithLabelValues(h.addr).Add(1)
}

func (h *gNetHook[ClientInfo]) OnWSHandShake(gc *gnetserver.GNetClient[ClientInfo]) {
	h.init()
	_, handshakecount := h.server.ConnCount()
	gnetHandShakeCount.WithLabelValues(h.addr).Add(1)
	gnetHandShakeingCount.WithLabelValues(h.addr).Set(float64(handshakecount))
}

func (h *gNetHook[ClientInfo]) OnDisConnect(gc *gnetserver.GNetClient[ClientInfo], removeClient bool, closeReason error) {
	h.init()
	count, handshakecount := h.server.ConnCount()
	gnetConningCount.WithLabelValues(h.addr).Set(float64(count))
	gnetHandShakeingCount.WithLabelValues(h.addr).Set(float64(handshakecount))
	gnetDisConnCount.WithLabelValues(h.addr).Add(1)
	if removeClient {
		gnetClientingCount.WithLabelValues(h.addr).Set(float64(h.server.ClientCount()))
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
	gnetConnCloseReason.WithLabelValues(h.addr, errDesc).Add(1)
}

func (h *gNetHook[ClientInfo]) OnAddClient(gc *gnetserver.GNetClient[ClientInfo]) {
	h.init()
	gnetClientingCount.WithLabelValues(h.addr).Set(float64(h.server.ClientCount()))
}

func (h *gNetHook[ClientInfo]) OnRemoveClient(gc *gnetserver.GNetClient[ClientInfo]) {
	h.init()
	gnetClientingCount.WithLabelValues(h.addr).Set(float64(h.server.ClientCount()))
}

func (h *gNetHook[ClientInfo]) OnSendData(gc *gnetserver.GNetClient[ClientInfo], len int) {
	h.init()
	gnetSendDataSize.WithLabelValues(h.addr).Add(float64(len))
}

func (h *gNetHook[ClientInfo]) OnRecvData(gc *gnetserver.GNetClient[ClientInfo], len int) {
	h.init()
	gnetRecvDataSize.WithLabelValues(h.addr).Add(float64(len))
}

func (h *gNetHook[ClientInfo]) OnSend(gc *gnetserver.GNetClient[ClientInfo], len_ int) {
	h.init()
	gnetSendCount.Inc()
	gnetSendSize.Add(float64(len_))
}

func (h *gNetHook[ClientInfo]) OnSendMsg(gc *gnetserver.GNetClient[ClientInfo], mr msger.Msger, len_ int) {
	h.init()
	if mner, _ := any(mr).(msger.MsgerName); mner != nil {
		gnetSendMsgCount.WithLabelValues(mner.MsgName()).Inc()
		gnetSendMsgSize.WithLabelValues(mner.MsgName()).Add(float64(len_))
	} else {
		gnetSendMsgCount.WithLabelValues(mr.MsgID()).Inc()
		gnetSendMsgSize.WithLabelValues(mr.MsgID()).Add(float64(len_))
	}
}

func (h *gNetHook[ClientInfo]) OnSendText(gc *gnetserver.GNetClient[ClientInfo], len_ int) {
	h.init()
	gnetSendTextCount.Inc()
	gnetSendTextSize.Add(float64(len_))
}

func (h *gNetHook[ClientInfo]) OnRecvMsg(gc *gnetserver.GNetClient[ClientInfo], mr msger.RecvMsger, len_ int) {
	h.init()
	if mner, _ := any(mr).(msger.MsgerName); mner != nil {
		gnetRecvMsgCount.WithLabelValues(mner.MsgName()).Inc()
		gnetRecvMsgSize.WithLabelValues(mner.MsgName()).Add(float64(len_))
	} else {
		gnetRecvMsgCount.WithLabelValues(mr.MsgID()).Inc()
		gnetRecvMsgSize.WithLabelValues(mr.MsgID()).Add(float64(len_))
	}
}

func (h *gNetHook[ClientInfo]) OnTick() {
	h.init()
	seqs := h.server.RecvSeqCount()
	num := 0
	for _, l := range seqs {
		num += l
	}
	gnetRecvSeqCount.WithLabelValues(h.addr).Set(float64(num))
}
