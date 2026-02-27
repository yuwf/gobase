package metrics

// https://github.com/yuwf/gobase

import (
	"sync"

	"github.com/yuwf/gobase/backend"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// HTTPBackend
	httpBackendOnce   sync.Once
	httpBackendServer *prometheus.GaugeVec
	httpBackendConned *prometheus.GaugeVec
)

type httpBackendHook[ServerInfo any] struct {
}

func (h *httpBackendHook[ServerInfo]) init() {
	httpBackendOnce.Do(func() {
		httpBackendServer = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "httpbackend_server"}, []string{"servicename", "serviceid"})
		httpBackendConned = DefaultReg().NewGaugeVec(prometheus.GaugeOpts{Name: "httpbackend_conned"}, []string{"servicename", "serviceid"})
	})
}

func (h *httpBackendHook[ServerInfo]) OnAdd(hs *backend.HttpService[ServerInfo]) {
	h.init()
	httpBackendServer.WithLabelValues(hs.ServiceName(), hs.ServiceId()).Set(1)
}
func (h *httpBackendHook[ServerInfo]) OnRemove(hs *backend.HttpService[ServerInfo]) {
	h.init()
	httpBackendServer.DeleteLabelValues(hs.ServiceName(), hs.ServiceId())
	httpBackendConned.DeleteLabelValues(hs.ServiceName(), hs.ServiceId())
}
func (h *httpBackendHook[ServerInfo]) OnConnected(hs *backend.HttpService[ServerInfo]) {
	h.init()
	httpBackendConned.WithLabelValues(hs.ServiceName(), hs.ServiceId()).Set(1)
}
func (h *httpBackendHook[ServerInfo]) OnDisConnect(hs *backend.HttpService[ServerInfo]) {
	h.init()
	httpBackendConned.DeleteLabelValues(hs.ServiceName(), hs.ServiceId())
}
