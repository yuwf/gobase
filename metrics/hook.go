package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/yuwf/gobase/backend"
	"github.com/yuwf/gobase/gnetserver"
	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/httprequest"
	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/redis"
	"github.com/yuwf/gobase/tcpserver"
)

var (
	// Redis
	redisCnt      = promauto.NewCounterVec(prometheus.CounterOpts{Name: "redis_count"}, []string{"cmd", "key", "caller"})
	redisCntError = promauto.NewCounterVec(prometheus.CounterOpts{Name: "redis_error"}, []string{"cmd", "key", "caller"})
	redisLatency  = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "redis_latency",
		Buckets: []float64{1, 2, 4, 8, 16, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384}},
		[]string{"cmd", "key", "caller"},
	)
	redisKeyRegexp *regexp2.Regexp

	// MySQL
	mysqlCnt      = promauto.NewCounterVec(prometheus.CounterOpts{Name: "mysql_count"}, []string{"cmd", "caller"})
	mysqlCntError = promauto.NewCounterVec(prometheus.CounterOpts{Name: "mysql_error"}, []string{"cmd", "caller"})
	mysqlLatency  = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "mysql_latency",
		Buckets: []float64{1, 2, 4, 8, 16, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384}},
		[]string{"cmd", "caller"},
	)

	// http
	httpCnt      = promauto.NewCounterVec(prometheus.CounterOpts{Name: "http_count"}, []string{"host", "path"})
	httpCntError = promauto.NewCounterVec(prometheus.CounterOpts{Name: "http_error"}, []string{"host", "path"})
	httpLatency  = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "http_latency",
		Buckets: []float64{1, 2, 4, 8, 16, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384}},
		[]string{"host", "path"},
	)
	httpPathRegexp *regexp2.Regexp

	// gin
	ginCnt = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gin_count"}, []string{"method", "path"})
	//ginCntError = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gin_error"}, []string{"method", "path"})
	ginLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: "gin_latency",
		Buckets: []float64{1, 2, 4, 8, 16, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384}},
		[]string{"method", "path"},
	)
	ginPathRegexp *regexp2.Regexp

	// gnet
	gnetConningCount      = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "gnet_conning_count"}, []string{"addr"})
	gnetClientingCount    = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "gnet_clienting_count"}, []string{"addr"})
	gnetHandShakeingCount = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "gnet_handshakeing_count"}, []string{"addr"})
	gnetConnCloseReason   = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gnet_conn_close_reason"}, []string{"addr"})
	gnetConnCount         = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gnet_conn_count"}, []string{"addr"})
	gnetHandShakeCount    = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gnet_handshake_count"}, []string{"addr"})
	gnetDisConnCount      = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gnet_disconn_count"}, []string{"addr"})
	gnetSendSize          = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gnet_send_size"}, []string{"addr"})
	gnetRecvSize          = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gnet_recv_size"}, []string{"addr"})

	// TCPServer
	tcpServerConningCount      = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "tcpserver_conning_count"}, []string{"addr"})
	tcpServerClientingCount    = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "tcpserver_clienting_count"}, []string{"addr"})
	tcpServerHandShakeingCount = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "tcpserver_handshakeing_count"}, []string{"addr"})
	tcpServerConnCloseReason   = promauto.NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_conn_close_reason"}, []string{"addr"})
	tcpServerConnCount         = promauto.NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_conn_count"}, []string{"addr"})
	tcpServerHandShakeCount    = promauto.NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_handshake_count"}, []string{"addr"})
	tcpServerDisConnCount      = promauto.NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_disconn_count"}, []string{"addr"})
	tcpServerSendSize          = promauto.NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_send_size"}, []string{"addr"})
	tcpServerRecvSize          = promauto.NewCounterVec(prometheus.CounterOpts{Name: "tcpserver_recv_size"}, []string{"addr"})

	// TCPBackend
	tcpBackendServer   = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "tcpbackend_server"}, []string{"servicename", "serviceid"})
	tcpBackendConned   = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "tcpbackend_conned"}, []string{"servicename", "serviceid"})
	tcpBackendSendSize = promauto.NewCounterVec(prometheus.CounterOpts{Name: "tcpbackend_send_size"}, []string{"servicename", "serviceid"})
	tcpBackendRecvSize = promauto.NewCounterVec(prometheus.CounterOpts{Name: "tcpbackend_recv_size"}, []string{"servicename", "serviceid"})

	// HTTPBackend
	httpBackendServer = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "httpbackend_server"}, []string{"servicename", "serviceid"})
	httpBackendConned = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: "httpbackend_conned"}, []string{"servicename", "serviceid"})
)

func init() {
	var err error
	// 分割 /\{}[]<>_-:.@#
	redisKeyRegexp, err = regexp2.Compile(`(?<=[/\\\{:_\-\.@#])(\d+|[^/\\\{\}:_\-\.@#]{17,})(?=[/\\\{\}:_\-\.@#]|$)`, regexp2.None)
	if err != nil {
		panic(err.Error())
	}
	// 分割 /
	httpPathRegexp, err = regexp2.Compile(`(?<=[/])(\d+|[^/]{17,})(?=[/]|$)`, regexp2.None)
	if err != nil {
		panic(err.Error())
	}
	// 分割 /
	ginPathRegexp, err = regexp2.Compile(`(?<=[/])(\d+|[^/]{17,})(?=[/]|$)`, regexp2.None) //regexp \d+|[^/]{16,}
	if err != nil {
		panic(err.Error())
	}
}

func redisHook(ctx context.Context, cmd *redis.RedisCommond) {
	// 找到key
	var key string
	if len(cmd.Args) >= 1 {
		k := fmt.Sprintf("%v", cmd.Args[0])
		k, err := redisKeyRegexp.Replace(k, "*", 0, -1)
		if err == nil {
			key = k
		}
	}
	redisCnt.WithLabelValues(strings.ToUpper(cmd.Cmd), key, cmd.Caller.Name()).Inc()
	if cmd.Err != nil {
		redisCntError.WithLabelValues(strings.ToUpper(cmd.Cmd), key, cmd.Caller.Name()).Inc()
	}
	redisLatency.WithLabelValues(strings.ToUpper(cmd.Cmd), key, cmd.Caller.Name()).Observe(float64(cmd.Elapsed / time.Millisecond))
}

func goredisHook(ctx context.Context, cmd *goredis.RedisCommond) {
	if cmd.Cmd != nil && len(cmd.Cmd.Args()) > 0 {
		// 找到key
		cmdName := fmt.Sprint(cmd.Cmd.Args()[0])
		var key string
		pos := goredis.GetFirstKeyPos(cmd.Cmd)
		if pos < len(cmd.Cmd.Args()) {
			for i := 1; i < pos; i++ {
				cmdName += fmt.Sprint(cmd.Cmd.Args()[i])
			}
			key = fmt.Sprint(cmd.Cmd.Args()[pos])
			key, _ = redisKeyRegexp.Replace(key, "*", 0, -1)
		}

		redisCnt.WithLabelValues(cmdName, key, cmd.Caller.Name()).Inc()
		if cmd.Cmd.Err() != nil && !goredis.IsNilError(cmd.Cmd.Err()) {
			redisCntError.WithLabelValues(cmdName, key, cmd.Caller.Name()).Inc()
		}
		redisLatency.WithLabelValues(cmdName, key, cmd.Caller.Name()).Observe(float64(cmd.Elapsed / time.Millisecond))
	}
}

func mysqlHook(ctx context.Context, cmd *mysql.MySQLCommond) {
	mysqlCnt.WithLabelValues(strings.ToUpper(cmd.Cmd), cmd.Caller.Name()).Inc()
	if cmd.Err != nil {
		mysqlCntError.WithLabelValues(strings.ToUpper(cmd.Cmd), cmd.Caller.Name()).Inc()
	}
	mysqlLatency.WithLabelValues(strings.ToUpper(cmd.Cmd), cmd.Caller.Name()).Observe(float64(cmd.Elapsed / time.Millisecond))
}

func httpHook(ctx context.Context, request *httprequest.HttpRequest) {
	var host string
	var path string
	if request.URL != nil {
		host = request.URL.Host
		k, err := httpPathRegexp.Replace(request.URL.Path, "*", 0, -1)
		if err == nil {
			path = k
		}
	}
	httpCnt.WithLabelValues(host, path).Inc()
	if request.Err != nil {
		httpCntError.WithLabelValues(host, path).Inc()
	}
	httpLatency.WithLabelValues(host, path).Observe(float64(request.Elapsed / time.Millisecond))
}

func ginHook(ctx context.Context, c *gin.Context, elapsed time.Duration) {
	if c.Writer.Status() == http.StatusNotFound {
		return
	}
	var path string
	k, err := ginPathRegexp.Replace(c.Request.URL.Path, "*", 0, -1)
	if err == nil {
		path = k
	}
	ginCnt.WithLabelValues(strings.ToUpper(c.Request.Method), path).Inc()
	ginLatency.WithLabelValues(strings.ToUpper(c.Request.Method), path).Observe(float64(elapsed / time.Millisecond))
}

type ConnCount interface {
	ConnCount() (int, int)
	ClientCount() int
}

type gNetHook[ClientInfo any] struct {
	addr      string
	connCount ConnCount
}

func (h *gNetHook[ClientInfo]) OnConnected(gc *gnetserver.GNetClient[ClientInfo]) {
	count, _ := h.connCount.ConnCount()
	gnetConningCount.WithLabelValues(h.addr).Set(float64(count))
	gnetConnCount.WithLabelValues(h.addr).Add(1)
}
func (h *gNetHook[ClientInfo]) OnWSHandShake(gc *gnetserver.GNetClient[ClientInfo]) {
	_, handshakecount := h.connCount.ConnCount()
	gnetHandShakeCount.WithLabelValues(h.addr).Add(1)
	gnetHandShakeingCount.WithLabelValues(h.addr).Set(float64(handshakecount))
}
func (h *gNetHook[ClientInfo]) OnDisConnect(gc *gnetserver.GNetClient[ClientInfo], removeClient bool, closeReason error) {
	count, handshakecount := h.connCount.ConnCount()
	gnetConningCount.WithLabelValues(h.addr).Set(float64(count))
	gnetHandShakeingCount.WithLabelValues(h.addr).Set(float64(handshakecount))
	gnetDisConnCount.WithLabelValues(h.addr).Add(1)
	if removeClient {
		gnetClientingCount.WithLabelValues(h.addr).Set(float64(h.connCount.ClientCount()))
	}
	if closeReason != nil {
		gnetConnCloseReason.WithLabelValues(closeReason.Error()).Add(1)
	} else {
		gnetConnCloseReason.WithLabelValues("nil").Add(1)
	}
}
func (h *gNetHook[ClientInfo]) OnAddClient(gc *gnetserver.GNetClient[ClientInfo]) {
	gnetClientingCount.WithLabelValues(h.addr).Set(float64(h.connCount.ClientCount()))
}
func (h *gNetHook[ClientInfo]) OnRemoveClient(gc *gnetserver.GNetClient[ClientInfo]) {
	gnetClientingCount.WithLabelValues(h.addr).Set(float64(h.connCount.ClientCount()))
}
func (h *gNetHook[ClientInfo]) OnSend(gc *gnetserver.GNetClient[ClientInfo], len int) {
	gnetSendSize.WithLabelValues(h.addr).Add(float64(len))
}
func (h *gNetHook[ClientInfo]) OnRecv(gc *gnetserver.GNetClient[ClientInfo], len int) {
	gnetRecvSize.WithLabelValues(h.addr).Add(float64(len))
}

type tcpServerHook[ClientInfo any] struct {
	addr      string
	connCount ConnCount
}

func (h *tcpServerHook[ClientInfo]) OnConnected(tc *tcpserver.TCPClient[ClientInfo]) {
	count, _ := h.connCount.ConnCount()
	tcpServerConningCount.WithLabelValues(h.addr).Set(float64(count))
	tcpServerConnCount.WithLabelValues(h.addr).Add(1)
}
func (h *tcpServerHook[ClientInfo]) OnWSHandShake(gc *tcpserver.TCPClient[ClientInfo]) {
	_, handshakecount := h.connCount.ConnCount()
	tcpServerHandShakeCount.WithLabelValues(h.addr).Add(1)
	tcpServerHandShakeingCount.WithLabelValues(h.addr).Set(float64(handshakecount))
}
func (h *tcpServerHook[ClientInfo]) OnDisConnect(tc *tcpserver.TCPClient[ClientInfo], removeClient bool, closeReason error) {
	count, handshakecount := h.connCount.ConnCount()
	tcpServerConningCount.WithLabelValues(h.addr).Set(float64(count))
	tcpServerHandShakeingCount.WithLabelValues(h.addr).Set(float64(handshakecount))
	tcpServerDisConnCount.WithLabelValues(h.addr).Add(1)
	if removeClient {
		tcpServerClientingCount.WithLabelValues(h.addr).Set(float64(h.connCount.ClientCount()))
	}
	if closeReason != nil {
		tcpServerConnCloseReason.WithLabelValues(closeReason.Error()).Add(1)
	} else {
		tcpServerConnCloseReason.WithLabelValues("nil").Add(1)
	}
}
func (h *tcpServerHook[ClientInfo]) OnAddClient(tc *tcpserver.TCPClient[ClientInfo]) {
	tcpServerClientingCount.WithLabelValues(h.addr).Set(float64(h.connCount.ClientCount()))
}
func (h *tcpServerHook[ClientInfo]) OnRemoveClient(tc *tcpserver.TCPClient[ClientInfo]) {
	tcpServerClientingCount.WithLabelValues(h.addr).Set(float64(h.connCount.ClientCount()))
}
func (h *tcpServerHook[ClientInfo]) OnSend(tc *tcpserver.TCPClient[ClientInfo], len int) {
	tcpServerSendSize.WithLabelValues(h.addr).Add(float64(len))
}
func (h *tcpServerHook[ClientInfo]) OnRecv(tc *tcpserver.TCPClient[ClientInfo], len int) {
	tcpServerRecvSize.WithLabelValues(h.addr).Add(float64(len))
}

type tcpBackendHook[ServerInfo any] struct {
}

func (h *tcpBackendHook[ServerInfo]) OnAdd(ts *backend.TcpService[ServerInfo]) {
	tcpBackendServer.WithLabelValues(ts.ServiceName(), ts.ServiceId()).Set(1)
}
func (h *tcpBackendHook[ServerInfo]) OnRemove(ts *backend.TcpService[ServerInfo]) {
	tcpBackendServer.DeleteLabelValues(ts.ServiceName(), ts.ServiceId())
	tcpBackendConned.DeleteLabelValues(ts.ServiceName(), ts.ServiceId())
	tcpBackendSendSize.DeleteLabelValues(ts.ServiceName(), ts.ServiceId())
	tcpBackendRecvSize.DeleteLabelValues(ts.ServiceName(), ts.ServiceId())
}
func (h *tcpBackendHook[ServerInfo]) OnConnected(ts *backend.TcpService[ServerInfo]) {
	tcpBackendConned.WithLabelValues(ts.ServiceName(), ts.ServiceId()).Set(1)
}
func (h *tcpBackendHook[ServerInfo]) OnDisConnect(ts *backend.TcpService[ServerInfo]) {
	tcpBackendConned.DeleteLabelValues(ts.ServiceName(), ts.ServiceId())
}
func (h *tcpBackendHook[ServerInfo]) OnSend(ts *backend.TcpService[ServerInfo], len int) {
	tcpBackendSendSize.WithLabelValues(ts.ServiceName(), ts.ServiceId()).Add(float64(len))
}
func (h *tcpBackendHook[ServerInfo]) OnRecv(ts *backend.TcpService[ServerInfo], len int) {
	tcpBackendRecvSize.WithLabelValues(ts.ServiceName(), ts.ServiceId()).Add(float64(len))
}

type httpBackendHook[ServerInfo any] struct {
}

func (h *httpBackendHook[ServerInfo]) OnAdd(hs *backend.HttpService[ServerInfo]) {
	httpBackendServer.WithLabelValues(hs.ServiceName(), hs.ServiceId()).Set(1)
}
func (h *httpBackendHook[ServerInfo]) OnRemove(hs *backend.HttpService[ServerInfo]) {
	httpBackendServer.DeleteLabelValues(hs.ServiceName(), hs.ServiceId())
	httpBackendConned.DeleteLabelValues(hs.ServiceName(), hs.ServiceId())
}
func (h *httpBackendHook[ServerInfo]) OnConnected(hs *backend.HttpService[ServerInfo]) {
	httpBackendConned.WithLabelValues(hs.ServiceName(), hs.ServiceId()).Set(1)
}
func (h *httpBackendHook[ServerInfo]) OnDisConnect(hs *backend.HttpService[ServerInfo]) {
	httpBackendConned.DeleteLabelValues(hs.ServiceName(), hs.ServiceId())
}
