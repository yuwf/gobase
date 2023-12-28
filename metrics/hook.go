package metrics

// https://github.com/yuwf/gobase

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yuwf/gobase/backend"
	"github.com/yuwf/gobase/gnetserver"
	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/httprequest"
	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/redis"
	"github.com/yuwf/gobase/tcpserver"

	"github.com/dlclark/regexp2"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Redis
	redisOnce sync.Once
	//redisCnt       *prometheus.CounterVec
	redisCntError  *prometheus.CounterVec
	redisLatency   *prometheus.HistogramVec
	redisKeyRegexp []*regexp2.Regexp

	// MySQL
	mysqlOnce sync.Once
	//mysqlCnt      *prometheus.CounterVec
	mysqlCntError *prometheus.CounterVec
	mysqlLatency  *prometheus.HistogramVec

	// http
	httpOnce sync.Once
	//httpCnt        *prometheus.CounterVec
	httpCntError   *prometheus.CounterVec
	httpLatency    *prometheus.HistogramVec
	httpPathRegexp *regexp2.Regexp

	// gin
	ginOnce sync.Once
	//ginCnt        *prometheus.CounterVec
	ginCntError   *prometheus.CounterVec
	ginLatency    *prometheus.HistogramVec
	ginPathRegexp *regexp2.Regexp

	// gnet
	gnetOnce              sync.Once
	gnetConningCount      *prometheus.GaugeVec
	gnetClientingCount    *prometheus.GaugeVec
	gnetHandShakeingCount *prometheus.GaugeVec
	gnetConnCloseReason   *prometheus.CounterVec
	gnetConnCount         *prometheus.CounterVec
	gnetHandShakeCount    *prometheus.CounterVec
	gnetDisConnCount      *prometheus.CounterVec
	gnetSendSize          *prometheus.CounterVec
	gnetRecvSize          *prometheus.CounterVec

	// TCPServer
	tcpServerOnce              sync.Once
	tcpServerConningCount      *prometheus.GaugeVec
	tcpServerClientingCount    *prometheus.GaugeVec
	tcpServerHandShakeingCount *prometheus.GaugeVec
	tcpServerConnCloseReason   *prometheus.CounterVec
	tcpServerConnCount         *prometheus.CounterVec
	tcpServerHandShakeCount    *prometheus.CounterVec
	tcpServerDisConnCount      *prometheus.CounterVec
	tcpServerSendSize          *prometheus.CounterVec
	tcpServerRecvSize          *prometheus.CounterVec

	// TCPBackend
	tcpBackendOnce     sync.Once
	tcpBackendServer   *prometheus.GaugeVec
	tcpBackendConned   *prometheus.GaugeVec
	tcpBackendSendSize *prometheus.CounterVec
	tcpBackendRecvSize *prometheus.CounterVec

	// HTTPBackend
	httpBackendOnce   sync.Once
	httpBackendServer *prometheus.GaugeVec
	httpBackendConned *prometheus.GaugeVec

	// MsgDispatch
	msgDispatchOnce    sync.Once
	msgDispatchLatency *prometheus.HistogramVec
)

func init() {
	var err error
	redisExpr := []string{
		`(?<=[/\\\{:_\-\.@#])(\d+|[^/\\\{\}:_\-\.@#]{17,})(?=[/\\\{\}:_\-\.@#]|$)`, //分割 /\{}[]<>_-:.@#
		`(?<=[/\\\{:_\.@#])(\d+|[^/\\\{\}:_\.@#]{17,})(?=[/\\\{\}:_\.@#]|$)`,       //分割 /\{}[]<>_:.@#
		`(?<=[/\\\{_\-\.@#])(\d+|[^/\\\{\}_\-\.@#]{17,})(?=[/\\\{\}_\-\.@#]|$)`,    //分割 /\{}[]<>_-.@#
		`(?<=[/\\\{:_\-@#])(\d+|[^/\\\{\}:_\-@#]{17,})(?=[/\\\{\}:_\-@#]|$)`,       //分割 /\{}[]<>_-:@#
	}
	redisKeyRegexp = make([]*regexp2.Regexp, len(redisExpr))
	for i, s := range redisExpr {
		// 分割
		redisKeyRegexp[i], err = regexp2.Compile(s, regexp2.None)
		if err != nil {
			panic(err.Error())
		}
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
	redisOnce.Do(func() {
		//redisCnt = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "redis_count"}, []string{"cmd", "key", "caller"})
		redisCntError = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "redis_error"}, []string{"cmd", "key", "caller"})
		redisLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: MetricsNamePrefix + "redis_latency",
			Buckets: []float64{1, 2, 4, 16, 64, 256, 1024, 2048, 4096, 16384, 65392, 261568, 1046272, 2092544, 4185088, 16740352}},
			[]string{"cmd", "key", "caller"},
		)
	})
	// 找到key
	var key string
	if len(cmd.Args) >= 1 {
		k := fmt.Sprintf("%v", cmd.Args[0])
		for _, exp := range redisKeyRegexp {
			k, err := exp.Replace(k, "*", 0, -1)
			if err == nil && (len(key) == 0 || len(k) < len(key)) {
				key = k
			}
		}
	}
	//redisCnt.WithLabelValues(strings.ToUpper(cmd.Cmd), key, cmd.Caller.Name()).Inc()
	if cmd.Err != nil {
		redisCntError.WithLabelValues(strings.ToUpper(cmd.Cmd), key, cmd.Caller.Name()).Inc()
	}
	redisLatency.WithLabelValues(strings.ToUpper(cmd.Cmd), key, cmd.Caller.Name()).Observe(float64(cmd.Elapsed) / float64(time.Microsecond))
}

func goredisHook(ctx context.Context, cmd *goredis.RedisCommond) {
	redisOnce.Do(func() {
		//redisCnt = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "redis_count"}, []string{"cmd", "key", "caller"})
		redisCntError = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "redis_error"}, []string{"cmd", "key", "caller"})
		redisLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: MetricsNamePrefix + "redis_latency",
			Buckets: []float64{1, 2, 4, 16, 64, 256, 1024, 2048, 4096, 16384, 65392, 261568, 1046272, 2092544, 4185088, 16740352}},
			[]string{"cmd", "key", "caller"},
		)
	})
	if cmd.Cmd != nil && len(cmd.Cmd.Args()) > 0 {
		// 找到key
		cmdName := fmt.Sprint(cmd.Cmd.Args()[0])
		var key string
		pos := goredis.GetFirstKeyPos(cmd.Cmd)
		if pos < len(cmd.Cmd.Args()) {
			//for i := 1; i < pos; i++ {
			//	cmdName += fmt.Sprint(cmd.Cmd.Args()[i])
			//}
			k := fmt.Sprint(cmd.Cmd.Args()[pos])
			for _, exp := range redisKeyRegexp {
				k, err := exp.Replace(k, "*", 0, -1)
				if err == nil && (len(key) == 0 || len(k) < len(key)) {
					key = k
				}
			}
		}

		//redisCnt.WithLabelValues(cmdName, key, cmd.Caller.Name()).Inc()
		if cmd.Cmd.Err() != nil && !goredis.IsNilError(cmd.Cmd.Err()) {
			redisCntError.WithLabelValues(cmdName, key, cmd.Caller.Name()).Inc()
		}
		redisLatency.WithLabelValues(cmdName, key, cmd.Caller.Name()).Observe(float64(cmd.Elapsed) / float64(time.Microsecond))
	}
}

func mysqlHook(ctx context.Context, cmd *mysql.MySQLCommond) {
	mysqlOnce.Do(func() {
		//mysqlCnt = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "mysql_count"}, []string{"cmd", "caller"})
		mysqlCntError = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "mysql_error"}, []string{"cmd", "caller"})
		mysqlLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: MetricsNamePrefix + "mysql_latency",
			Buckets: []float64{1, 2, 4, 16, 64, 256, 1024, 2048, 4096, 16384, 65392, 261568, 1046272, 2092544, 4185088, 16740352}},
			[]string{"cmd", "caller"},
		)
	})
	//mysqlCnt.WithLabelValues(strings.ToUpper(cmd.Cmd), cmd.Caller.Name()).Inc()
	if cmd.Err != nil {
		mysqlCntError.WithLabelValues(strings.ToUpper(cmd.Cmd), cmd.Caller.Name()).Inc()
	}
	mysqlLatency.WithLabelValues(strings.ToUpper(cmd.Cmd), cmd.Caller.Name()).Observe(float64(cmd.Elapsed) / float64(time.Microsecond))
}

func httpHook(ctx context.Context, request *httprequest.HttpRequest) {
	httpOnce.Do(func() {
		//httpCnt = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "http_count"}, []string{"host", "path"})
		httpCntError = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "http_error"}, []string{"host", "path", "error"})
		httpLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: MetricsNamePrefix + "http_latency",
			Buckets: []float64{1, 2, 4, 16, 64, 256, 1024, 2048, 4096, 16384, 65392, 261568, 1046272, 2092544, 4185088, 16740352}},
			[]string{"host", "path"},
		)
	})
	var host string
	var path string
	if request.URL != nil {
		host = request.URL.Host
		k, err := httpPathRegexp.Replace(request.URL.Path, "*", 0, -1)
		if err == nil {
			path = k
		}
	}
	//httpCnt.WithLabelValues(host, path).Inc()
	if request.Err != nil {
		httpCntError.WithLabelValues(host, path, request.Err.Error()).Inc()
	}
	httpLatency.WithLabelValues(host, path).Observe(float64(request.Elapsed) / float64(time.Microsecond))
}

func ginHook(ctx context.Context, c *gin.Context, elapsed time.Duration) {
	ginOnce.Do(func() {
		//ginCnt = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "gin_count"}, []string{"method", "path"})
		ginCntError = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "gin_error"}, []string{"method", "path", "error"})
		ginLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: MetricsNamePrefix + "gin_latency",
			Buckets: []float64{1, 2, 4, 16, 64, 256, 1024, 2048, 4096, 16384, 65392, 261568, 1046272, 2092544, 4185088, 16740352}},
			[]string{"method", "path"},
		)
	})
	if c.Writer.Status() == http.StatusNotFound || c.Writer.Status() == http.StatusBadGateway {
		return
	}
	var path string
	k, err := ginPathRegexp.Replace(c.Request.URL.Path, "*", 0, -1)
	if err == nil {
		path = k
	}
	//ginCnt.WithLabelValues(strings.ToUpper(c.Request.Method), path).Inc()
	if len(c.Errors) > 0 {
		ginCntError.WithLabelValues(strings.ToUpper(c.Request.Method), path, c.Errors[0].Error()).Inc()
	} else if c.Writer.Status() != http.StatusOK {
		ginCntError.WithLabelValues(strings.ToUpper(c.Request.Method), path, strconv.Itoa(c.Writer.Status())).Inc()
	}
	ginLatency.WithLabelValues(strings.ToUpper(c.Request.Method), path).Observe(float64(elapsed) / float64(time.Microsecond))
}

type ConnCount interface {
	ConnCount() (int, int)
	ClientCount() int
}

type gNetHook[ClientInfo any] struct {
	addr      string
	connCount ConnCount
}

func (h *gNetHook[ClientInfo]) init() {
	gnetOnce.Do(func() {
		gnetConningCount = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: MetricsNamePrefix + "gnet_conning_count"}, []string{"addr"})
		gnetClientingCount = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: MetricsNamePrefix + "gnet_clienting_count"}, []string{"addr"})
		gnetHandShakeingCount = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: MetricsNamePrefix + "gnet_handshakeing_count"}, []string{"addr"})
		gnetConnCloseReason = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "gnet_conn_close_reason"}, []string{"addr"})
		gnetConnCount = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "gnet_conn_count"}, []string{"addr"})
		gnetHandShakeCount = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "gnet_handshake_count"}, []string{"addr"})
		gnetDisConnCount = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "gnet_disconn_count"}, []string{"addr"})
		gnetSendSize = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "gnet_send_size"}, []string{"addr"})
		gnetRecvSize = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "gnet_recv_size"}, []string{"addr"})
	})
}

func (h *gNetHook[ClientInfo]) OnConnected(gc *gnetserver.GNetClient[ClientInfo]) {
	h.init()
	count, _ := h.connCount.ConnCount()
	gnetConningCount.WithLabelValues(h.addr).Set(float64(count))
	gnetConnCount.WithLabelValues(h.addr).Add(1)
}
func (h *gNetHook[ClientInfo]) OnWSHandShake(gc *gnetserver.GNetClient[ClientInfo]) {
	h.init()
	_, handshakecount := h.connCount.ConnCount()
	gnetHandShakeCount.WithLabelValues(h.addr).Add(1)
	gnetHandShakeingCount.WithLabelValues(h.addr).Set(float64(handshakecount))
}
func (h *gNetHook[ClientInfo]) OnDisConnect(gc *gnetserver.GNetClient[ClientInfo], removeClient bool, closeReason error) {
	h.init()
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
	h.init()
	gnetClientingCount.WithLabelValues(h.addr).Set(float64(h.connCount.ClientCount()))
}
func (h *gNetHook[ClientInfo]) OnRemoveClient(gc *gnetserver.GNetClient[ClientInfo]) {
	h.init()
	gnetClientingCount.WithLabelValues(h.addr).Set(float64(h.connCount.ClientCount()))
}
func (h *gNetHook[ClientInfo]) OnSend(gc *gnetserver.GNetClient[ClientInfo], len int) {
	h.init()
	gnetSendSize.WithLabelValues(h.addr).Add(float64(len))
}
func (h *gNetHook[ClientInfo]) OnRecv(gc *gnetserver.GNetClient[ClientInfo], len int) {
	h.init()
	gnetRecvSize.WithLabelValues(h.addr).Add(float64(len))
}

type tcpServerHook[ClientInfo any] struct {
	addr      string
	connCount ConnCount
}

func (h *tcpServerHook[ClientInfo]) init() {
	tcpServerOnce.Do(func() {
		tcpServerConningCount = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: MetricsNamePrefix + "tcpserver_conning_count"}, []string{"addr"})
		tcpServerClientingCount = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: MetricsNamePrefix + "tcpserver_clienting_count"}, []string{"addr"})
		tcpServerHandShakeingCount = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: MetricsNamePrefix + "tcpserver_handshakeing_count"}, []string{"addr"})
		tcpServerConnCloseReason = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "tcpserver_conn_close_reason"}, []string{"addr"})
		tcpServerConnCount = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "tcpserver_conn_count"}, []string{"addr"})
		tcpServerHandShakeCount = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "tcpserver_handshake_count"}, []string{"addr"})
		tcpServerDisConnCount = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "tcpserver_disconn_count"}, []string{"addr"})
		tcpServerSendSize = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "tcpserver_send_size"}, []string{"addr"})
		tcpServerRecvSize = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "tcpserver_recv_size"}, []string{"addr"})
	})
}

func (h *tcpServerHook[ClientInfo]) OnConnected(tc *tcpserver.TCPClient[ClientInfo]) {
	h.init()
	count, _ := h.connCount.ConnCount()
	tcpServerConningCount.WithLabelValues(h.addr).Set(float64(count))
	tcpServerConnCount.WithLabelValues(h.addr).Add(1)
}
func (h *tcpServerHook[ClientInfo]) OnWSHandShake(gc *tcpserver.TCPClient[ClientInfo]) {
	h.init()
	_, handshakecount := h.connCount.ConnCount()
	tcpServerHandShakeCount.WithLabelValues(h.addr).Add(1)
	tcpServerHandShakeingCount.WithLabelValues(h.addr).Set(float64(handshakecount))
}
func (h *tcpServerHook[ClientInfo]) OnDisConnect(tc *tcpserver.TCPClient[ClientInfo], removeClient bool, closeReason error) {
	h.init()
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
	h.init()
	tcpServerClientingCount.WithLabelValues(h.addr).Set(float64(h.connCount.ClientCount()))
}
func (h *tcpServerHook[ClientInfo]) OnRemoveClient(tc *tcpserver.TCPClient[ClientInfo]) {
	h.init()
	tcpServerClientingCount.WithLabelValues(h.addr).Set(float64(h.connCount.ClientCount()))
}
func (h *tcpServerHook[ClientInfo]) OnSend(tc *tcpserver.TCPClient[ClientInfo], len int) {
	tcpServerSendSize.WithLabelValues(h.addr).Add(float64(len))
}
func (h *tcpServerHook[ClientInfo]) OnRecv(tc *tcpserver.TCPClient[ClientInfo], len int) {
	h.init()
	tcpServerRecvSize.WithLabelValues(h.addr).Add(float64(len))
}

type tcpBackendHook[ServerInfo any] struct {
}

func (h *tcpBackendHook[ServerInfo]) init() {
	tcpBackendOnce.Do(func() {
		tcpBackendServer = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: MetricsNamePrefix + "tcpbackend_server"}, []string{"servicename", "serviceid"})
		tcpBackendConned = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: MetricsNamePrefix + "tcpbackend_conned"}, []string{"servicename", "serviceid"})
		tcpBackendSendSize = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "tcpbackend_send_size"}, []string{"servicename", "serviceid"})
		tcpBackendRecvSize = promauto.NewCounterVec(prometheus.CounterOpts{Name: MetricsNamePrefix + "tcpbackend_recv_size"}, []string{"servicename", "serviceid"})
	})
}

func (h *tcpBackendHook[ServerInfo]) OnAdd(ts *backend.TcpService[ServerInfo]) {
	h.init()
	tcpBackendServer.WithLabelValues(ts.ServiceName(), ts.ServiceId()).Set(1)
}
func (h *tcpBackendHook[ServerInfo]) OnRemove(ts *backend.TcpService[ServerInfo]) {
	h.init()
	tcpBackendServer.DeleteLabelValues(ts.ServiceName(), ts.ServiceId())
	tcpBackendConned.DeleteLabelValues(ts.ServiceName(), ts.ServiceId())
	tcpBackendSendSize.DeleteLabelValues(ts.ServiceName(), ts.ServiceId())
	tcpBackendRecvSize.DeleteLabelValues(ts.ServiceName(), ts.ServiceId())
}
func (h *tcpBackendHook[ServerInfo]) OnConnected(ts *backend.TcpService[ServerInfo]) {
	h.init()
	tcpBackendConned.WithLabelValues(ts.ServiceName(), ts.ServiceId()).Set(1)
}
func (h *tcpBackendHook[ServerInfo]) OnDisConnect(ts *backend.TcpService[ServerInfo]) {
	h.init()
	tcpBackendConned.DeleteLabelValues(ts.ServiceName(), ts.ServiceId())
}
func (h *tcpBackendHook[ServerInfo]) OnSend(ts *backend.TcpService[ServerInfo], len int) {
	h.init()
	tcpBackendSendSize.WithLabelValues(ts.ServiceName(), ts.ServiceId()).Add(float64(len))
}
func (h *tcpBackendHook[ServerInfo]) OnRecv(ts *backend.TcpService[ServerInfo], len int) {
	h.init()
	tcpBackendRecvSize.WithLabelValues(ts.ServiceName(), ts.ServiceId()).Add(float64(len))
}

type httpBackendHook[ServerInfo any] struct {
}

func (h *httpBackendHook[ServerInfo]) init() {
	httpBackendOnce.Do(func() {
		httpBackendServer = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: MetricsNamePrefix + "httpbackend_server"}, []string{"servicename", "serviceid"})
		httpBackendConned = promauto.NewGaugeVec(prometheus.GaugeOpts{Name: MetricsNamePrefix + "httpbackend_conned"}, []string{"servicename", "serviceid"})
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

func msgDispatchHook(ctx context.Context, msgid string, elapsed time.Duration) {
	msgDispatchOnce.Do(func() {
		msgDispatchLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{Name: MetricsNamePrefix + "msgdispatch_latency",
			Buckets: []float64{1, 2, 4, 16, 64, 256, 1024, 2048, 4096, 16384, 65392, 261568, 1046272, 2092544, 4185088, 16740352}},
			[]string{"msgid"},
		)
	})

	msgDispatchLatency.WithLabelValues(msgid).Observe(float64(elapsed) / float64(time.Microsecond))
}
