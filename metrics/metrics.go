package metrics

// https://github.com/yuwf/gobase

import (
	"sync"

	"github.com/yuwf/gobase/backend"
	"github.com/yuwf/gobase/ginserver"
	"github.com/yuwf/gobase/gnetserver"
	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/httprequest"
	"github.com/yuwf/gobase/msger"
	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/tcpserver"
	"github.com/yuwf/gobase/utils"

	"github.com/dlclark/regexp2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 注册各种组件的hook，实现统计

// 指标名前缀，需要Reg之前设置，需要再下面的注册函数之前设置
var MetricsNamePrefix = ""

// 指标统一添加的tag，需要再下面的注册函数之前设置
var MetricsTag = make(map[string]string)

var ScrapeInterval = 15 // 一些定时的抓取间隔，单位秒

// 配置是否使用Histogram，否则使用Counter统计
var GinHistogram = false
var HTTPHistogram = false
var GoRedisHistogram = false
var MysqlHistogram = false
var MsgerHistogram = false
var TCPServerConn = false // 连接TCPServer每个实例的接受和发送数据

var (
	// prometheus的注册对象
	reg     *Register
	regOnce sync.Once

	// 链接关闭原因字符串过滤
	connCloseReasonRegexp []*regexp2.Regexp
)

func init() {
	var err error
	expr := []string{
		`\d+`, //数字替换
	}
	connCloseReasonRegexp = make([]*regexp2.Regexp, len(expr))
	for i, s := range expr {
		// 分割
		connCloseReasonRegexp[i], err = regexp2.Compile(s, regexp2.None)
		if err != nil {
			panic(err.Error())
		}
	}
}

type Register struct {
	promauto.Factory
}

func DefaultReg() *Register {
	regOnce.Do(func() {
		// 前缀和公共标签标签
		registerer := prometheus.DefaultRegisterer
		if len(MetricsNamePrefix) > 0 {
			registerer = prometheus.WrapRegistererWithPrefix(MetricsNamePrefix, registerer)
		}
		if len(MetricsTag) > 0 {
			registerer = prometheus.WrapRegistererWith(MetricsTag, registerer)
		}
		reg = &Register{Factory: promauto.With(registerer)}
	})
	return reg
}

// interval设置抓取间隔，单位秒，[1,60]
func SetScrapeInterval(interval int) {
	if interval < 1 {
		interval = 1
	} else if interval > 60 {
		interval = 60
	}
	ScrapeInterval = interval

	recursiveMutexCrontMetrics()
	mysqlCronMetrics()
}

// RecursiveMutex统计
func RegRecursiveMutex() {
	recursiveMutexHookOnce.Do(func() {
		utils.RegRecursiveMutexHook(recursiveMutexHook)
	})
}

// GoRedis统计
func RegGoRedis(redis *goredis.Redis) {
	if redis != nil {
		redis.RegHook(goredisHook)
	}
}

// MySQL统计
func RegMySQL(mysql *mysql.MySQL) {
	if mysql != nil {
		mysql.RegHook(mysqlHook)
	}
}

// Http调用统计
func RegHttp() {
	httprequest.RegHook(httpHook)
}

// GinServer统计
func RegGinServer(gin *ginserver.GinServer) {
	if gin != nil {
		gin.RegHook(ginHook)
	}
}

// GNetServer统计
func RegGNetServer[ClientId any, ClientInfo any](gnet *gnetserver.GNetServer[ClientId, ClientInfo]) {
	if gnet != nil {
		gnet.RegHook(&gNetHook[ClientInfo]{addr: gnet.Address, server: gnet})
	}
}

// TCPServer统计
func RegTCPServer[ClientId any, ClientInfo any](ts *tcpserver.TCPServer[ClientId, ClientInfo]) {
	if ts != nil {
		ts.RegHook(&tcpServerHook[ClientInfo]{addr: ts.Address, server: ts})
	}
}

// TCPBackend统计
func RegTCPBackend[ServiceInfo any](tb *backend.TcpBackend[ServiceInfo]) {
	if tb != nil {
		tb.RegHook(&tcpBackendHook[ServiceInfo]{})
	}
}

// HTTPBackend统计
func RegHttpBackend[ServerInfo any](hb *backend.HttpBackend[ServerInfo]) {
	if hb != nil {
		hb.RegHook(&httpBackendHook[ServerInfo]{})
	}
}

// MsgDispatch统计
func RegMsgDispatch(md *msger.MsgDispatch) {
	if md != nil {
		md.RegHook(msgDispatchHook)
	}
}
