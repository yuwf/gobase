package metrics

// https://github.com/yuwf/gobase

import (
	"github.com/yuwf/gobase/backend"
	"github.com/yuwf/gobase/dispatch"
	"github.com/yuwf/gobase/ginserver"
	"github.com/yuwf/gobase/gnetserver"
	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/httprequest"
	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/redis"
	"github.com/yuwf/gobase/tcpserver"
	"github.com/yuwf/gobase/utils"
)

// 注册各种组件的hook，实现统计

// 指标名前缀，需要Reg之前设置
var MetricsNamePrefix = ""

// Redis统计
func RegRedis(redis *redis.Redis) {
	if redis != nil {
		redis.RegHook(redisHook)
	}
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
		gnet.RegHook(&gNetHook[ClientInfo]{addr: gnet.Address, connCount: gnet})
	}
}

// TCPServer统计
func RegTCPServer[ClientId any, ClientInfo any](ts *tcpserver.TCPServer[ClientId, ClientInfo]) {
	if ts != nil {
		ts.RegHook(&tcpServerHook[ClientInfo]{addr: ts.Address, connCount: ts})
	}
}

// TCPBackend统计
func RegTCPBackend[ServerInfo any](tb *backend.TcpBackend[ServerInfo]) {
	if tb != nil {
		tb.RegHook(&tcpBackendHook[ServerInfo]{})
	}
}

// HTTPBackend统计
func RegHttpBackend[ServerInfo any](hb *backend.HttpBackend[ServerInfo]) {
	if hb != nil {
		hb.RegHook(&httpBackendHook[ServerInfo]{})
	}
}

// MsgDispatch统计
func RegMsgDispatch[Mr utils.RecvMsger, Cr any](md *dispatch.MsgDispatch[Mr, Cr]) {
	if md != nil {
		md.RegHook(msgDispatchHook)
	}
}
