package mysql

// https://github.com/yuwf/gobase

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/yuwf/gobase/utils"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const CtxKey_NoDuplicate = utils.CtxKey("_NoDuplicate_")

type Config struct {
	Source string `json:"source,omitempty"` //地址 username:password@tcp(ip:port)/database?charset=utf8
	// 如果Source为空 就用下面的配置
	Addr     string `json:"addr,omitempty"` // host:port
	UserName string `json:"username,omitempty"`
	Passwd   string `json:"passwd,omitempty"`
	DB       string `json:"db,omitempty"`
	Param    string `json:"param,omitempty"` // 链接参数 k1=v1&k2=v2, 默认添加charset=utf8

	MaxOpenConns int `json:"maxopenconns,omitempty"`
	MaxIdleConns int `json:"maxidleconns,omitempty"`
}

var defaultMySQL *MySQL

type MySQL struct {
	db *sqlx.DB

	// 执行命令时的回调 不使用锁，默认要求提前注册好 管道部分待完善
	hook []func(ctx context.Context, cmd *MySQLCommond)
}

// 事务
type MySQLTx struct {
	m  *MySQL
	tx *sql.Tx
}

type MySQLCommond struct {
	// 命令名和参数
	Cmd   string
	Query string
	Args  []interface{}

	// 执行结果
	Err     error
	Elapsed time.Duration
}

func DefaultMySQL() *MySQL {
	return defaultMySQL
}

func InitDefaultMySQL(conf *Config) (*MySQL, error) {
	var err error
	defaultMySQL, err = NewMySQL(conf)
	if err != nil {
		return nil, err
	}
	return defaultMySQL, nil
}

// NewMySQL ...
func NewMySQL(conf *Config) (*MySQL, error) {
	conf.Source = strings.TrimSpace(conf.Source)
	if len(conf.Source) == 0 {
		conf.Source = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8&%s", conf.UserName, conf.Passwd, conf.Addr, conf.DB, conf.Param)
	}
	db, err := sqlx.Connect("mysql", conf.Source)
	if err != nil {
		log.Error().Err(err).Str("source", conf.Source).Msg("MySQL Conn Fail")
		return nil, err
	}
	db.SetMaxOpenConns(conf.MaxOpenConns)
	db.SetMaxIdleConns(conf.MaxIdleConns)

	mysql := &MySQL{
		db: db,
	}
	log.Info().Str("source", conf.Source).Msg("MySQL Conn Success")

	return mysql, nil
}

// DB 暴露原始对象
func (m *MySQL) DB() *sqlx.DB {
	return m.db
}

func getCmd(query, def string) string {
	for i, r := range query {
		if unicode.IsSpace(r) { // 检查是否为空格或其他空白字符
			return strings.ToUpper(query[:i])
		}
	}
	return def
}

func (m *MySQL) RegHook(f func(ctx context.Context, cmd *MySQLCommond)) {
	m.hook = append(m.hook, f)
}

func (m *MySQL) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {

	ctx = utils.CtxCaller(ctx, 1)
	mysqlCmd := &MySQLCommond{
		Cmd:   getCmd(query, "Get"),
		Query: query,
		Args:  args,
	}

	entry := time.Now()
	mysqlCmd.Err = m.db.GetContext(ctx, dest, query, args...)
	mysqlCmd.Elapsed = time.Since(entry)

	if mysqlCmd.Err != nil && mysqlCmd.Err != sql.ErrNoRows {
		utils.LogCtx(log.Error(), ctx).Err(mysqlCmd.Err).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
			Str("query", query).Interface("args", args).
			Msg("MySQL " + mysqlCmd.Cmd + " Fail")
	} else {
		logOut := !utils.CtxHasNolog(ctx)
		if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
				Str("query", query).Interface("args", args).
				Interface("dest", dest).
				Msg("MySQL " + mysqlCmd.Cmd + " Success")
		}
	}
	// 回调
	func() {
		defer utils.HandlePanic()
		for _, f := range m.hook {
			f(ctx, mysqlCmd)
		}
	}()
	return mysqlCmd.Err
}

func (m *MySQL) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	ctx = utils.CtxCaller(ctx, 1)
	mysqlCmd := &MySQLCommond{
		Cmd:   getCmd(query, "Select"),
		Query: query,
		Args:  args,
	}

	entry := time.Now()
	mysqlCmd.Err = m.db.SelectContext(ctx, dest, query, args...)
	mysqlCmd.Elapsed = time.Since(entry)

	if mysqlCmd.Err != nil {
		utils.LogCtx(log.Error(), ctx).Err(mysqlCmd.Err).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
			Str("query", query).Interface("args", args).
			Msg("MySQL " + mysqlCmd.Cmd + " Fail")
	} else {
		logOut := !utils.CtxHasNolog(ctx)
		if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
				Str("query", query).Interface("args", args).
				Interface("dest", dest).
				Msg("MySQL " + mysqlCmd.Cmd + " Success")
		}
	}
	// 回调
	func() {
		defer utils.HandlePanic()
		for _, f := range m.hook {
			f(ctx, mysqlCmd)
		}
	}()
	return mysqlCmd.Err
}

func (m *MySQL) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	ctx = utils.CtxCaller(ctx, 1)
	mysqlCmd := &MySQLCommond{
		Cmd:   getCmd(query, "Exec"),
		Query: query,
		Args:  args,
	}

	entry := time.Now()
	var resp sql.Result
	resp, mysqlCmd.Err = m.db.ExecContext(ctx, query, args...)
	mysqlCmd.Elapsed = time.Since(entry)

	if mysqlCmd.Err != nil {
		// 如果是插入，并且是主键冲突，外部可能会做测试，如果设置了跳过报警
		if nop := ctx.Value(CtxKey_NoDuplicate); nop != nil && strings.EqualFold(mysqlCmd.Cmd, "INSERT") && utils.IsMatch("*Error 1062**Duplicate*PRIMARY*", mysqlCmd.Err.Error()) {
			// 主键冲突
		} else {
			utils.LogCtx(log.Error(), ctx).Err(mysqlCmd.Err).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
				Str("query", query).Interface("args", args).
				Msg("MySQL " + mysqlCmd.Cmd + " Fail")
		}
	} else {
		logOut := !utils.CtxHasNolog(ctx)
		if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
				Str("query", query).Interface("args", args).
				Msg("MySQL " + mysqlCmd.Cmd + " Success")
		}
	}
	// 回调
	func() {
		defer utils.HandlePanic()
		for _, f := range m.hook {
			f(ctx, mysqlCmd)
		}
	}()
	return resp, mysqlCmd.Err
}

func (m *MySQL) Update(ctx context.Context, query string, args ...interface{}) (int64, error) {
	ctx = utils.CtxCaller(ctx, 1)
	mysqlCmd := &MySQLCommond{
		Cmd:   getCmd(query, "Update"),
		Query: query,
		Args:  args,
	}

	entry := time.Now()
	var resp sql.Result
	resp, mysqlCmd.Err = m.db.ExecContext(ctx, query, args...)
	mysqlCmd.Elapsed = time.Since(entry)

	if mysqlCmd.Err != nil {
		utils.LogCtx(log.Error(), ctx).Err(mysqlCmd.Err).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
			Str("query", query).Interface("args", args).
			Msg("MySQL " + mysqlCmd.Cmd + " Fail")
	} else {
		logOut := !utils.CtxHasNolog(ctx)
		if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
				Str("query", query).Interface("args", args).
				Msg("MySQL " + mysqlCmd.Cmd + " Success")
		}
	}
	if mysqlCmd.Err != nil {
		return 0, mysqlCmd.Err
	}
	// 回调
	func() {
		defer utils.HandlePanic()
		for _, f := range m.hook {
			f(ctx, mysqlCmd)
		}
	}()
	return resp.RowsAffected()
}

func (m *MySQL) Begin(ctx context.Context) (*MySQLTx, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		utils.LogCtx(log.Error(), ctx).Err(err).Msg("MySQL Begin Fail")
	}
	return &MySQLTx{m: m, tx: tx}, err
}

// Close ...
func (m *MySQL) Close() error {
	return m.db.Close()
}

func (mt *MySQLTx) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	ctx = utils.CtxCaller(ctx, 1)
	mysqlCmd := &MySQLCommond{
		Cmd:   getCmd(query, "TxExec"),
		Query: query,
		Args:  args,
	}

	entry := time.Now()
	var resp sql.Result
	resp, mysqlCmd.Err = mt.tx.ExecContext(ctx, query, args...)
	mysqlCmd.Elapsed = time.Since(entry)

	if mysqlCmd.Err != nil {
		utils.LogCtx(log.Error(), ctx).Err(mysqlCmd.Err).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
			Str("query", query).Interface("args", args).
			Msg("MySQL " + mysqlCmd.Cmd + " Fail")
	} else {
		logOut := !utils.CtxHasNolog(ctx)
		if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
				Str("query", query).Interface("args", args).
				Msg("MySQL " + mysqlCmd.Cmd + " Success")
		}
	}
	// 回调
	func() {
		defer utils.HandlePanic()
		for _, f := range mt.m.hook {
			f(ctx, mysqlCmd)
		}
	}()
	return resp, mysqlCmd.Err
}

func (mt *MySQLTx) Commit(ctx context.Context) error {
	ctx = utils.CtxCaller(ctx, 1)
	mysqlCmd := &MySQLCommond{
		Cmd:   "TxCommit",
		Query: "Commit",
		Args:  nil,
	}

	entry := time.Now()
	mysqlCmd.Err = mt.tx.Commit()
	mysqlCmd.Elapsed = time.Since(entry)

	if mysqlCmd.Err != nil {
		utils.LogCtx(log.Error(), ctx).Err(mysqlCmd.Err).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
			Msg("MySQL " + mysqlCmd.Cmd + " Fail")
	} else {
		logOut := !utils.CtxHasNolog(ctx)
		if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
				Msg("MySQL " + mysqlCmd.Cmd + " Success")
		}
	}
	// 回调
	func() {
		defer utils.HandlePanic()
		for _, f := range mt.m.hook {
			f(ctx, mysqlCmd)
		}
	}()
	return mysqlCmd.Err
}

func (mt *MySQLTx) Rollback(ctx context.Context) error {
	ctx = utils.CtxCaller(ctx, 1)
	mysqlCmd := &MySQLCommond{
		Cmd:   "TxRollback",
		Query: "Rollback",
		Args:  nil,
	}

	entry := time.Now()
	mysqlCmd.Err = mt.tx.Rollback()
	mysqlCmd.Elapsed = time.Since(entry)

	if mysqlCmd.Err != nil {
		utils.LogCtx(log.Error(), ctx).Err(mysqlCmd.Err).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
			Msg("MySQL " + mysqlCmd.Cmd + " Fail")
	} else {
		logOut := !utils.CtxHasNolog(ctx)
		if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			utils.LogCtx(log.Debug(), ctx).Int32("elapsed", int32(mysqlCmd.Elapsed/time.Millisecond)).
				Msg("MySQL " + mysqlCmd.Cmd + " Success")
		}
	}
	// 回调
	func() {
		defer utils.HandlePanic()
		for _, f := range mt.m.hook {
			f(ctx, mysqlCmd)
		}
	}()
	return mysqlCmd.Err
}
