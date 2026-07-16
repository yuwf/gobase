package mrcache

// https://github.com/yuwf/gobase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type CacheOptions struct {
	// 生成key时的hasgtag tag必须在c.condFields存在
	HashTagField string `json:"hashTagField"`

	// 自增字段配置
	IncrementField string `json:"incrementField"` // mysql中自增字段tag名 区分大小写
	TableCount     int    `json:"tableCount"`     // 拆表的个数 默认0 不用拆表，自增字段会根据拆表信息来生成不同的自增id
	TableIndex     int    `json:"tableIndex"`     // 第几个拆表，从0开始

	// 设置key前后缀时， 设置时不需要添加 _ 下划线，程序判断不为空时自动添加前后下划线
	KeyPrefix string `json:"keyPrefix"` // key的前缀 各个缓存模型有自己的默认值
	KeySuffix string `json:"keySuffix"` // key的后缀 一般用来版本控制 如v1 v2 ...

	// 缓存过期时间 单位秒 不设置默认为36h
	Expire int `json:"expire"`

	AsyncToMysql  bool `json:"asyncToMysql"`  // 异步保存到mysql 默认false
	AsyncMaxCount int  `json:"asyncMaxCount"` // 异步保存到mysql的最大并发数 默认10
	AsyncTimeout  int  `json:"asyncTimeout"`  // 异步保存到mysql的超时时间 默认60秒

	QueryCond TableConds `json:"queryCond"` // 查找数据总过滤条件
}

// 基础类
// 配置类接口需要初始化时设置好，运行时不可再修改
// 内部redis和mysql同步数据时有分布式锁保护，如果外层有业务锁可以配置不需要内部的锁
type Cache struct {
	// 运行时数据，结构表数据
	*TableStruct // 不可修改 不可接受数据 只是用来记录结构类型

	redis           *goredis.Redis
	mysql           *mysql.MySQL
	tableName       string   // 表名
	condFields      []string // 固定的查询字段 一定要有索引
	condFieldsIndex []int    // condFields对应的索引
	condFieldsLog   string   // log时专用

	// options配置
	hashTagField    string
	hashTagFieldIdx int // hashTagField在tableInfo中的索引

	// 自增字段配置
	incrementField      string
	tableCount          int
	tableIndex          int
	incrementFieldIndex int   // 自增key在tableInfo中的索引
	incrementMaxInit    int64 // 初始化时读取的表的最大自增值，如果出现了自增冲突，重读取下

	expire int

	keyPrefix string
	keySuffix string

	queryCond TableConds // 查找数据总过滤条件

	asyncToMysql       bool   // 异步保存到mysql
	asyncMaxCount      int    // 异步保存到mysql的最大并发数 默认10
	asyncTimeout       int    // 异步保存到mysql的超时时间 默认60秒
	dirtyKey           string // 脏数据key
	dirtyKeyProcessing string // 脏数据正在处理列表key
	dirtyKeyLastCheck  string // 脏数据上次检查时间key

	msgHeadLog string // log时专用 日志头 样式 "CacheRow TableName"
}

func NewCache[T any](redis *goredis.Redis, mysql *mysql.MySQL, tableName string, condFields []string, opts *CacheOptions) (*Cache, error) {
	table, err := GetTableStruct[T]()
	if err != nil {
		return nil, err
	}
	// 检查条件字段是否合理
	if len(condFields) == 0 {
		return nil, fmt.Errorf("condFields can not empty in %s", table.T.String())
	}
	var condFields_ []string
	var condFieldsIndex []int
	for _, f := range condFields {
		idx := table.FindIndexByTag(f)
		if idx == -1 {
			return nil, fmt.Errorf("tag:%s not find in %s", f, table.T.String())
		}
		// 条件只能是基本的数据int 和 string 类型
		if !table.IsBaseType(table.Fields[idx].Type) {
			err := fmt.Errorf("tag:%s(%s) type error", f, table.T.String())
			return nil, err
		}
		condFields_ = append(condFields_, f)
		condFieldsIndex = append(condFieldsIndex, idx)
	}

	c := &Cache{
		TableStruct:     table,
		redis:           redis,
		mysql:           mysql,
		tableName:       tableName,
		condFields:      condFields_,
		condFieldsIndex: condFieldsIndex,
		condFieldsLog:   "[" + strings.Join(condFields_, ",") + "]",
		hashTagField:    opts.HashTagField,
		incrementField:  opts.IncrementField,
		tableCount:      opts.TableCount,
		tableIndex:      opts.TableIndex,
		expire:          opts.Expire,
		keyPrefix:       opts.KeyPrefix,
		keySuffix:       opts.KeySuffix,
		queryCond:       opts.QueryCond,
		asyncToMysql:    opts.AsyncToMysql,
		asyncMaxCount:   opts.AsyncMaxCount,
		asyncTimeout:    opts.AsyncTimeout,
	}

	// 验证opts是否合理

	// hashTagField必须在condFields中
	if c.hashTagField != "" {
		c.hashTagFieldIdx = utils.IndexOf(condFields_, c.hashTagField)
		if c.hashTagFieldIdx == -1 {
			return nil, fmt.Errorf("tag:%s not find in %s", c.hashTagField, c.condFieldsLog)
		}
	}
	// tableCount校验
	if c.tableCount > 1 {
		if c.tableIndex >= 0 {
			c.tableIndex = c.tableIndex % c.tableCount
		} else {
			c.tableIndex = 0
		}
	} else {
		c.tableIndex = 0
		c.tableCount = 0
	}

	// 自增字段必须存在 且类型是int或者uint
	if len(c.incrementField) != 0 {
		c.incrementFieldIndex = c.FindIndexByTag(c.incrementField)
		if c.incrementFieldIndex == -1 {
			return nil, fmt.Errorf("tag:%s not find in %s", c.incrementField, c.T.String())
		}
		switch c.Fields[c.incrementFieldIndex].Type.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			break
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			break
		default:
			return nil, fmt.Errorf("tag:%s not int or uint", c.incrementField)
		}
	}

	if c.expire == 0 {
		c.expire = Expire
	}

	if c.asyncMaxCount <= 0 {
		c.asyncMaxCount = 10
	}
	if c.asyncTimeout <= 0 {
		c.asyncTimeout = 60
	}
	// 脏数据列表的key
	if len(c.keyPrefix) > 0 {
		c.dirtyKey += c.keyPrefix + "_"
	}
	c.dirtyKey += "{" + c.tableName + "}_dirty" // 添加上相同的hashkey
	c.dirtyKeyProcessing = c.dirtyKey + "_processing"
	c.dirtyKeyLastCheck = c.dirtyKey + "_lastcheck"
	if len(c.keySuffix) > 0 {
		c.dirtyKey += "_" + c.keySuffix
		c.dirtyKeyProcessing += "_" + c.keySuffix
		c.dirtyKeyLastCheck += "_" + c.keySuffix
	}

	if c.asyncToMysql {
		addAsyncCache(c)
	}

	return c, nil
}

func (c *Cache) Redis() *goredis.Redis {
	return c.redis
}

func (c *Cache) MySQL() *mysql.MySQL {
	return c.mysql
}

func (c *Cache) TableName() string {
	return c.tableName
}

// 删除脏数据列表，删除后未需要异步同步的数据无法同步到mysql
func (c *Cache) DelDirtyKey(ctx context.Context) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Err(_err_).Msgf("%s DelCache", c.msgHeadLog)
	})()

	keys := []string{c.dirtyKey, c.dirtyKeyProcessing, c.dirtyKeyLastCheck}
	cmd := c.redis.Del(ctx, keys...) // 删缓存
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

func (c *Cache) logContext(ctx *context.Context, _err_ *error, fun func(l *zerolog.Event), ignore ...error) func() {
	logOut := !utils.CtxHasNolog(*ctx)
	if logOut && DBNolog {
		*ctx = utils.CtxSetNolog(*ctx)
	}
	return func() {
		var l *zerolog.Event
		if *_err_ != nil && !utils.Contains(ignore, *_err_) {
			l = utils.LogCtx(log.Error(), *ctx)
		} else if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			l = utils.LogCtx(log.Debug(), *ctx)
		}
		if l != nil {
			fun(l)
		}
	}
}

func (c *Cache) checkFiledValue(at int, v interface{}) error {
	elemType := c.Fields[at].Type
	if v == nil {
		err := fmt.Errorf("value type is nil at tag:%s, should be %s", c.Tags[at], elemType.String())
		return err
	}
	actualType := reflect.TypeOf(v)
	if !(elemType == actualType || (elemType.Kind() == reflect.Pointer && elemType.Elem() == actualType) || (actualType.Kind() == reflect.Pointer && actualType.Elem() == elemType)) {
		err := fmt.Errorf("value type is invalid at tag:%s(%s), should be %s", c.Tags[at], actualType.String(), elemType.String())
		return err
	}
	return nil
}

func (c *Cache) checkFiledType(at int, actualType reflect.Type) error {
	elemType := c.Fields[at].Type
	if !(elemType == actualType || (elemType.Kind() == reflect.Pointer && elemType.Elem() == actualType) || (actualType.Kind() == reflect.Pointer && actualType.Elem() == elemType)) {
		err := fmt.Errorf("value type is invalid at tag:%s(%s), should be %s", c.Tags[at], actualType.String(), elemType.String())
		return err
	}
	return nil
}

// 检查条件值是否合法
func (c *Cache) checkCondValues(condValues []interface{}) error {
	if len(condValues) != len(c.condFields) {
		return errors.New("condValues size not match condFields")
	}
	for i, v := range condValues {
		// v必须有效 且类型要和T类型对应的字段类型一致
		at := c.condFieldsIndex[i]
		err := c.checkFiledValue(at, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) checkJsonArrayFieldValues(fieldValues map[string][]string) error {
	for tag := range fieldValues {
		// 字段名是否存在
		at := c.FindIndexByTag(tag)
		if at == -1 {
			err := fmt.Errorf("tag:%s not find in %s", tag, c.T.String())
			return err
		}
		// 判断是否为slice,并且slice内部是基础类型
		if c.Fields[at].Type.Kind() != reflect.Slice {
			err := fmt.Errorf("type is not slice, tag:%s(%s)", tag, c.T.String())
			return err
		}
		// 元素类型是否一致
		elemType := c.Fields[at].Type.Elem()
		if !(elemType.Kind() == reflect.String || (elemType.Kind() == reflect.Pointer && elemType.Elem().Kind() == reflect.String)) {
			err := fmt.Errorf("elem type is not string at tag:%s(%s)", tag, c.T.String())
			return err
		}
	}
	return nil
}

// 生成key，不检查condValues是否符合condFields，需要调用的地方保证参数
// key的命名: [keyPrefix_]表名[:dataValueField][_keySuffix]_{condValue1}_condValue2  []内的是根据配置来生成
// CacheColumn缓存会设置dataValueField，key中添加上:dataValueField
// hashTagField == condField时 condValue1会添加上{}
func (c *Cache) genCondValuesKey(condValues []interface{}) string {
	var key strings.Builder
	key.Grow(len(c.keyPrefix) + 1 + len(c.tableName) + 1 + 1 + len(c.keySuffix) + len(condValues)*20) // 预估一个大小
	if len(c.keyPrefix) > 0 {
		key.WriteString(c.keyPrefix + "_")
	}
	key.WriteString(c.tableName)
	if len(c.keySuffix) > 0 {
		key.WriteString("_" + c.keySuffix)
	}
	for i, v := range condValues {
		if c.condFields[i] == c.hashTagField {
			key.WriteString("_{" + c.fmtBaseType(v) + "}")
		} else {
			key.WriteString("_" + c.fmtBaseType(v))
		}
	}
	return key.String()
}

// 判断condValues是否符合condFileds，并生成key
func (c *Cache) checkCondValuesGenKey(condValues []interface{}) (string, error) {
	err := c.checkCondValues(condValues)
	if err != nil {
		return "", err
	}
	return c.genCondValuesKey(condValues), nil
}

// 检查结构数据 是否为c.Tags的一部分
// 可以是结构或者结构指针 data.tags名称需要和T一致，可以是T的一部分
// 如果合理 返回data的结构信息
func (c *Cache) checkStructData(data interface{}) (*utils.StructValue, *ModifyData, error) {
	dataInfo, err := utils.GetStructInfoByTag(data, DBTag)
	if err != nil {
		return nil, nil, err
	}
	modifyData, err := c.checkMapData(dataInfo.TagElemsInterface())
	if err != nil {
		return nil, nil, err
	}
	return dataInfo, modifyData, nil
}

// 检查Map数据 是否为c.Tags的一部分
func (c *Cache) checkMapData(data map[string]interface{}) (*ModifyData, error) {
	// 结构中的字段必须都存在，且类型还要一致
	modifyData := &ModifyData{
		st:    c.StructType,
		data:  make(map[string]interface{}, len(c.Tags)),
		index: make([]int, 0, len(c.Tags)),
		tags:  make([]string, 0, len(c.Tags)),
	}
	for i, tag := range c.Tags {
		if v, ok := data[tag]; ok {
			vt := reflect.TypeOf(v)
			if v != nil { // 空set时表示删除
				err := c.checkFiledType(i, vt)
				if err != nil {
					return nil, err
				}
			}
			modifyData.data[tag] = v
			modifyData.index = append(modifyData.index, i)
			modifyData.tags = append(modifyData.tags, tag)
		}
	}
	if len(modifyData.data) != len(data) {
		var notFoundTags []string
		for tag := range data {
			if _, ok := modifyData.data[tag]; !ok {
				notFoundTags = append(notFoundTags, tag)
			}
		}
		return nil, fmt.Errorf("tag:%s no tag found in %s", strings.Join(notFoundTags, ","), c.T.String())
	}
	return modifyData, nil
}

// redis -> mysql 的锁，同时时使用
func (c *Cache) saveLock(ctx context.Context, key string) (func(), error) {
	return c.redis.Lock(utils.CtxSetNolog(ctx), key+"_lock_save", time.Second*8)
}

// 获取自增值
func (c *Cache) getIncrement(ctx context.Context) (int64, error) {
	if c.incrementMaxInit == 0 {
		// 从mysql中读取最大自增值，保存到Redis中
		var maxIncrement int64
		err := c.mysql.Get(utils.CtxSetNolog(ctx), &maxIncrement, "SELECT IFNULL(MAX("+c.incrementField+"), 0) FROM "+c.tableName)
		if err != nil {
			return 0, err
		}
		c.incrementMaxInit = maxIncrement
		c.redis.HSet(utils.CtxSetNolog(ctx), IncrementKey, c.tableName, maxIncrement)
	}
	// 获取自增id
	var incrementId int64
	err := c.redis.Script(utils.CtxSetNolog(ctx), incrScript, []string{IncrementKey}, c.tableName, c.tableCount, c.tableIndex).Bind(&incrementId)
	if err != nil {
		return 0, err
	}
	return incrementId, nil
}

// 往MySQL中添加一条数据，返回自增值，如果条件是=的，会设置为默认值
func (c *Cache) addToMySQL(ctx context.Context, condValues []interface{}, keyValues []interface{}, data map[string]interface{}) (int64, error) {
	var incrementId int64
	if len(c.incrementField) != 0 {
		// 如果结构中有自增字段，优先使用
		if v, ok := data[c.incrementField]; ok && v != nil {
			incrementId = c.int64Value(v)
		}
		if incrementId == 0 {
			var err error
			incrementId, err = c.getIncrement(ctx)
			if err != nil {
				return 0, err
			}
		}
	}

	fields := make([]string, 0, len(c.Tags))
	args := make([]interface{}, 0, len(c.Tags))
	for i, tag := range c.Tags {
		if len(c.incrementField) != 0 && tag == c.incrementField {
			fields = append(fields, tag)
			args = append(args, &incrementId) // 这里填充地址，下面如果自增主键冲突了，会再次修改，mysql内部支持*int的转化操作，Redis不会
			continue
		}
		// 从条件变量中查找
		condi := utils.IndexOf(c.condFieldsIndex, i)
		if condi != -1 {
			fields = append(fields, tag)
			args = append(args, condValues[condi])
			continue
		}
		// 从数据中查找
		if v, ok := data[tag]; ok && v != nil {
			// 如果是Time类型，且没有填充，忽略
			if t, tok := v.(time.Time); tok && t.IsZero() {
				continue
			}
			fields = append(fields, tag)
			args = append(args, v)
		}
	}

	var sqlStr strings.Builder
	sqlStr.WriteString("INSERT INTO ")
	sqlStr.WriteString(c.tableName)
	sqlStr.WriteString(" (")
	for i, tag := range fields {
		if i > 0 {
			sqlStr.WriteString(",")
		}
		sqlStr.WriteString(tag)
	}
	sqlStr.WriteString(") VALUES(")
	sqlStr.WriteString(strings.Repeat("?,", len(fields)-1) + "?")
	sqlStr.WriteString(")")

	_, err := c.mysql.Exec(context.WithValue(ctx, mysql.CtxKey_NoDuplicate, 1), sqlStr.String(), args...)

	if err != nil {
		// 自增ID冲突了 重新获取下最大的ID
		if len(c.incrementField) != 0 && utils.IsMatch("*Error 1062**Duplicate*PRIMARY*", err.Error()) {
			c.incrementMaxInit = 0 // 这里要还原初始值
			var err error
			incrementId, err = c.getIncrement(ctx)
			if err == nil {
				_, err := c.mysql.Exec(ctx, sqlStr.String(), args...)
				if err == nil {
					return incrementId, nil
				}
			}
		}
		return 0, err
	}
	return incrementId, nil
}

// 删除MYSQL数据
func (c *Cache) delToMySQL(ctx context.Context, cond TableConds) error {
	var sqlStr strings.Builder
	sqlStr.WriteString("DELETE FROM ")
	sqlStr.WriteString(c.tableName)

	cond = append(cond, c.queryCond...)
	if len(cond) > 0 {
		sqlStr.WriteString(" WHERE ")
	}
	args := cond.fmtCond(&sqlStr)

	_, err := c.mysql.Exec(ctx, sqlStr.String(), args...)

	if err != nil {
		return err
	}
	return nil
}

// 读取mysql数据 返回的是 *T 会返回空错误
// fields表示读取的字段名，内部为string类型
func (c *Cache) getFromMySQL(ctx context.Context, T reflect.Type, fields []string, cond TableConds) (interface{}, error) {
	var sqlStr strings.Builder
	sqlStr.WriteString("SELECT ")

	for i, tag := range fields {
		if i > 0 {
			sqlStr.WriteString(",")
		}
		sqlStr.WriteString(tag)
	}
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(c.tableName)

	cond = append(cond, c.queryCond...)
	if len(cond) > 0 {
		sqlStr.WriteString(" WHERE ")
	}
	args := cond.fmtCond(&sqlStr)

	t := reflect.New(T)
	err := c.mysql.Get(ctx, t.Interface(), sqlStr.String(), args...)

	if err == sql.ErrNoRows { // mysql的Get会返回sql.ErrNoRows， 其他方法不会
		return nil, ErrNullData
	}
	if err != nil {
		return nil, err
	}
	return t.Interface(), nil
}

// 读取mysql数据 返回的是 []*T  不会返回空错误
// fields表示读取的字段名，内部为string类型
func (c *Cache) getsFromMySQL(ctx context.Context, T reflect.Type, fields []string, cond TableConds) (interface{}, error) {
	var sqlStr strings.Builder
	sqlStr.WriteString("SELECT ")

	for i, tag := range fields {
		if i > 0 {
			sqlStr.WriteString(",")
		}
		sqlStr.WriteString(tag)
	}
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(c.tableName)

	cond = append(cond, c.queryCond...)
	if len(cond) > 0 {
		sqlStr.WriteString(" WHERE ")
	}
	args := cond.fmtCond(&sqlStr)

	t := reflect.New(reflect.SliceOf(reflect.PtrTo(T)))
	err := c.mysql.Select(ctx, t.Interface(), sqlStr.String(), args...)

	// select不会返回ErrNoRows
	//if err == sql.ErrNoRows {
	//	return nil, ErrNullData
	//}
	if err != nil {
		return nil, err
	}
	return t.Elem().Interface(), nil
}

// 根据条件获取查询值
func (c *Cache) getCondValuesFromMySQL(ctx context.Context, cond TableConds) ([][]interface{}, error) {
	var sqlStr strings.Builder
	sqlStr.WriteString("SELECT ")

	for i, tag := range c.condFields {
		if i > 0 {
			sqlStr.WriteString(",")
		}
		sqlStr.WriteString(tag)
	}
	sqlStr.WriteString(" FROM ")
	sqlStr.WriteString(c.tableName)

	cond = append(cond, c.queryCond...)
	if len(cond) > 0 {
		sqlStr.WriteString(" WHERE ")
	}
	args := cond.fmtCond(&sqlStr)

	rows, err := c.mysql.Query(ctx, sqlStr.String(), args...)
	if err != nil {
		return nil, err
	}

	rst := make([][]interface{}, 0)
	for rows.Next() {
		ptrs := make([]interface{}, len(c.condFields))
		// 为这一行创建扫描目标
		for i := range c.condFields {
			ptrs[i] = reflect.New(c.Fields[c.condFieldsIndex[i]].Type).Interface()
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		// 取出这一行的值
		row := make([]interface{}, len(c.condFields))
		for i := range ptrs {
			row[i] = reflect.ValueOf(ptrs[i]).Elem().Interface()
		}
		rst = append(rst, row)
	}
	return rst, nil
}

func (c *Cache) saveToMySQL(ctx context.Context, cond TableConds, data map[string]interface{}) error {
	var sqlStr strings.Builder
	sqlStr.WriteString("UPDATE ")
	sqlStr.WriteString(c.tableName)
	sqlStr.WriteString(" SET ")

	args := make([]interface{}, 0, len(cond)+len(data))
	num := 0
	for tag, v := range data {
		if c.saveIgnoreTag(tag) {
			continue
		}
		// 如果是Time类型，且没有填充，忽略
		if t, tok := v.(time.Time); tok && t.IsZero() {
			continue
		}

		if num > 0 {
			sqlStr.WriteString(",")
		}
		num++
		sqlStr.WriteString(tag)
		sqlStr.WriteString("=?")
		if v != nil {
			args = append(args, v)
		} else {
			at := c.FindIndexByTag(tag)
			args = append(args, reflect.Zero(c.Fields[at].Type).Interface())
		}
	}
	if num == 0 {
		return nil // 没啥可更新的
	}
	sqlStr.WriteString(" WHERE ")

	for i, v := range cond {
		if i > 0 {
			if len(cond[i-1].link) > 0 {
				sqlStr.WriteString(" " + cond[i-1].link + " ")
			} else {
				sqlStr.WriteString(" AND ")
			}
		}
		sqlStr.WriteString(v.field)
		sqlStr.WriteString(v.op)
		args = append(args, v.values...)
	}

	// 不能判断返回影响的行数，如果更新的值相等，影响的行数也是0
	_, err := c.mysql.Update(ctx, sqlStr.String(), args...)
	return err
}

// mysql的JSON_SEARCH 不支持数字类型的查找，这里明确添加的类型必须是string
func (c *Cache) jsonArrayToMySQL(ctx context.Context, cond TableConds, add, del map[string][]string) error {
	var sqlStr strings.Builder
	sqlStr.WriteString("UPDATE ")
	sqlStr.WriteString(c.tableName)
	sqlStr.WriteString(" SET ")

	args := make([]interface{}, 0, 0)
	num := 0
	for tag, values := range add {
		if len(values) == 0 {
			continue
		}
		if c.saveIgnoreTag(tag) {
			continue
		}

		if num > 0 {
			sqlStr.WriteString(",")
		}
		num++

		sqlStr.WriteString(tag + "=")
		sqlStr.WriteString("JSON_ARRAY_APPEND(")
		sqlStr.WriteString("IF(JSON_TYPE(" + tag + ") = 'ARRAY', " + tag + ", JSON_ARRAY())")
		for _, v := range values {
			sqlStr.WriteString(",'$',?")
			args = append(args, v)
		}
		sqlStr.WriteString(")")
	}
	for tag, values := range del {
		if len(values) == 0 {
			continue
		}
		if c.saveIgnoreTag(tag) {
			continue
		}

		// 每一个字段每一个删除的值成一个case，如果合成一个，删除会出错
		for _, v := range values {
			if num > 0 {
				sqlStr.WriteString(",")
			}
			num++
			sqlStr.WriteString(tag)
			sqlStr.WriteString("=")
			sqlStr.WriteString("CASE WHEN JSON_SEARCH(" + tag + ", 'one', ?) IS NOT NULL THEN")
			sqlStr.WriteString(" JSON_REMOVE(" + tag + ", JSON_UNQUOTE(JSON_SEARCH(" + tag + ", 'one', ?)))")
			sqlStr.WriteString(" ELSE " + tag + " END")

			args = append(args, v)
			args = append(args, v)
		}

	}

	if num == 0 {
		return nil // 没啥可更新的
	}
	sqlStr.WriteString(" WHERE ")

	for i, v := range cond {
		if i > 0 {
			if len(cond[i-1].link) > 0 {
				sqlStr.WriteString(" " + cond[i-1].link + " ")
			} else {
				sqlStr.WriteString(" AND ")
			}
		}
		sqlStr.WriteString(v.field)
		sqlStr.WriteString(v.op)
		args = append(args, v.values...)
	}

	// 不能判断返回影响的行数，如果更新的值相等，影响的行数也是0
	_, err := c.mysql.Update(ctx, sqlStr.String(), args...)
	return err
}

func (c *Cache) saveIgnoreTag(tag string) bool {
	if tag == c.incrementField {
		return true // 忽略自增字段
	}
	if utils.Contains(c.condFields, tag) {
		return true // 忽略条件字段
	}
	return false
}

// 适配rowGetScript的参数
func (c *Cache) redisGetParam() []interface{} {
	redisParams := make([]interface{}, 0, 1+len(c.Tags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, c.RedisTagsInterface()...)
	return redisParams
}

// 适配rowModifyScript的参数
// numIncr 表示是否是数值类型是否使用增量
func (c *Cache) redisModifyParam(data map[string]interface{}, numIncr bool, keyValuesStr string) []interface{} {
	redisParams := make([]interface{}, 0, 3+len(data)*3)
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, utils.If(c.asyncToMysql, 1, 0))
	redisParams = append(redisParams, keyValuesStr)
	for tag, v := range data {
		if c.saveIgnoreTag(tag) {
			continue
		}
		tagIndex := utils.IndexOf(c.Tags, tag)
		redisParams = append(redisParams, c.RedisTags[tagIndex]) // 真实填充的是redistag
		vfmt := goredis.ValueToRedisArg(reflect.ValueOf(v))
		op := utils.If(vfmt == nil, "del", "set") // 空数据 删除字段
		if numIncr {
			// 数值类型增量操作，如果vfmt是nil，就只读取，不修改
			switch c.Fields[tagIndex].Type.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				fallthrough
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				op = utils.If(vfmt == nil, "get", "incr")
			case reflect.Float32, reflect.Float64:
				op = utils.If(vfmt == nil, "get", "fincr")
			}
		}
		redisParams = append(redisParams, op)
		redisParams = append(redisParams, vfmt)
	}
	return redisParams
}

// 适配rowModifyGetScript的参数
// tags 在data中不存在是只读取，存在是修改后读取
// numIncr 表示是否是数值类型是否使用增量
func (c *Cache) redisModifyGetParam(tags []string, data map[string]interface{}, numIncr bool) []interface{} {
	redisParams := make([]interface{}, 0, 2+len(tags)*3)
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, utils.If(c.asyncToMysql, 1, 0))
	for _, tag := range tags {
		tagIndex := utils.IndexOf(c.Tags, tag)
		redisParams = append(redisParams, c.RedisTags[tagIndex]) // 真实填充的是redistag
		if c.saveIgnoreTag(tag) {
			redisParams = append(redisParams, "get") // 忽略的字段 只读取
			redisParams = append(redisParams, nil)
			continue
		}
		v, ok := data[tag]
		if !ok {
			redisParams = append(redisParams, "get") // 只读取
			redisParams = append(redisParams, nil)
			continue
		}
		vfmt := goredis.ValueToRedisArg(reflect.ValueOf(v))
		op := utils.If(vfmt == nil, "del", "set") // 空数据 删除字段
		if numIncr {
			// 数值类型增量操作，如果vfmt是nil，就只读取，不修改
			switch c.Fields[tagIndex].Type.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				fallthrough
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				op = utils.If(vfmt == nil, "get", "incr")
			case reflect.Float32, reflect.Float64:
				op = utils.If(vfmt == nil, "get", "fincr")
			}
		}
		redisParams = append(redisParams, op)
		redisParams = append(redisParams, vfmt)
	}
	return redisParams
}

// 适配rowJsonArrayModifyScript的参数
func (c *Cache) redisJsonArrayParam(add, del map[string][]string, duplicate bool) ([]string, []string, []interface{}) {
	num := 0
	for _, values := range add {
		num += 3 + len(values)
	}
	for _, values := range del {
		num += 3 + len(values)
	}
	redisParams := make([]interface{}, 0, 3+num)
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, utils.If(c.asyncToMysql, 1, 0))
	redisParams = append(redisParams, utils.If(duplicate, 1, 0))
	addFields := []string{}
	delFields := []string{}
	for tag, values := range add {
		if c.saveIgnoreTag(tag) {
			continue
		}
		if len(values) == 0 {
			continue
		}
		redisParams = append(redisParams, c.GetRedisTagByTag(tag)) // 真实填充的是redistag
		redisParams = append(redisParams, "add")
		redisParams = append(redisParams, len(values))
		for _, v := range values {
			redisParams = append(redisParams, v)
		}
		addFields = append(addFields, tag)
	}
	for tag, values := range del {
		if c.saveIgnoreTag(tag) {
			continue
		}
		if len(values) == 0 {
			continue
		}
		redisParams = append(redisParams, c.GetRedisTagByTag(tag)) // 真实填充的是redistag
		redisParams = append(redisParams, "del")
		redisParams = append(redisParams, len(values))
		for _, v := range values {
			redisParams = append(redisParams, v)
		}
		delFields = append(delFields, tag)
	}
	return addFields, delFields, redisParams
}
