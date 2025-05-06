package mrcache

// https://github.com/yuwf/gobase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// 基础类
// 配置类接口需要初始化时设置好，运行时不可再修改
// 内部redis和mysql同步数据时有分布式锁保护，如果外层有业务锁可以配置不需要内部的锁
type Cache struct {
	// 运行时数据，结构表数据
	*TableStruct // 不可修改 不可接受数据 只是用来记录结构类型

	redis           *goredis.Redis
	mysql           *mysql.MySQL
	tableName       string   // 表名
	tableCount      int      // 拆表的个数 默认0 不用拆表
	tableIndex      int      // 第几个拆表，从0开始 如果tableCount>0 tableName+tableIndex 才是真正的表名
	condFields      []string // 固定的查询字段 查询出的结果要唯一，最好有索引(唯一索引最好)，可直接使用到覆盖索引
	condFieldsIndex []int    // condFields对应的索引
	condFieldsLog   string   // log时专用

	// CacheRows使用
	keyFields      []string // 查询结果中唯一的字段tag，用来做key，区分大小写
	keyFieldsIndex []int    // dataKeyField在tableInfo中的索引
	keyFieldsLog   string   // log时专用

	// 其他配置参数
	// 生成key时的hasgtag
	hashTagField    string // 如果表结构条件中有字段名等于该值，就用查询你条件中这个字段的值设置redis中hashtag
	hashTagFieldIdx int    // hashTagField在tableInfo中的索引

	// 新增数据的自增ID，自增时通过redis来做的，redis根据incrementKey通过HINCRBY命令获取增长ID，其中hash的field就是incrementTable
	incrementReids      *goredis.Redis // 存储自增的Reids对象 默认值和redis为同一个对象
	incrementField      string         // mysql中自增字段tag名 区分大小写
	incrementFieldIndex int            // 自增key在tableInfo中的索引

	// 缓存过期时间 单位秒 不设置默认为36h
	expire int

	// 设置key前后缀时， 设置时不需要添加 _ 下划线，程序判断不为空时自动添加前后下划线
	keyPrefix string // key的前缀 各个缓存模型有自己的默认值
	keySuffix string // key的后缀 一般用来版本控制 如v1 v2 ...

	queryCond TableConds // 查找数据总过滤条件

	lock         bool // 同步数据锁保护
	toMysqlAsync bool // 异步保存到mysql
}

func NewCache[T any](redis *goredis.Redis, mysql *mysql.MySQL, tableName string, tableCount, tableIndex int, condFields []string) (*Cache, error) {
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
		if !table.IsBaseType(idx) {
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
		expire:          Expire,
		lock:            true,
		toMysqlAsync:    true,
	}
	if tableCount > 1 {
		if tableIndex >= 0 {
			c.tableIndex = tableIndex % tableCount
		} else {
			c.tableIndex = 0
		}
		c.tableCount = tableCount
	} else {
		c.tableIndex = 0
		c.tableCount = 0
	}
	return c, nil
}

func (c *Cache) Redis() *goredis.Redis {
	return c.redis
}

func (c *Cache) MySQL() *mysql.MySQL {
	return c.mysql
}

// 配置redishashtag，tag必须在c.condFields存在
func (c *Cache) ConfigHashTag(hashTagField string) error {
	at := utils.IndexOf(c.condFields, hashTagField)
	if at == -1 {
		return fmt.Errorf("tag:%s not find in %s", hashTagField, c.condFieldsLog)
	}
	c.hashTagField = hashTagField
	c.hashTagFieldIdx = at
	return nil
}

// 总过滤条件
func (c *Cache) ConfigQueryCond(cond TableConds) error {
	c.queryCond = cond
	return nil
}

// 配置自增参数
func (c *Cache) ConfigIncrement(incrementReids *goredis.Redis, incrementField string) error {
	if incrementReids == nil {
		return errors.New("incrementReids is nil")
	}
	// 自增字段必须存在 且类型是int或者uint
	idx := c.FindIndexByTag(incrementField)
	if idx == -1 {
		return fmt.Errorf("tag:%s not find in %s", incrementField, c.T.String())
	}
	switch c.Fields[idx].Type.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		break
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		break
	default:
		return fmt.Errorf("tag:%s not int or uint", incrementField)
	}

	c.incrementReids = incrementReids
	c.incrementField = incrementField
	c.incrementFieldIndex = idx
	return nil
}

func (c *Cache) ConfigToMysqlAsync(async bool) error {
	c.toMysqlAsync = async
	return nil
}

// 配置生成key的前缀
func (c *Cache) ConfigKeyPrefix(prefix, suffix string) error {
	c.keyPrefix = prefix
	c.keySuffix = suffix
	return nil
}

func (c *Cache) ConfigExpire(expire int) error {
	c.expire = expire
	return nil
}

func (c *Cache) TableName() string {
	if c.tableCount == 0 {
		return c.tableName
	} else {
		return c.tableName + strconv.Itoa(c.tableIndex)
	}
}

func (c *Cache) logContext(ctx *context.Context, _err_ *error, fun func(l *zerolog.Event), ignore ...error) func() {
	*ctx = utils.CtxCaller(*ctx, 2)
	logOut := !utils.CtxHasNolog(*ctx)
	if logOut && DBNolog {
		*ctx = utils.CtxNolog(*ctx)
	}
	return func() {
		var l *zerolog.Event
		if *_err_ != nil && !utils.Contains(ignore, *_err_) {
			l = utils.LogCtx(log.Error(), *ctx)
		} else if logOut && zerolog.DebugLevel >= log.Logger.GetLevel() {
			l = utils.LogCtx(log.Debug(), *ctx)
		}
		if l != nil {
			fun(l.Str("table", c.TableName()))
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
	if !(elemType == actualType || (elemType.Kind() == reflect.Pointer && elemType.Elem() == actualType)) {
		err := fmt.Errorf("value type is invalid at tag:%s(%s), should be %s", c.Tags[at], actualType.String(), elemType.String())
		return err
	}
	return nil
}

func (c *Cache) checkFiledType(at int, actualType reflect.Type) error {
	elemType := c.Fields[at].Type
	if !(elemType == actualType || (elemType.Kind() == reflect.Pointer && elemType.Elem() == actualType)) {
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

// 检查keyValues是否合法
func (c *Cache) checkKeyValues(keyValues []interface{}) error {
	if len(keyValues) != len(c.keyFields) {
		return errors.New("keyValues size not match keyFields")
	}
	for i, v := range keyValues {
		// v必须有效 且类型要和T类型对应的字段类型一致
		at := c.keyFieldsIndex[i]
		err := c.checkFiledValue(at, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) checkCondFieldValues(condFieldValues map[string]interface{}) ([]string, []interface{}, error) {
	condFields := make([]string, 0, len(condFieldValues))
	condValues := make([]interface{}, 0, len(condFieldValues))
	for tag, v := range condFieldValues {
		condFields = append(condFields, tag)
		condValues = append(condValues, v)
	}

	for i := 0; i < len(condFields); i++ {
		tag := condFields[i]
		// 在c.condFields是否存在
		at := utils.IndexOf(c.condFields, tag)
		if at == -1 {
			err := fmt.Errorf("tag:%s not find in %s", tag, c.condFieldsLog)
			return condFields, condValues, err
		}
		v := condValues[i]
		elemType := c.Fields[at].Type
		if v == nil {
			err := fmt.Errorf("condValues value type is nil at tag:%s, should be %s", c.condFields[at], elemType.String())
			return condFields, condValues, err
		}
		actualType := reflect.TypeOf(v)
		if !(elemType == actualType || (elemType.Kind() == reflect.Pointer && elemType.Elem() == actualType)) {
			err := fmt.Errorf("condValues value type is invalid at tag:%s(%s), should be %s", c.condFields[at], actualType.String(), elemType.String())
			return condFields, condValues, err
		}
	}
	return condFields, condValues, nil
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

// 生成keyValuesStr，不检查keyValues是否符合keyFields，需要调用的地方保证参数
func (c *Cache) genKeyValuesStr(keyValues []interface{}) string {
	var flag strings.Builder
	for i, v := range keyValues {
		if i > 0 {
			flag.WriteByte(':')
		}
		flag.WriteString(c.fmtBaseType(v))
	}
	return flag.String()
}

// 判断condValues是否符合condFileds，并生成key
func (c *Cache) checkCondValuesGenKey(condValues []interface{}) (string, error) {
	err := c.checkCondValues(condValues)
	if err != nil {
		return "", err
	}
	return c.genCondValuesKey(condValues), nil
}

// 判断keyValues是否符合keyFileds,并生成keyValuesStr
func (c *Cache) checkKeyValuesGenStr(keyValues []interface{}) (string, error) {
	err := c.checkKeyValues(keyValues)
	if err != nil {
		return "", err
	}
	return c.genKeyValuesStr(keyValues), nil
}

// 判断从data中去的数据是否符合keyFileds,并生成keyValuesStr
func (c *Cache) checkKeyValuesGenStrByMap(data map[string]interface{}) (string, []interface{}, error) {
	var keyValues []interface{}
	for _, f := range c.keyFields {
		v, ok := data[f]
		if !ok {
			err := fmt.Errorf("not find %s field", f)
			return "", nil, err
		}
		keyValues = append(keyValues, v)
	}
	err := c.checkKeyValues(keyValues)
	if err != nil {
		return "", nil, err
	}
	return c.genKeyValuesStr(keyValues), keyValues, nil
}

// 检查结构数据 是否为c.Tags的一部分
// 可以是结构或者结构指针 data.tags名称需要和T一致，可以是T的一部分
// 如果合理 返回data的结构信息
func (c *Cache) checkStructData(data interface{}) (*utils.StructValue, error) {
	dataInfo, err := utils.GetStructInfoByTag(data, DBTag)
	if err != nil {
		return nil, err
	}
	// 结构中的字段必须都存在，且类型还要一致
	for i, tag := range dataInfo.Tags {
		at := c.FindIndexByTag(tag)
		if at == -1 {
			err := fmt.Errorf("tag:%s not find in %s", tag, c.T.String())
			return nil, err
		}
		err := c.checkFiledType(at, dataInfo.Fields[i].Type)
		if err != nil {
			return nil, err
		}
	}
	return dataInfo, nil
}

// 检查Map数据 是否为c.Tags的一部分
func (c *Cache) checkMapData(data map[string]interface{}) error {
	// 结构中的字段必须都存在，且类型还要一致
	for tag, v := range data {
		at := c.FindIndexByTag(tag)
		if at == -1 {
			err := fmt.Errorf("tag:%s not find in %s", tag, c.T.String())
			return err
		}
		vt := reflect.TypeOf(v)
		if v != nil { // 空set时表示删除
			err := c.checkFiledType(at, vt)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// mysql -> redis 的锁
// 等待式加锁 返回的fun!=nil才表示加锁成功
func (c *Cache) preLoadLock(ctx context.Context, key string) (func(), error) {
	if !c.lock {
		return func() {}, nil // 不需要锁，直接返回加锁成功
	}

	return c.redis.TryLockWait(utils.CtxNolog(ctx), key+"_lock_preLoads", time.Second*8)

	// 若key已经存在了，加锁失败
	// 这种不会存在加载的间隙，但会有两个问题 1：如果redis数据时错的 无法再次加载  2：针对CacheRows使用索引的方式，不能对索引加锁
	// return c.redis.KeyLockWait(utils.CtxNolog(ctx), key, key+"_lock_preLoad", time.Second*8)
}

// redis -> mysql 的锁
func (c *Cache) saveLock(ctx context.Context, key string) (func(), error) {
	if !c.lock {
		return func() {}, nil // 不需要锁，直接返回加锁成功
	}

	return c.redis.Lock(utils.CtxNolog(ctx), key+"_lock_save", time.Second*8)
}

// 往MySQL中添加一条数据，返回自增值，如果条件是=的，会设置为默认值
func (c *Cache) addToMySQL(ctx context.Context, condValues []interface{}, data map[string]interface{}) (int64, error) {
	var incrementId int64
	if len(c.incrementField) != 0 {
		// 如果结构中有自增字段，优先使用
		if v, ok := data[c.incrementField]; ok && v != nil {
			incrementId = c.int64Value(v)
		}
		if incrementId == 0 {
			// 获取自增id
			err := c.incrementReids.DoScript2(utils.CtxNolog(ctx), incrScript, []string{IncrementKey}, c.TableName(), c.tableCount, c.tableIndex).Bind(&incrementId)
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
	sqlStr.WriteString(c.TableName())
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
		// 自增ID冲突了 尝试获取最大的ID， 重新写入下
		if len(c.incrementField) != 0 && utils.IsMatch("*Error 1062**Duplicate*PRIMARY*", err.Error()) {
			var maxIncrement int64
			err2 := c.mysql.Get(utils.CtxNolog(ctx), &maxIncrement, "SELECT MAX("+c.incrementField+") FROM "+c.TableName())
			if err2 == nil {
				incrementId = maxIncrement + 1000
				if c.tableCount > 0 {
					mod := incrementId % int64(c.tableCount)
					if mod != int64(c.tableIndex) {
						incrementId = incrementId - mod + int64(c.tableIndex)
					}
				}
				_, err := c.mysql.Exec(ctx, sqlStr.String(), args...)
				if err == nil {
					c.incrementReids.Do(utils.CtxNolog(ctx), "HSET", IncrementKey, c.TableName(), incrementId) // 保存下最大的key
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
	sqlStr.WriteString(c.TableName())

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
	sqlStr.WriteString(c.TableName())

	cond = append(cond, c.queryCond...)
	if len(cond) > 0 {
		sqlStr.WriteString(" WHERE ")
	}
	args := cond.fmtCond(&sqlStr)

	t := reflect.New(T)
	err := c.mysql.Get(ctx, t.Interface(), sqlStr.String(), args...)

	if err == sql.ErrNoRows {
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
	sqlStr.WriteString(c.TableName())

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

func (c *Cache) saveToMySQL(ctx context.Context, cond TableConds, data map[string]interface{}, key string, call func(err error)) error {
	var sqlStr strings.Builder
	sqlStr.WriteString("UPDATE ")
	sqlStr.WriteString(c.TableName())
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
		if call != nil {
			call(nil)
		}
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
		sqlStr.WriteString(v.op + "?")
		args = append(args, v.value)
	}

	if c.toMysqlAsync {
		utils.Submit(func() {
			// 不能判断返回影响的行数，如果更新的值相等，影响的行数也是0
			_, err := c.mysql.Update(ctx, sqlStr.String(), args...)
			if err == sql.ErrNoRows {
				err = ErrNullData
			}
			if call != nil {
				call(err)
			}
		})
		return nil
	} else {
		// 不能判断返回影响的行数，如果更新的值相等，影响的行数也是0
		_, err := c.mysql.Update(ctx, sqlStr.String(), args...)
		if err == sql.ErrNoRows {
			err = ErrNullData
		}
		if call != nil {
			call(err)
		}
		return err
	}
}

func (c *Cache) saveIgnoreTag(tag string) bool {
	if tag == c.incrementField {
		return true // 忽略自增字段
	}
	if utils.Contains(c.condFields, tag) {
		return true // 忽略条件字段
	}
	if utils.Contains(c.keyFields, tag) {
		return true // 忽略条件字段
	}
	return false
}

// params参数放到过期时间后面
func (c *Cache) redisSetParam(param string, data map[string]interface{}) []interface{} {
	redisParams := make([]interface{}, 0, 2+len(c.Tags)*3)
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, param)
	for tag, v := range data {
		if c.saveIgnoreTag(tag) {
			continue
		}
		redisParams = append(redisParams, c.GetRedisTagByTag(tag)) // 真实填充的是redistag
		vfmt := goredis.ValueFmt(reflect.ValueOf(v))
		if vfmt == nil {
			redisParams = append(redisParams, "del") // 空数据 删除字段
			redisParams = append(redisParams, nil)
			continue
		}
		redisParams = append(redisParams, "set")
		redisParams = append(redisParams, vfmt)
	}
	return redisParams
}

// params参数放到过期时间后面
// tags 表示填充data时 按tags来填充
func (c *Cache) redisSetGetParam(param string, tags []string, data map[string]interface{}, numIncr bool) []interface{} {
	redisParams := make([]interface{}, 0, 2+len(c.Tags)*3)
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, param)
	for _, tag := range tags {
		redisParams = append(redisParams, c.GetRedisTagByTag(tag)) // 真实填充的是redistag
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
		vfmt := goredis.ValueFmt(reflect.ValueOf(v))
		if vfmt == nil {
			redisParams = append(redisParams, "del") // 空数据 删除字段
			redisParams = append(redisParams, nil)
			continue
		}
		if numIncr {
			switch reflect.ValueOf(vfmt).Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				fallthrough
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				redisParams = append(redisParams, "incr") // 数值都是增量
			case reflect.Float32, reflect.Float64:
				redisParams = append(redisParams, "fincr") // 数值都是增量
			default:
				redisParams = append(redisParams, "set") // 其他都是直接设置
			}
		} else {
			redisParams = append(redisParams, "set")
		}
		redisParams = append(redisParams, vfmt)
	}
	return redisParams
}
