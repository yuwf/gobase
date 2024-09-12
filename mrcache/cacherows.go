package mrcache

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/utils"
)

// T 为数据库结构类型，可重复多协程使用
// 使用场景：查询条件对应多个结果 redis使用多个key缓存这个结果，一条数据对应一个key
// 索引key：有一个总key存储所有的数据key，hash结构，datakeyvalue:datakey datakey命名：索引key名_dataKeyField对应的值
// 数据key：存储一条数据的key，dataKey对应的存储方式和cacherow一样
// 需要使用ConfigDataKeyField配置数据key对应的字段
// 为了效率，索引key一直缓存所有的数据key，但数据key可能在redis中不存在时，此时就重新加载
// 最好查询条件和dataKeyField做一个唯一索引
type CacheRows[T any] struct {
	*Cache
}

func NewCacheRows[T any](redis *goredis.Redis, mysql *mysql.MySQL, tableName string) *CacheRows[T] {
	table := GetTableStruct[T]()
	c := &CacheRows[T]{
		Cache: &Cache{
			redis:     redis,
			mysql:     mysql,
			tableName: tableName,
			expire:    Expire,
			tableInfo: table,
		},
	}
	// 默认配置第一个Key为自增
	if len(table.MySQLTags) > 0 {
		c.ConfigIncrement(redis, table.MySQLTags[0].(string), tableName)
	}
	// 默认配置第一个Key为数据key字段
	if len(table.MySQLTags) > 0 {
		c.ConfigDataKeyField(table.MySQLTags[0].(string))
	}
	return c
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheRows[T]) GetAllOC(ctx context.Context, condValue interface{}) ([]*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		err := errors.New("need config oneCondField")
		return nil, err
	}
	return c.GetAll(ctx, NewConds().Eq(c.oneCondField, condValue))
}

// 读取数据
// cond：查询条件变量
// 返回值：是T结构类型的指针列表
func (c *CacheRows[T]) GetAll(ctx context.Context, cond TableConds) ([]*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return nil, err
	}

	// 从Redis中读取
	redisParams := make([]interface{}, 0, 1+len(c.tableInfo.MySQLTags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, c.tableInfo.RedisTags...)

	dest, err := c.redisGetAll(ctx, key, redisParams)
	if err == nil {
		return dest, nil
	} else if err == ErrNullData {
		if GetPass(key) {
			return nil, ErrNullData
		}
		// 不处理执行下面的预加载
	} else {
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, err := c.preLoadAll(ctx, cond, key)
	if err == ErrNullData {
		return nil, err // 空数据直接返回
	}
	if err != nil {
		return nil, err
	}
	if preData != nil { // 执行了预加载
		return preData, nil
	}

	// 如果不是自己执行的预加载，这里重新读取下
	dest, err = c.redisGetAll(ctx, key, redisParams)
	if err == nil {
		return dest, nil
	} else if err == ErrNullData {
		return nil, ErrNullData
	} else {
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheRows[T]) GetOC(ctx context.Context, condValue interface{}, dataKeyValue interface{}) (*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, errors.New("need config oneCondField")
	}
	return c.Get(ctx, NewConds().Eq(c.oneCondField, condValue), dataKeyValue)
}

// 读取一条数据
// cond：查询条件变量
// 返回值：是T结构类型的指针列表
func (c *CacheRows[T]) Get(ctx context.Context, cond TableConds, dataKeyValue interface{}) (*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return nil, err
	}

	// 数据key的类型和值
	dataKeyVO := reflect.ValueOf(dataKeyValue)
	if !(c.tableInfo.ElemtsType[c.dataKeyFieldIndex] == dataKeyVO.Type() ||
		(c.tableInfo.ElemtsType[c.dataKeyFieldIndex].Kind() == reflect.Pointer && c.tableInfo.ElemtsType[c.dataKeyFieldIndex].Elem() == dataKeyVO.Type())) {
		err := fmt.Errorf("dataKeyValue:%s type err, should be %s", dataKeyVO.Type().String(), c.tableInfo.ElemtsType[c.dataKeyFieldIndex].String())
		return nil, err
	}
	dataKey := key + "_" + c.dataKeyValue(dataKeyVO)

	// 从Redis中读取
	redisParams := make([]interface{}, 0, 1+len(c.tableInfo.MySQLTags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, c.tableInfo.RedisTags...)

	dest := new(T)
	destInfo, _ := utils.GetStructInfoByTag(dest, DBTag) // 这里不用判断err了
	err = c.redis.DoScript2(ctx, rowsGetScript, []string{key, dataKey}, redisParams).BindValues(destInfo.Elemts)
	if err == nil {
		return dest, nil
	} else if goredis.IsNilError(err) {
		if GetPass(dataKey) {
			return nil, ErrNullData
		}
		// 不处理执行下面的预加载
	} else {
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, _, err := c.preLoad(ctx, cond, key, dataKeyValue, false, nil)
	if err == ErrNullData {
		return nil, err // 空数据直接返回
	}
	if err != nil {
		return nil, err
	}
	// 预加载的数据就是新增的
	if preData != nil {
		return preData, nil
	}

	// 如果不是自己执行的预加载，这里重新读取下
	dest = new(T) // 防止上面的数据有干扰
	destInfo, _ = utils.GetStructInfoByTag(dest, DBTag)
	err = c.redis.DoScript2(ctx, rowGetScript, []string{key, dataKey}, redisParams).BindValues(destInfo.Elemts)
	if err == nil {
		return dest, nil
	} else if goredis.IsNilError(err) {
		return nil, ErrNullData
	} else {
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheRows[T]) ExistOC(ctx context.Context, condValue interface{}, dataKeyValue interface{}) (bool, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return false, errors.New("need config oneCondField")
	}
	return c.Exist(ctx, NewConds().Eq(c.oneCondField, condValue), dataKeyValue)
}

// 读取数据
// cond：查询条件变量
// 返回值：是T结构类型的指针
func (c *CacheRows[T]) Exist(ctx context.Context, cond TableConds, dataKeyValue interface{}) (bool, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return false, err
	}

	// 数据key的类型和值
	dataKeyVO := reflect.ValueOf(dataKeyValue)
	if !(c.tableInfo.ElemtsType[c.dataKeyFieldIndex] == dataKeyVO.Type() ||
		(c.tableInfo.ElemtsType[c.dataKeyFieldIndex].Kind() == reflect.Pointer && c.tableInfo.ElemtsType[c.dataKeyFieldIndex].Elem() == dataKeyVO.Type())) {
		err := fmt.Errorf("dataKeyValue:%s type err, should be %s", dataKeyVO.Type().String(), c.tableInfo.ElemtsType[c.dataKeyFieldIndex].String())
		return false, err
	}
	dataKey := key + "_" + c.dataKeyValue(dataKeyVO)

	// 从Redis中读取
	redisParams := make([]interface{}, 0, 1+len(c.tableInfo.MySQLTags))
	redisParams = append(redisParams, c.expire)

	var rst int
	err = c.redis.DoScript2(ctx, rowsExistScript, []string{key, dataKey}, redisParams).Bind(&rst)
	if err == nil {
		return rst == 1, nil
	} else if goredis.IsNilError(err) {
		if GetPass(dataKey) {
			return false, nil
		}
		// 不处理执行下面的预加载
	} else {
		return false, err
	}

	// 预加载 尝试从数据库中读取
	preData, _, err := c.preLoad(ctx, cond, key, dataKeyValue, false, nil)
	if err == ErrNullData {
		return false, nil // 空数据 返回false
	}
	if err != nil {
		return false, err
	}
	// 预加载的数据就是新增的
	if preData != nil {
		return true, nil
	}

	// 如果不是自己执行的预加载，这里重新读取下
	err = c.redis.DoScript2(ctx, rowsExistScript, []string{key, dataKey}, redisParams).Bind(&rst)
	if err == nil {
		return rst == 1, nil
	} else if err == ErrNullData {
		return false, nil
	} else {
		return false, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Set说明
func (c *CacheRows[T]) AddOC(ctx context.Context, condValue interface{}, data interface{}) (*T, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, nil, errors.New("need config oneCondField")
	}
	return c.Add(ctx, NewConds().Eq(c.oneCondField, condValue), data)
}

// 添加数据，需要外部已经确保没有数据了调用此函数，直接添加数据
// cond：查询条件变量
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有数据key字段】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 数据key字段 会忽略set
// 返回值：是T结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRows[T]) Add(ctx context.Context, cond TableConds, data interface{}) (*T, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}

	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return nil, nil, err
	}

	// 检查data数据
	dataInfo, err := c.checkData(data)
	if err != nil {
		return nil, nil, err
	}

	// 判断data中是否含有数据key字段
	dataKeyIndex := dataInfo.FindIndexByTag(c.dataKeyField)
	if dataKeyIndex == -1 {
		err := fmt.Errorf("%s must has %s field", dataInfo.T.String(), c.dataKeyField)
		return nil, nil, err
	}
	// 数据key的值
	dataKeyValue := utils.ValueFmt(dataInfo.Elemts[dataKeyIndex])

	// 加锁
	unlock, err := c.redis.Lock(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
	if err != nil {
		return nil, nil, err
	}
	defer unlock()

	incrValue, err := c.addToMySQL(ctx, cond, dataInfo) // 自增加进去
	if err != nil {
		return nil, nil, err
	}
	DelPass(key)
	// 保存到redis中
	t, err := c.mysqlToRedis(ctx, cond, key, dataKeyValue)
	if err != nil {
		return nil, nil, err
	}
	return t, incrValue, nil
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheRows[T]) DelCacheOC(ctx context.Context, condValue interface{}) error {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return errors.New("need config oneCondField")
	}
	return c.DelCache(ctx, NewConds().Eq(c.oneCondField, condValue))
}

// 只Cache删除数据
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// 返回值：error
func (c *CacheRows[T]) DelCache(ctx context.Context, cond TableConds) error {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}

	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return err
	}

	cmd := c.redis.DoScript(ctx, rowsDelScript, []string{key})
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Set说明
func (c *CacheRows[T]) SetOC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (*T, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, nil, errors.New("need config oneCondField")
	}
	return c.Set(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 写数据
// cond：查询条件变量
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有数据key字段】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略set
// nc：表示不存在是否创建
// 返回值：是T结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRows[T]) Set(ctx context.Context, cond TableConds, data interface{}, nc bool) (*T, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查data数据
	dataInfo, err := c.checkData(data)
	if err != nil {
		return nil, nil, err
	}
	return c._Set(ctx, cond, dataInfo, nc)
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见SetM说明
func (c *CacheRows[T]) SetMOC(ctx context.Context, condValue interface{}, data map[string]interface{}, nc bool) (*T, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, nil, errors.New("need config oneCondField")
	}
	return c.SetM(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 写数据
// cond：查询条件变量
// data：修改内容
// -     【内部必须要有数据key字段】
// -     data中key名称需要和T的tag一致，可以是T的一部分
// -     若data中含有设置的自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：是T结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRows[T]) SetM(ctx context.Context, cond TableConds, data map[string]interface{}, nc bool) (*T, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查data数据
	dataInfo, err := c.checkDataM(data)
	if err != nil {
		return nil, nil, err
	}
	return c._Set(ctx, cond, dataInfo, nc)
}

func (c *CacheRows[T]) _Set(ctx context.Context, cond TableConds, dataInfo *utils.StructInfo, nc bool) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return nil, nil, err
	}

	// 判断data中是否含有数据key字段
	dataKeyIndex := dataInfo.FindIndexByTag(c.dataKeyField)
	if dataKeyIndex == -1 {
		err := fmt.Errorf("%s must has %s field", dataInfo.T.String(), c.dataKeyField)
		return nil, nil, err
	}
	// 数据key的值
	dataKeyValue := utils.ValueFmt(dataInfo.Elemts[dataKeyIndex])
	dataKey := key + "_" + c.dataKeyValue(dataInfo.Elemts[dataKeyIndex])

	// 写数据
	dest, err := c.redisSetGetToMysql(ctx, cond, dataKeyValue, key, dataKey, dataInfo)
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, cond, key, dataKeyValue, nc, dataInfo)
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return preData, incrValue, nil
	}

	// 再次写数据
	dest, err = c.redisSetGetToMysql(ctx, cond, dataKeyValue, key, dataKey, dataInfo)
	if err == nil {
		return dest, nil, nil
	} else {
		return nil, nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Set2说明
func (c *CacheRows[T]) Set2OC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, errors.New("need config oneCondField")
	}
	return c.Set2(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 写数据
// cond：查询条件变量
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有数据key字段】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略set
// nc：表示不存在是否创建
// 返回值：如果有自增，返回自增ID interface是int64类型 =nil表示没有新增
func (c *CacheRows[T]) Set2(ctx context.Context, cond TableConds, data interface{}, nc bool) (interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查data数据
	dataInfo, err := c.checkData(data)
	if err != nil {
		return nil, err
	}
	return c._Set2(ctx, cond, dataInfo, nc)
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见SetM2说明
func (c *CacheRows[T]) SetM2OC(ctx context.Context, condValue interface{}, data map[string]interface{}, nc bool) (interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, errors.New("need config oneCondField")
	}
	return c.SetM2(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 写数据
// cond：查询条件变量
// data：修改内容
// -     【内部必须要有数据key字段】
// -     data中key名称需要和T的tag一致，可以是T的一部分
// -     若data中含有设置的自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：如果有自增，返回自增ID interface是int64类型 =nil表示没有新增
func (c *CacheRows[T]) SetM2(ctx context.Context, cond TableConds, data map[string]interface{}, nc bool) (interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查data数据
	dataInfo, err := c.checkDataM(data)
	if err != nil {
		return nil, err
	}
	return c._Set2(ctx, cond, dataInfo, nc)
}

func (c *CacheRows[T]) _Set2(ctx context.Context, cond TableConds, dataInfo *utils.StructInfo, nc bool) (interface{}, error) {
	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return nil, err
	}

	// 判断data中是否含有数据key字段
	dataKeyIndex := dataInfo.FindIndexByTag(c.dataKeyField)
	if dataKeyIndex == -1 {
		err := fmt.Errorf("%s must has %s field", dataInfo.T.String(), c.dataKeyField)
		return nil, err
	}
	// 数据key的值
	dataKeyValue := utils.ValueFmt(dataInfo.Elemts[dataKeyIndex])
	dataKey := key + "_" + c.dataKeyValue(dataInfo.Elemts[dataKeyIndex])

	// 写数据
	err = c.redisSetToMysql(ctx, cond, dataKeyValue, key, dataKey, dataInfo)
	if err == nil {
		return nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	_, incrValue, err := c.preLoad(ctx, cond, key, dataKeyValue, nc, dataInfo)
	if err != nil {
		return nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return incrValue, nil
	}

	// 再次写数据
	err = c.redisSetToMysql(ctx, cond, dataKeyValue, key, dataKey, dataInfo)
	if err == nil {
		return incrValue, nil
	} else {
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Modify说明
func (c *CacheRows[T]) ModifyOC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (*T, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, nil, errors.New("need config oneCondField")
	}
	return c.Modify(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 写数据
// cond：查询条件变量
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有数据key字段】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略掉modify
// nc：表示不存在是否创建
// 返回值：是T结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRows[T]) Modify(ctx context.Context, cond TableConds, data interface{}, nc bool) (*T, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查data数据
	dataInfo, err := c.checkData(data)
	if err != nil {
		return nil, nil, err
	}

	// 修改后的值
	res := reflect.New(dataInfo.T).Interface()
	resInfo, _ := utils.GetStructInfoByTag(res, DBTag)

	return c._Modify(ctx, cond, dataInfo, resInfo, nc)
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见ModifyM说明
func (c *CacheRows[T]) ModifyMOC(ctx context.Context, condValue interface{}, data map[string]interface{}, nc bool) (*T, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, nil, errors.New("need config oneCondField")
	}
	return c.ModifyM(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 写数据
// cond：查询条件变量
// data：修改内容
// -     【内部必须要有数据key字段】
// -     data中key名称需要和T的tag一致，可以是T的一部分
// -     若data中含有设置的自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：是T结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRows[T]) ModifyM(ctx context.Context, cond TableConds, data map[string]interface{}, nc bool) (*T, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查data数据
	dataInfo, err := c.checkDataM(data)
	if err != nil {
		return nil, nil, err
	}

	// 修改后的值
	resInfo := &utils.StructInfo{
		T:      dataInfo.T,
		V:      dataInfo.V,
		Tags:   dataInfo.Tags,
		Elemts: []reflect.Value{},
	}
	for _, e := range dataInfo.Elemts {
		resInfo.Elemts = append(resInfo.Elemts, reflect.New(e.Type()).Elem()) // 用来接受修改后的值
	}

	return c._Modify(ctx, cond, dataInfo, resInfo, nc)
}

func (c *CacheRows[T]) _Modify(ctx context.Context, cond TableConds, dataInfo, resInfo *utils.StructInfo, nc bool) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return nil, nil, err
	}

	// 判断data中是否含有数据key字段
	dataKeyIndex := dataInfo.FindIndexByTag(c.dataKeyField)
	if dataKeyIndex == -1 {
		err := fmt.Errorf("%s must has %s field", dataInfo.T.String(), c.dataKeyField)
		return nil, nil, err
	}
	// 自增字段的值
	dataKeyValue := utils.ValueFmt(dataInfo.Elemts[dataKeyIndex])
	dataKey := key + "_" + c.dataKeyValue(dataInfo.Elemts[dataKeyIndex])

	// 写数据
	dest, err := c.redisModifyGetToMysql1(ctx, cond, dataKeyValue, key, dataKey, dataInfo, resInfo)
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, cond, key, dataKeyValue, nc, dataInfo)
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return preData, incrValue, nil
	}

	// 再次写数据
	dest, err = c.redisModifyGetToMysql1(ctx, cond, dataKeyValue, key, dataKey, dataInfo, resInfo)
	if err == nil {
		return dest, nil, nil
	} else {
		return nil, nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Modify2说明
func (c *CacheRows[T]) Modify2OC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (interface{}, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, nil, errors.New("need config oneCondField")
	}
	return c.Modify2(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 写数据
// cond：查询条件变量
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有数据key字段】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略掉modify
// nc：表示不存在是否创建
// 返回值：是data结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRows[T]) Modify2(ctx context.Context, cond TableConds, data interface{}, nc bool) (interface{}, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}

	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return nil, nil, err
	}

	// 检查data数据
	dataInfo, err := c.checkData(data)
	if err != nil {
		return nil, nil, err
	}

	// 判断data中是否含有数据key字段
	dataKeyIndex := dataInfo.FindIndexByTag(c.dataKeyField)
	if dataKeyIndex == -1 {
		err := fmt.Errorf("%s must has %s field", dataInfo.T.String(), c.dataKeyField)
		return nil, nil, err
	}
	// 自增字段的值
	dataKeyValue := utils.ValueFmt(dataInfo.Elemts[dataKeyIndex])
	dataKey := key + "_" + c.dataKeyValue(dataInfo.Elemts[dataKeyIndex])

	// 修改后的值
	res := reflect.New(dataInfo.T).Interface()
	resInfo, _ := utils.GetStructInfoByTag(res, DBTag)

	// 写数据
	err = c.redisModifyGetToMysql2(ctx, cond, dataKeyValue, key, dataKey, dataInfo, resInfo)
	if err == nil {
		return res, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, cond, key, dataKeyValue, nc, dataInfo)
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，从预加载数据中拷贝返回值
	if incrValue != nil {
		dInfo, _ := utils.GetStructInfoByTag(preData, DBTag)
		dInfo.CopyTo(resInfo)
		return res, incrValue, nil
	}

	// 再次写数据
	err = c.redisModifyGetToMysql2(ctx, cond, dataKeyValue, key, dataKey, dataInfo, resInfo)
	if err == nil {
		return res, nil, nil
	} else {
		return nil, nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见ModifyM2说明
func (c *CacheRows[T]) ModifyM2OC(ctx context.Context, condValue interface{}, data map[string]interface{}, nc bool) (interface{}, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, nil, errors.New("need config oneCondField")
	}
	return c.ModifyM2(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 写数据
// cond：查询条件变量
// data：修改内容
// -     【内部必须要有数据key字段】
// -     data中key名称需要和T的tag一致，可以是T的一部分
// -     若data中含有设置的自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：是data结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRows[T]) ModifyM2(ctx context.Context, cond TableConds, data map[string]interface{}, nc bool) (interface{}, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}

	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return nil, nil, err
	}

	// 检查data数据
	dataInfo, err := c.checkDataM(data)
	if err != nil {
		return nil, nil, err
	}

	// 判断data中是否含有数据key字段
	dataKeyIndex := dataInfo.FindIndexByTag(c.dataKeyField)
	if dataKeyIndex == -1 {
		err := fmt.Errorf("%s must has %s field", dataInfo.T.String(), c.dataKeyField)
		return nil, nil, err
	}
	// 自增字段的值
	dataKeyValue := utils.ValueFmt(dataInfo.Elemts[dataKeyIndex])
	dataKey := key + "_" + c.dataKeyValue(dataInfo.Elemts[dataKeyIndex])

	// 修改后的值
	resInfo := &utils.StructInfo{
		T:      dataInfo.T,
		V:      dataInfo.V,
		Tags:   dataInfo.Tags,
		Elemts: []reflect.Value{},
	}
	for _, e := range dataInfo.Elemts {
		resInfo.Elemts = append(resInfo.Elemts, reflect.New(e.Type()).Elem()) // 用来接受修改后的值
	}

	// 写数据
	err = c.redisModifyGetToMysql2(ctx, cond, dataKeyValue, key, dataKey, dataInfo, resInfo)
	if err == nil {
		res := map[string]interface{}{}
		for i, e := range resInfo.Elemts {
			res[resInfo.Tags[i].(string)] = e.Interface()
		}
		return res, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, cond, key, dataKeyValue, nc, dataInfo)
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，从预加载数据中拷贝返回值
	if incrValue != nil {
		dInfo, _ := utils.GetStructInfoByTag(preData, DBTag)
		dInfo.CopyTo(resInfo)
		res := map[string]interface{}{}
		for i, e := range resInfo.Elemts {
			res[resInfo.Tags[i].(string)] = e.Interface()
		}
		return res, incrValue, nil
	}

	// 再次写数据
	err = c.redisModifyGetToMysql2(ctx, cond, dataKeyValue, key, dataKey, dataInfo, resInfo)
	if err == nil {
		res := map[string]interface{}{}
		for i, e := range resInfo.Elemts {
			res[resInfo.Tags[i].(string)] = e.Interface()
		}
		return res, nil, nil
	} else {
		return nil, nil, err
	}
}

// 没有返回值
func (c *CacheRows[T]) redisSetToMysql(ctx context.Context, cond TableConds, dataKeyValue interface{}, key, dataKey string, dataInfo *utils.StructInfo) error {
	// 获取Redis参数
	redisParams := c.redisSetParam(cond, dataInfo)
	cmd := c.redis.DoScript2(ctx, rowsSetScript, []string{key, dataKey}, redisParams...)
	if cmd.Cmd.Err() == nil {
		// 同步mysql，添加上数据key字段
		err := c.saveToMySQL(ctx, cond.Eq(c.dataKeyField, dataKeyValue), dataInfo)
		if err != nil {
			c.redis.Del(ctx, dataKey) // mysql错了 删除数据键
			return err
		}
		return nil
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return ErrNullData
		}
		return cmd.Cmd.Err()
	}
}

// 返回值*T
func (c *CacheRows[T]) redisSetGetToMysql(ctx context.Context, cond TableConds, dataKeyValue interface{}, key, dataKey string, dataInfo *utils.StructInfo) (*T, error) {
	// 获取Redis参数
	redisParams := c.redisSetGetParam1(cond, dataInfo, false)
	cmd := c.redis.DoScript2(ctx, rowsModifyScript, []string{key, dataKey}, redisParams...)
	if cmd.Cmd.Err() == nil {
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByTag(dest, DBTag)
		err := cmd.BindValues(destInfo.Elemts)
		if err == nil {
			// 同步mysql，添加上数据key字段
			err := c.saveToMySQL(ctx, cond.Eq(c.dataKeyField, dataKeyValue), dataInfo)
			if err != nil {
				c.redis.Del(ctx, dataKey) // mysql错了 删除数据键
				return nil, err
			}
			return dest, nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, dataKey) // 删除数据键
			return nil, err
		}
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return nil, ErrNullData
		}
		return nil, cmd.Cmd.Err()
	}
}

// 返回值*T 值数据自增
// resInfo 是修改后的数据 和 dataInfo类型一致
func (c *CacheRows[T]) redisModifyGetToMysql1(ctx context.Context, cond TableConds, dataKeyValue interface{}, key, dataKey string, dataInfo *utils.StructInfo, resInfo *utils.StructInfo) (*T, error) {
	// 获取Redis参数
	redisParams := c.redisSetGetParam1(cond, dataInfo, true)
	cmd := c.redis.DoScript2(ctx, rowsModifyScript, []string{key, dataKey}, redisParams...)
	if cmd.Cmd.Err() == nil {
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByTag(dest, DBTag)
		err := cmd.BindValues(destInfo.Elemts)
		if err == nil {
			// 同步mysql，把T结构的值 拷贝到新创建的resInfo中再保存
			destInfo.CopyTo(resInfo)
			err := c.saveToMySQL(ctx, cond.Eq(c.dataKeyField, dataKeyValue), resInfo)
			if err != nil {
				c.redis.Del(ctx, dataKey) // mysql错了 删除数据键
				return nil, err
			}
			return dest, nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, dataKey) // 删除数据键
			return nil, err
		}
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return nil, ErrNullData
		}
		return nil, cmd.Cmd.Err()
	}
}

// 不要返回值
// resInfo 是修改后的数据 和 dataInfo类型一致
func (c *CacheRows[T]) redisModifyGetToMysql2(ctx context.Context, cond TableConds, dataKeyValue interface{}, key, dataKey string, dataInfo *utils.StructInfo, resInfo *utils.StructInfo) error {
	// 获取Redis参数
	redisParams := c.redisSetGetParam2(cond, dataInfo, true)
	cmd := c.redis.DoScript2(ctx, rowsModifyScript, []string{key, dataKey}, redisParams...)
	if cmd.Cmd.Err() == nil {
		err := cmd.BindValues(resInfo.Elemts)
		if err == nil {
			// 同步mysql，添加上数据字段
			err := c.saveToMySQL(ctx, cond.Eq(c.dataKeyField, dataKeyValue), resInfo)
			if err != nil {
				c.redis.Del(ctx, dataKey) // mysql错了 删除数据键
				return err
			}
			return nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, dataKey) // 删除数据键
			return err
		}
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return ErrNullData
		}
		return cmd.Cmd.Err()
	}
}

// 检查条件，返回索引key
// key格式：mrrs_表名_{condValue1}_condValue2  如果CacheRow.hashTagField == condValue1 condValue1会添加上{}
func (c *CacheRows[T]) checkCond(cond TableConds) (string, error) {
	if len(cond) == 0 {
		return "", errors.New("cond is nil")
	}

	// 必须要设置数据key
	if len(c.dataKeyField) == 0 {
		return "", errors.New("dataKeyField is nil")
	}

	// 条件中的字段必须都存在，且类型还要一致
	for _, v := range cond {
		if v.op != "=" {
			err := fmt.Errorf("cond:%s not Eq", v.field) // 条件变量的是等号
			return "", err
		}
		at := c.tableInfo.FindIndexByTag(v.field)
		if at == -1 {
			err := fmt.Errorf("tag:%s not find in %s", v.field, c.tableInfo.T.String())
			return "", err
		}
		vo := reflect.ValueOf(v.value)
		if !(c.tableInfo.ElemtsType[at] == vo.Type() ||
			(c.tableInfo.ElemtsType[at].Kind() == reflect.Pointer && c.tableInfo.ElemtsType[at].Elem() == vo.Type())) {
			err := fmt.Errorf("tag:%s(%s) type err, should be %s", v.field, vo.Type().String(), c.tableInfo.ElemtsType[at].String())
			return "", err
		}
	}

	return c.genKey(cond), nil
}

// 预加载所有数据，确保写到Redis中
// key：主key
// 返回值
// []*T： 因为加载时抢占式的，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// error： 执行结果
func (c *CacheRows[T]) preLoadAll(ctx context.Context, cond TableConds, key string) ([]*T, error) {
	unlock, err := c.redis.TryLockWait(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
	if err == nil && unlock != nil {
		defer unlock()
		// 加载
		t, err := c.mysqlAllToRedis(ctx, cond, key)
		return t, err
	}
	return nil, nil
}

func (c *CacheRows[T]) mysqlAllToRedis(ctx context.Context, cond TableConds, key string) ([]*T, error) {
	t, err := c.getsFromMySQL(ctx, c.tableInfo.TS, c.tableInfo.MySQLTags, cond)
	if err != nil {
		if err == ErrNullData {
			SetPass(key)
		}
		return nil, err
	}
	allData := t.([]*T)

	// 保存到redis中
	dataKeys := []string{key}
	redisKV := make([][]interface{}, 0, 1+len(allData))
	redisKV = append(redisKV, make([]interface{}, 0, 1+len(allData))) // 第0个位置是总key数据
	redisKVNum := 0
	for _, d := range allData {
		tInfo, _ := utils.GetStructInfoByTag(d, DBTag)
		thisKeyValue := utils.ValueFmt(tInfo.Elemts[c.dataKeyFieldIndex])
		if thisKeyValue == nil {
			continue // 理论上不应该 忽略这条数据
		}
		sli := make([]interface{}, 0, len(tInfo.Tags))
		for i, v := range tInfo.Elemts {
			vfmt := utils.ValueFmt(v)
			if vfmt == nil {
				continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
			}
			sli = append(sli, c.tableInfo.RedisTags[i])
			sli = append(sli, vfmt)
		}
		thisK := key + "_" + fmt.Sprint(thisKeyValue)
		dataKeys = append(dataKeys, thisK)
		redisKV[0] = append(redisKV[0], thisKeyValue)
		redisKV[0] = append(redisKV[0], thisK)
		redisKV = append(redisKV, sli)
		redisKVNum += (2 + len(sli))
	}

	redisParams := make([]interface{}, 0, 1+len(dataKeys)+redisKVNum) // 过期 num数 kv数
	redisParams = append(redisParams, c.expire)
	for _, params := range redisKV {
		redisParams = append(redisParams, len(params))
		redisParams = append(redisParams, params...)
	}

	cmd := c.redis.DoScript(ctx, rowsAddScript, dataKeys, redisParams...)
	if cmd.Err() != nil {
		c.redis.Del(ctx, dataKeys...) // 失败了，删除所有键
		return nil, cmd.Err()
	}
	return allData, nil
}

// 预加载一条数据，确保写到Redis中
// key：主key
// dataKeyValue 这条数据的值
// nc 不存在就创建
// 返回值
// *T： 因为加载时抢占式的，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// interface{} != nil时 表示产生的自增id 创建时才返回
// error： 执行结果
func (c *CacheRows[T]) preLoad(ctx context.Context, cond TableConds, key string, dataKeyValue interface{}, nc bool, dataInfo *utils.StructInfo) (*T, interface{}, error) {
	// 根据是否要创建数据，来判断使用什么锁
	if nc {
		// 不存在要创建数据
		unlock, err := c.redis.Lock(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
		if err != nil {
			return nil, nil, err
		}
		defer unlock()

		// 查询mysql是否存在这条数据
		_, err = c.getFromMySQL(ctx, c.tableInfo.T, c.tableInfo.MySQLTags, cond.Eq(c.dataKeyField, dataKeyValue)) // 此处用get函数
		if err != nil && err != ErrNullData {
			return nil, nil, err
		}
		var incrValue interface{}
		if err == ErrNullData {
			// 如果是空数据 添加一条
			incrValue, err = c.addToMySQL(ctx, cond, dataInfo) // 自增加进去
			if err != nil {
				return nil, nil, err
			}
			DelPass(key)
		}

		// 加载
		t, err := c.mysqlToRedis(ctx, cond, key, dataKeyValue)
		return t, incrValue, err
	} else {
		// 不用创建
		unlock, err := c.redis.TryLockWait(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
		if err == nil && unlock != nil {
			defer unlock()
			// 加载
			t, err := c.mysqlToRedis(ctx, cond, key, dataKeyValue)
			return t, nil, err
		}
		return nil, nil, nil
	}
}

func (c *CacheRows[T]) mysqlToRedis(ctx context.Context, cond TableConds, key string, dataKeyValue interface{}) (*T, error) {
	// 先查询到要读取的数据
	t, err := c.getFromMySQL(ctx, c.tableInfo.T, c.tableInfo.MySQLTags, cond.Eq(c.dataKeyField, dataKeyValue))
	if err != nil {
		if err == ErrNullData {
			SetPass(key)
		}
		return nil, err
	}
	// 查询所有的dataValue，只读取dataKeyField对应的值，最好控制好正常覆盖索引就可以读取所有的数了
	allKeyT, err := c.getsFromMySQL(ctx, c.tableInfo.TS, []interface{}{c.dataKeyField}, cond)
	if err != nil {
		return nil, err
	}

	data := t.(*T)
	allData := allKeyT.([]*T)

	// 保存到redis中
	dataKeys := []string{key}
	redisKV := make([][]interface{}, 0, 1+len(allData))
	redisKV = append(redisKV, make([]interface{}, 0, 1+len(allData))) // 第0个位置是总key数据
	redisKVNum := 0
	for _, d := range allData {
		tInfo, _ := utils.GetStructInfoByTag(d, DBTag)
		thisKeyValue := utils.ValueFmt(tInfo.Elemts[c.dataKeyFieldIndex])
		if thisKeyValue == nil {
			continue // 理论上不应该 忽略这条数据
		}
		sli := make([]interface{}, 0)
		if fmt.Sprint(thisKeyValue) == fmt.Sprint(dataKeyValue) {
			dataInfo, _ := utils.GetStructInfoByTag(data, DBTag) // 数据使用上面读取的那一条
			sli = make([]interface{}, 0, len(dataInfo.Tags))
			for i, v := range dataInfo.Elemts {
				vfmt := utils.ValueFmt(v)
				if vfmt == nil {
					continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
				}
				sli = append(sli, c.tableInfo.RedisTags[i])
				sli = append(sli, vfmt)
			}
		}
		thisK := key + "_" + fmt.Sprint(thisKeyValue)
		dataKeys = append(dataKeys, thisK)
		redisKV[0] = append(redisKV[0], thisKeyValue)
		redisKV[0] = append(redisKV[0], thisK)
		redisKV = append(redisKV, sli)
		redisKVNum += (2 + len(sli))
	}

	redisParams := make([]interface{}, 0, 1+len(dataKeys)+redisKVNum) // 过期 num数 kv数
	redisParams = append(redisParams, c.expire)
	for _, params := range redisKV {
		redisParams = append(redisParams, len(params))
		redisParams = append(redisParams, params...)
	}

	cmd := c.redis.DoScript(ctx, rowsAddScript, dataKeys, redisParams...)
	if cmd.Err() != nil {
		c.redis.Del(ctx, dataKeys...) // 失败了，删除所有键
		return nil, cmd.Err()
	}
	return data, nil
}

// 解析Redis数据，reply的长度就是数据的数量
// 按T来解析
func (c *CacheRows[T]) redisGetAll(ctx context.Context, key string, redisParams []interface{}) ([]*T, error) {
	reply := make([][]interface{}, 0)
	err := c.redis.DoScript2(ctx, rowsGetAllScript, []string{key}, redisParams).BindSlice(&reply)
	if err == nil {
		res := make([]*T, 0, len(reply))
		for _, r := range reply {
			dest := new(T)
			destInfo, _ := utils.GetStructInfoByTag(dest, DBTag) // 这里不用判断err了
			for i, v := range r {
				if v == nil {
					continue
				}
				err := utils.InterfaceToValue(v, destInfo.Elemts[i])
				if err != nil {
					return nil, err
				}
			}
			res = append(res, dest)
		}
		return res, nil
	} else {
		if goredis.IsNilError(err) {
			return nil, ErrNullData
		}
		return nil, err
	}
}

// 组织redis数据，数据格式符合rowsSetScript脚本解析
func (c *CacheRows[T]) redisSetParam(cond TableConds, dataInfo *utils.StructInfo) []interface{} {
	redisParams := make([]interface{}, 0, 1+len(dataInfo.Elemts)*2)
	redisParams = append(redisParams, c.expire)
	for i, v := range dataInfo.Elemts {
		if c.saveIgnoreTag(dataInfo.Tags[i].(string), cond) {
			continue
		}
		vfmt := utils.ValueFmt(v)
		if vfmt == nil {
			continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
		}
		redisParams = append(redisParams, c.tableInfo.GetRedisTagByTag(dataInfo.Tags[i]))
		redisParams = append(redisParams, vfmt)
	}
	return redisParams
}
