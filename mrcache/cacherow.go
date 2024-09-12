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
// 使用场景：查询条件只对应一个结果 redis使用hash结构缓存这个结果
// redis的hash的field和mysql的field对应
// redis的key 见CacheRow.checkCond说明
type CacheRow[T any] struct {
	*Cache
}

func NewCacheRow[T any](redis *goredis.Redis, mysql *mysql.MySQL, tableName string) *CacheRow[T] {
	table := GetTableStruct[T]()
	c := &CacheRow[T]{
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
	return c
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheRow[T]) GetOC(ctx context.Context, condValue interface{}) (*T, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, errors.New("need config oneCondField")
	}
	return c.Get(ctx, NewConds().Eq(c.oneCondField, condValue))
}

// 读取数据
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// 返回值：是T结构类型的指针
func (c *CacheRow[T]) Get(ctx context.Context, cond TableConds) (*T, error) {
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

	dest := new(T)
	destInfo, _ := utils.GetStructInfoByTag(dest, DBTag) // 这里不用判断err了
	err = c.redis.DoScript2(ctx, rowGetScript, []string{key}, redisParams).BindValues(destInfo.Elemts)
	if err == nil {
		return dest, nil
	} else if goredis.IsNilError(err) {
		if GetPass(key) {
			return nil, ErrNullData
		}
		// 不处理执行下面的预加载
	} else {
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, _, err := c.preLoad(ctx, cond, key, false, nil)
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
	err = c.redis.DoScript2(ctx, rowGetScript, []string{key}, redisParams).BindValues(destInfo.Elemts)
	if err == nil {
		return dest, nil
	} else if err == ErrNullData {
		return nil, ErrNullData
	} else {
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheRow[T]) ExistOC(ctx context.Context, condValue interface{}) (bool, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return false, errors.New("need config oneCondField")
	}
	return c.Exist(ctx, NewConds().Eq(c.oneCondField, condValue))
}

// 读取数据
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// 返回值：是否存在
func (c *CacheRow[T]) Exist(ctx context.Context, cond TableConds) (bool, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return false, err
	}

	// 从Redis中读取
	redisParams := make([]interface{}, 0, 1+len(c.tableInfo.MySQLTags))
	redisParams = append(redisParams, c.expire)

	var rst int
	err = c.redis.DoScript2(ctx, rowExistScript, []string{key}, redisParams).Bind(&rst)
	if err == nil {
		return rst == 1, nil
	} else if goredis.IsNilError(err) {
		if GetPass(key) {
			return false, nil
		}
		// 不处理执行下面的预加载
	} else {
		return false, err
	}

	// 预加载 尝试从数据库中读取
	preData, _, err := c.preLoad(ctx, cond, key, false, nil)
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
	err = c.redis.DoScript2(ctx, rowExistScript, []string{key}, redisParams).Bind(&rst)
	if err == nil {
		return rst == 1, nil
	} else if err == ErrNullData {
		return false, nil
	} else {
		return false, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheRow[T]) AddOC(ctx context.Context, condValue interface{}, data interface{}) (*T, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, nil, errors.New("need config oneCondField")
	}
	return c.Add(ctx, NewConds().Eq(c.oneCondField, condValue), data)
}

// 添加数据，需要外部已经确保没有数据了调用此函数，直接添加数据
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// 返回值：是T结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRow[T]) Add(ctx context.Context, cond TableConds, data interface{}) (*T, interface{}, error) {
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

	// 加锁
	unlock, err := c.redis.Lock(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
	if err != nil {
		return nil, nil, err
	}
	defer unlock()

	incrValue, err := c.addToMySQL(ctx, cond, dataInfo)
	if err != nil {
		return nil, nil, err
	}
	DelPass(key)
	// 重新加载下
	t, err := c.getFromMySQL(ctx, c.tableInfo.T, c.tableInfo.MySQLTags, cond)
	if err != nil {
		return nil, nil, err
	}

	resp := t.(*T)
	// 保存到redis中
	err = c.mysqlToRedis(ctx, key, resp)
	if err != nil {
		return nil, nil, err
	}
	return resp, incrValue, nil
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheRow[T]) DelOC(ctx context.Context, condValue interface{}) error {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return errors.New("need config oneCondField")
	}
	return c.Del(ctx, NewConds().Eq(c.oneCondField, condValue))
}

// 删除数据
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// 返回值：error
func (c *CacheRow[T]) Del(ctx context.Context, cond TableConds) error {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}

	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return err
	}

	c.redis.Del(ctx, key)         // 删缓存
	err = c.delToMySQL(ctx, cond) // 删mysql

	if err != nil {
		return err
	}
	return nil
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
func (c *CacheRow[T]) DelCacheOC(ctx context.Context, condValue interface{}) error {
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
func (c *CacheRow[T]) DelCache(ctx context.Context, cond TableConds) error {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}

	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return err
	}

	cmd := c.redis.Del(ctx, key) // 删缓存

	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Set说明
func (c *CacheRow[T]) SetOC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (*T, interface{}, error) {
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
// -     可以是结构或者结构指针，内部的数据是要保存的数据
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：是T结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRow[T]) Set(ctx context.Context, cond TableConds, data interface{}, nc bool) (*T, interface{}, error) {
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
func (c *CacheRow[T]) SetMOC(ctx context.Context, condValue interface{}, data map[string]interface{}, nc bool) (*T, interface{}, error) {
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
// -     data中key名称需要和T的tag一致，可以是T的一部分
// -     若data中含有设置的自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：是T结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRow[T]) SetM(ctx context.Context, cond TableConds, data map[string]interface{}, nc bool) (*T, interface{}, error) {
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

func (c *CacheRow[T]) _Set(ctx context.Context, cond TableConds, dataInfo *utils.StructInfo, nc bool) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return nil, nil, err
	}

	dest, err := c.redisSetGetToMysql1(ctx, cond, key, dataInfo)
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, cond, key, nc, dataInfo)
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return preData, incrValue, nil
	}

	// 再次写数据
	dest, err = c.redisSetGetToMysql1(ctx, cond, key, dataInfo)
	if err == nil {
		return dest, nil, nil
	} else {
		return nil, nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Set2说明
func (c *CacheRow[T]) Set2OC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (interface{}, error) {
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
// -     可以是结构或者结构指针，内部的数据是要保存的数据
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：如果有自增，返回自增ID interface是int64类型 =nil表示没有新增
func (c *CacheRow[T]) Set2(ctx context.Context, cond TableConds, data interface{}, nc bool) (interface{}, error) {
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
// 见Set2说明
func (c *CacheRow[T]) SetM2OC(ctx context.Context, condValue interface{}, data map[string]interface{}, nc bool) (interface{}, error) {
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
// -     data中key名称需要和T的tag一致，可以是T的一部分
// -     若data中含有设置的自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：如果有自增，返回自增ID interface是int64类型 =nil表示没有新增
func (c *CacheRow[T]) SetM2(ctx context.Context, cond TableConds, data map[string]interface{}, nc bool) (interface{}, error) {
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

func (c *CacheRow[T]) _Set2(ctx context.Context, cond TableConds, dataInfo *utils.StructInfo, nc bool) (interface{}, error) {
	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return nil, err
	}
	err = c.redisSetToMysql(ctx, cond, key, dataInfo)
	if err == nil {
		return nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	_, incrValue, err := c.preLoad(ctx, cond, key, nc, dataInfo)
	if err != nil {
		return nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return incrValue, nil
	}

	// 再次写数据
	err = c.redisSetToMysql(ctx, cond, key, dataInfo)
	if err == nil {
		return incrValue, nil
	} else {
		return nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Modify说明
func (c *CacheRow[T]) ModifyOC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (*T, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, nil, errors.New("need config oneCondField")
	}
	return c.Modify(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 增量修改数据
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：是T结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRow[T]) Modify(ctx context.Context, cond TableConds, data interface{}, nc bool) (*T, interface{}, error) {
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
func (c *CacheRow[T]) ModifyMOC(ctx context.Context, condValue interface{}, data map[string]interface{}, nc bool) (*T, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, nil, errors.New("need config oneCondField")
	}
	return c.ModifyM(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 增量修改数据
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// data：修改内容
// -     data中key名称需要和T的tag一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：是T结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRow[T]) ModifyM(ctx context.Context, cond TableConds, data map[string]interface{}, nc bool) (*T, interface{}, error) {
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

func (c *CacheRow[T]) _Modify(ctx context.Context, cond TableConds, dataInfo, resInfo *utils.StructInfo, nc bool) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return nil, nil, err
	}
	// 写数据
	dest, err := c.redisModifyGetToMysql1(ctx, cond, key, dataInfo, resInfo)
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, cond, key, nc, dataInfo)
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return preData, incrValue, nil
	}

	// 再次写数据
	dest, err = c.redisModifyGetToMysql1(ctx, cond, key, dataInfo, resInfo)
	if err == nil {
		return dest, nil, nil
	} else {
		return nil, nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见Modify2说明
func (c *CacheRow[T]) Modify2OC(ctx context.Context, condValue interface{}, data interface{}, nc bool) (interface{}, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, nil, errors.New("need config oneCondField")
	}
	return c.Modify2(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 增量修改数据
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：是data结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRow[T]) Modify2(ctx context.Context, cond TableConds, data interface{}, nc bool) (interface{}, interface{}, error) {
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

	// 修改后的值
	res := reflect.New(dataInfo.T).Interface()
	resInfo, _ := utils.GetStructInfoByTag(res, DBTag)

	// 写数据
	err = c.redisModifyGetToMysql2(ctx, cond, key, dataInfo, resInfo)
	if err == nil {
		return res, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, cond, key, nc, dataInfo)
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，从预加载数据中拷贝返回值
	if incrValue != nil {
		dInfo, _ := utils.GetStructInfoByTag(preData, DBTag)
		dInfo.CopyTo(resInfo)
		return res, incrValue, nil
	}

	// 再次写数据，这种情况很少
	err = c.redisModifyGetToMysql2(ctx, cond, key, dataInfo, resInfo)
	if err == nil {
		return res, nil, nil
	} else {
		return nil, nil, err
	}
}

// 使用该函数需要提前使用ConfigOneCondField配置相关内容
// 见ModifyM2说明
func (c *CacheRow[T]) ModifyM2OC(ctx context.Context, condValue interface{}, data map[string]interface{}, nc bool) (map[string]interface{}, interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}
	if len(c.oneCondField) == 0 {
		return nil, nil, errors.New("need config oneCondField")
	}
	return c.ModifyM2(ctx, NewConds().Eq(c.oneCondField, condValue), data, nc)
}

// 增量修改数据
// cond：查询条件变量 field:value, 可以有多个条件但至少有一个条件 (条件具有唯一性，不唯一只能读取一条数据)
// data：修改内容
// -     data中key名称需要和T的tag一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略掉
// nc：表示不存在是否创建
// 返回值：是data结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRow[T]) ModifyM2(ctx context.Context, cond TableConds, data map[string]interface{}, nc bool) (map[string]interface{}, interface{}, error) {
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
	err = c.redisModifyGetToMysql2(ctx, cond, key, dataInfo, resInfo)
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
	preData, incrValue, err := c.preLoad(ctx, cond, key, nc, dataInfo)
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

	// 再次写数据，这种情况很少
	err = c.redisModifyGetToMysql2(ctx, cond, key, dataInfo, resInfo)
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
func (c *CacheRow[T]) redisSetToMysql(ctx context.Context, cond TableConds, key string, dataInfo *utils.StructInfo) error {
	// redis参数
	redisParams := c.redisSetParam(cond, dataInfo)
	cmd := c.redis.DoScript2(ctx, rowSetScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		// 同步mysql
		err := c.saveToMySQL(ctx, cond, dataInfo)
		if err != nil {
			c.redis.Del(ctx, key) // mysql错了 要删缓存
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
func (c *CacheRow[T]) redisSetGetToMysql1(ctx context.Context, cond TableConds, key string, dataInfo *utils.StructInfo) (*T, error) {
	// redis参数
	redisParams := c.redisSetGetParam1(cond, dataInfo, false)
	cmd := c.redis.DoScript2(ctx, rowModifyScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByTag(dest, DBTag)
		err := cmd.BindValues(destInfo.Elemts)
		if err == nil {
			// 同步mysql
			err := c.saveToMySQL(ctx, cond, dataInfo)
			if err != nil {
				c.redis.Del(ctx, key) // mysql错了 要删缓存
				return nil, err
			}
			return dest, nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, key)
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
func (c *CacheRow[T]) redisModifyGetToMysql1(ctx context.Context, cond TableConds, key string, dataInfo, resInfo *utils.StructInfo) (*T, error) {
	// redis参数
	redisParams := c.redisSetGetParam1(cond, dataInfo, true)
	cmd := c.redis.DoScript2(ctx, rowModifyScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByTag(dest, DBTag)
		err := cmd.BindValues(destInfo.Elemts)
		if err == nil {
			// 同步mysql，把T结构的值 拷贝到新创建的resInfo中再保存
			destInfo.CopyTo(resInfo)
			err = c.saveToMySQL(ctx, cond, resInfo)
			if err != nil {
				c.redis.Del(ctx, key) // mysql错了 要删缓存
				return nil, err
			}
			return dest, nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, key)
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
func (c *CacheRow[T]) redisModifyGetToMysql2(ctx context.Context, cond TableConds, key string, dataInfo *utils.StructInfo, resInfo *utils.StructInfo) error {
	// redis参数
	redisParams := c.redisSetGetParam2(cond, dataInfo, true)
	cmd := c.redis.DoScript2(ctx, rowModifyScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		err := cmd.BindValues(resInfo.Elemts)
		if err == nil {
			// 同步mysql
			err = c.saveToMySQL(ctx, cond, resInfo)
			if err != nil {
				c.redis.Del(ctx, key) // mysql错了 要删缓存
				return err
			}
			return nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, key)
			return err
		}
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return ErrNullData
		}
		return cmd.Cmd.Err()
	}
}

// 检查条件，返回缓存key
// key可以：mrr_表名_{condValue1}_condValue2  如果CacheRow.hashTagField == condValue1 condValue1会添加上{}
func (c *CacheRow[T]) checkCond(cond TableConds) (string, error) {
	if len(cond) == 0 {
		return "", errors.New("cond is nil")
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

// 预加载，确保写到Redis中
// key：用来加锁的key
// nc：不存在是否创建
// 返回值
// *T： 因为加载时抢占式的，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// error： 执行结果
func (c *CacheRow[T]) preLoad(ctx context.Context, cond TableConds, key string, nc bool, dataInfo *utils.StructInfo) (*T, interface{}, error) {
	// 根据是否要创建数据，来判断使用什么锁
	if nc {
		// 不存在要创建数据
		unlock, err := c.redis.Lock(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
		if err != nil {
			return nil, nil, err
		}
		defer unlock()

		var incrValue interface{}
		t, err := c.getFromMySQL(ctx, c.tableInfo.T, c.tableInfo.MySQLTags, cond)
		if err == ErrNullData {
			// 创建数据
			incrValue, err = c.addToMySQL(ctx, cond, dataInfo)
			if err != nil {
				return nil, nil, err
			}
			DelPass(key)
			// 重新加载下
			t, err = c.getFromMySQL(ctx, c.tableInfo.T, c.tableInfo.MySQLTags, cond)
			if err != nil {
				return nil, nil, err
			}
		} else if err != nil {
			return nil, nil, err
		}
		data := t.(*T)
		// 保存到redis中
		err = c.mysqlToRedis(ctx, key, data)
		if err != nil {
			return nil, nil, err
		}
		return data, incrValue, nil
	} else {
		// 不用创建
		unlock, err := c.redis.TryLockWait(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
		if err == nil && unlock != nil {
			defer unlock()
			t, err := c.getFromMySQL(ctx, c.tableInfo.T, c.tableInfo.MySQLTags, cond)
			if err != nil {
				if err == ErrNullData {
					SetPass(key)
				}
				return nil, nil, err
			}
			data := t.(*T)
			// 保存到redis中
			err = c.mysqlToRedis(ctx, key, data)
			if err != nil {
				return nil, nil, err
			}
			return data, nil, nil
		}
		return nil, nil, nil
	}
}

func (c *CacheRow[T]) mysqlToRedis(ctx context.Context, key string, data *T) error {
	// 保存到redis中
	tInfo, _ := utils.GetStructInfoByTag(data, DBTag)
	redisParams := make([]interface{}, 0, 1+len(tInfo.Tags))
	redisParams = append(redisParams, c.expire)
	for i, v := range tInfo.Elemts {
		vfmt := utils.ValueFmt(v)
		if vfmt == nil {
			continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
		}
		redisParams = append(redisParams, c.tableInfo.RedisTags[i])
		redisParams = append(redisParams, vfmt)
	}
	cmd := c.redis.DoScript(ctx, rowAddScript, []string{key}, redisParams...)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}
