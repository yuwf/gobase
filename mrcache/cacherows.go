package mrcache

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"
	"fmt"
	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/utils"
	"reflect"
	"sort"
	"strings"
	"time"
)

// T 为数据库结构类型，可重复多协程使用
// 使用场景：查询条件对应多个结果 redis使用多个key缓存这个结果，一条数据对应一个key
// 索引key：有一个总key存储所有的数据key，hash结构，datakeyvalue:datakey datakey命名：索引key名_dataKeyField对应的值
// 数据key：存储一条数据的eky，dataKey对应的存储方式和cacherow一样
// 需要使用ConfigDataKeyField配置数据key对应的字段
// 为了效率，索引key一直缓存所有的数据key，但数据key可能在redis中不存在时，此时就重新加载
// 最好查询条件和dataKeyField做一个唯一索引
type CacheRows[T any] struct {
	*Cache
}

func NewCacheRows[T any](redis *goredis.Redis, mysql *mysql.MySQL, tableName string) *CacheRows[T] {
	dest := new(T)
	sInfo, _ := utils.GetStructInfoByTag(dest, DBTag) // 不需要判断err，后续的查询中都会通过checkCond判断
	elemtsType := make([]reflect.Type, 0, len(sInfo.Elemts))
	for _, e := range sInfo.Elemts {
		elemtsType = append(elemtsType, e.Type())
	}
	c := &CacheRows[T]{
		Cache: &Cache{
			redis:     redis,
			mysql:     mysql,
			tableName: tableName,
			expire:    Expire,
			tableInfo: &TableStruct{
				T:          sInfo.T,
				TS:         reflect.ValueOf([]*T{}).Type(),
				Tags:       sInfo.Tags,
				ElemtsType: elemtsType,
			},
		},
	}
	// 默认配置第一个Key为自增
	if len(sInfo.Tags) > 0 {
		c.ConfigIncrement(redis, sInfo.Tags[0].(string), tableName)
	}
	// 默认配置第一个Key为数据key字段
	if len(sInfo.Tags) > 0 {
		c.ConfigDataKeyField(sInfo.Tags[0].(string))
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
	redisParams := make([]interface{}, 0, 1+len(c.tableInfo.Tags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, c.tableInfo.Tags...)

	dest, err := c.redisGetAll(ctx, key, redisParams)
	if err == nil {
		return dest, nil
	} else if err == ErrNullData {
		if IsPass(key) {
			return nil, fmt.Errorf("%s is pass", key)
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
	redisParams := make([]interface{}, 0, 1+len(c.tableInfo.Tags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, c.tableInfo.Tags...)

	dest := new(T)
	destInfo, _ := utils.GetStructInfoByTag(dest, DBTag) // 这里不用判断err了
	err = c.redis.DoScript2(ctx, rowsGetScript, []string{key, dataKey}, redisParams).BindValues(destInfo.Elemts)
	if err == nil {
		return dest, nil
	} else if goredis.IsNilError(err) {
		if IsPass(dataKey) {
			return nil, fmt.Errorf("%s is pass", key)
		}
		// 不处理执行下面的预加载
	} else {
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, _, err := c.preLoad(ctx, cond, key, dataKey, dataKeyValue, false, nil)
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
// -     若data.tags中含有设置的自增自增字段 或者 条件字段 数据key字段 会忽略set
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
	dataKey := key + "_" + c.dataKeyValue(dataInfo.Elemts[dataKeyIndex])

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
	// 保存到redis中
	t, err := c.mysqlToRedis(ctx, cond, key, dataKey, dataKeyValue)
	if err != nil {
		return nil, nil, err
	}
	return t, incrValue, nil
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
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有自增字段】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增自增字段 或者 条件字段 会忽略set
// nc：表示不存在是否创建
// 返回值：是T结构类型的指针(最新的值), 自增ID(如果新增数据 int64类型), error
func (c *CacheRows[T]) Set(ctx context.Context, cond TableConds, data interface{}, nc bool) (*T, interface{}, error) {
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
	dataKey := key + "_" + c.dataKeyValue(dataInfo.Elemts[dataKeyIndex])

	// 获取Redis参数
	redisParams := c.redisModifyParam1(cond, dataInfo, false)

	// 写数据
	dest, err := c.redisSetToMysql(ctx, cond, dataKeyValue, key, dataKey, redisParams, dataInfo)
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		if IsPass(dataKey) {
			return nil, nil, fmt.Errorf("%s is pass", key)
		}
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, cond, key, dataKey, dataKeyValue, nc, dataInfo)
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return preData, incrValue, nil
	}

	// 再次写数据
	dest, err = c.redisSetToMysql(ctx, cond, dataKeyValue, key, dataKey, redisParams, dataInfo)
	if err == nil {
		return dest, nil, nil
	} else {
		return nil, nil, err
	}
}

func (c *CacheRows[T]) redisSetToMysql(ctx context.Context, cond TableConds, dataKeyValue interface{}, key, dataKey string, redisParams []interface{}, dataInfo *utils.StructInfo) (*T, error) {
	cmd := c.redis.DoScript2(ctx, rowsModifyScript, []string{key, dataKey}, redisParams...)
	if cmd.Cmd.Err() == nil {
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByTag(dest, DBTag)
		err := cmd.BindValues(destInfo.Elemts)
		if err == nil {
			// 同步mysql，添加上数据key字段
			err := c.saveToMySQL(ctx, cond.Eq(c.dataKeyField, dataKeyValue), dataInfo)
			if err != nil {
				c.redis.Del(ctx, dataKey) // mysql错了 要删缓存
				return nil, err
			}
			return dest, nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, dataKey)
			return nil, err
		}
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return nil, ErrNullData
		}
		return nil, cmd.Cmd.Err()
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
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有自增字段】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增自增字段 或者 条件字段 会忽略set
// nc：表示不存在是否创建
// 返回值：如果有自增，返回自增ID interface是int64类型 =nil表示没有新增
func (c *CacheRows[T]) Set2(ctx context.Context, cond TableConds, data interface{}, nc bool) (interface{}, error) {
	if ctx.Value(utils.CtxKey_caller) == nil {
		ctx = context.WithValue(ctx, utils.CtxKey_caller, utils.GetCallerDesc(1))
	}

	// 检查条件变量
	key, err := c.checkCond(cond)
	if err != nil {
		return nil, err
	}

	// 检查data数据
	dataInfo, err := c.checkData(data)
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

	// 获取Redis参数
	redisParams := c.redisSetParam(cond, dataInfo)

	// 写数据
	err = c.redisSet2ToMysql(ctx, cond, dataKeyValue, key, dataKey, redisParams, dataInfo)
	if err == nil {
		return nil, nil
	} else if err == ErrNullData {
		if IsPass(dataKey) {
			return nil, fmt.Errorf("%s is pass", key)
		}
		// 不处理执行下面的预加载
	} else {
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	_, incrValue, err := c.preLoad(ctx, cond, key, dataKey, dataKeyValue, nc, dataInfo)
	if err != nil {
		return nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return incrValue, nil
	}

	// 再次写数据
	err = c.redisSet2ToMysql(ctx, cond, dataKeyValue, key, dataKey, redisParams, dataInfo)
	if err == nil {
		return incrValue, nil
	} else {
		return nil, err
	}
}

func (c *CacheRows[T]) redisSet2ToMysql(ctx context.Context, cond TableConds, dataKeyValue interface{}, key, dataKey string, redisParams []interface{}, dataInfo *utils.StructInfo) error {
	cmd := c.redis.DoScript2(ctx, rowsSetScript, []string{key, dataKey}, redisParams...)
	if cmd.Cmd.Err() == nil {
		// 同步mysql，添加上数据key字段
		err := c.saveToMySQL(ctx, cond.Eq(c.dataKeyField, dataKeyValue), dataInfo)
		if err != nil {
			c.redis.Del(ctx, dataKey) // mysql错了 要删缓存
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
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有自增字段】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增自增字段 或者 条件字段 会忽略掉modify
// nc：表示不存在是否创建
// 返回值：是data结构类型的数据，修改后的值
func (c *CacheRows[T]) Modify(ctx context.Context, cond TableConds, data interface{}, nc bool) (*T, interface{}, error) {
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

	// 获取Redis参数
	redisParams := c.redisModifyParam1(cond, dataInfo, true)

	// 写数据
	dest, err := c.redisModifyToMysql(ctx, cond, dataKeyValue, key, dataKey, redisParams, dataInfo)
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		if IsPass(dataKey) {
			return nil, nil, fmt.Errorf("%s is pass", key)
		}
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, cond, key, dataKey, dataKeyValue, nc, dataInfo)
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return preData, preData, nil
	}

	// 再次写数据
	dest, err = c.redisModifyToMysql(ctx, cond, dataKeyValue, key, dataKey, redisParams, dataInfo)
	if err == nil {
		return dest, nil, nil
	} else {
		return nil, nil, err
	}
}

func (c *CacheRows[T]) redisModifyToMysql(ctx context.Context, cond TableConds, dataKeyValue interface{}, key, dataKey string, redisParams []interface{}, dataInfo *utils.StructInfo) (*T, error) {
	cmd := c.redis.DoScript2(ctx, rowsModifyScript, []string{key, dataKey}, redisParams...)
	if cmd.Cmd.Err() == nil {
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByTag(dest, DBTag)
		err := cmd.BindValues(destInfo.Elemts)
		if err == nil {
			// 同步mysql，把T结构的值 拷贝到新创建的dataInfo.T中再保存
			t := reflect.New(dataInfo.T).Interface()
			tInfo, _ := utils.GetStructInfoByTag(t, DBTag)
			destInfo.CopyTo(tInfo)
			err := c.saveToMySQL(ctx, cond.Eq(c.dataKeyField, dataKeyValue), tInfo)
			if err != nil {
				c.redis.Del(ctx, dataKey) // mysql错了 要删缓存
				return nil, err
			}
			return dest, nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, dataKey)
			return nil, err
		}
	} else {
		if goredis.IsNilError(cmd.Cmd.Err()) {
			return nil, ErrNullData
		}
		return nil, cmd.Cmd.Err()
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
// -     可以是结构或者结构指针，内部的数据是要保存的数据 【内部必须要有自增字段】
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增自增字段 或者 条件字段 会忽略掉modify
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

	// 获取Redis参数
	redisParams := c.redisModifyParam2(cond, dataInfo, true)

	// 返回值
	res := reflect.New(dataInfo.T).Interface()
	resInfo, _ := utils.GetStructInfoByTag(res, DBTag)

	// 写数据
	err = c.redisModify2ToMysql(ctx, cond, dataKeyValue, key, dataKey, redisParams, resInfo)
	if err == nil {
		return res, nil, nil
	} else if err == ErrNullData {
		if IsPass(dataKey) {
			return nil, nil, fmt.Errorf("%s is pass", key)
		}
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, cond, key, dataKey, dataKeyValue, nc, dataInfo)
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
	err = c.redisModify2ToMysql(ctx, cond, dataKeyValue, key, dataKey, redisParams, resInfo)
	if err == nil {
		return res, nil, nil
	} else {
		return nil, nil, err
	}
}

func (c *CacheRows[T]) redisModify2ToMysql(ctx context.Context, cond TableConds, dataKeyValue interface{}, key, dataKey string, redisParams []interface{}, resInfo *utils.StructInfo) error {
	cmd := c.redis.DoScript2(ctx, rowsModifyScript, []string{key, dataKey}, redisParams...)
	if cmd.Cmd.Err() == nil {
		err := cmd.BindValues(resInfo.Elemts)
		if err == nil {
			// 同步mysql，添加上数据字段
			err := c.saveToMySQL(ctx, cond.Eq(c.dataKeyField, dataKeyValue), resInfo)
			if err != nil {
				c.redis.Del(ctx, dataKey) // mysql错了 要删缓存
				return err
			}
			return nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, dataKey)
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

	temp := cond
	if len(cond) > 1 {
		// 根据field排序，不要影响cond，拷贝一份
		temp = make(TableConds, len(cond))
		copy(temp, cond)
		sort.Slice(temp, func(i, j int) bool { return temp[i].field < temp[j].field })
	}

	var key strings.Builder
	key.WriteString(KeyPrefix + "mrrs_" + c.tableName)
	for _, v := range temp {
		if v.field == c.hashTagField {
			key.WriteString(fmt.Sprintf("_{%v}", v.value))
		} else {
			key.WriteString(fmt.Sprintf("_%v", v.value))
		}
	}
	return key.String(), nil
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
	t, err := c.getsFromMySQL(ctx, c.tableInfo.TS, c.tableInfo.Tags, cond)
	if err != nil {
		if err == ErrNullData {
			SetPass(key)
		}
		return nil, err
	}
	data := t.([]*T)

	// 保存到redis中
	dataKeys := []string{key}
	redisKV := make([][]interface{}, 0, 1+len(data))
	redisKV = append(redisKV, make([]interface{}, 0, 1+len(data))) // 第0个位置是总key数据
	redisKVNum := 0
	for _, d := range data {
		tInfo, _ := utils.GetStructInfoByTag(d, DBTag)
		dataKeyValue := utils.ValueFmt(tInfo.Elemts[c.dataKeyFieldIndex])
		if dataKeyValue == nil {
			continue // 理论上不应该 忽略这条数据
		}
		sli := make([]interface{}, 0, len(tInfo.Tags))
		for i, v := range tInfo.Elemts {
			vfmt := utils.ValueFmt(v)
			if vfmt == nil {
				continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
			}
			sli = append(sli, tInfo.Tags[i])
			sli = append(sli, vfmt)
		}
		k := key + "_" + fmt.Sprint(dataKeyValue)
		dataKeys = append(dataKeys, k)
		redisKV[0] = append(redisKV[0], dataKeyValue)
		redisKV[0] = append(redisKV[0], k)
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
		c.redis.Del(ctx, dataKeys...) // 失败了，删除主键
		return nil, cmd.Err()
	}
	return data, nil
}

// 预加载一条数据，确保写到Redis中
// key：主key
// dataKeyValue 这条数据的值
// nc 不存在就创建
// 返回值
// *T： 因为加载时抢占式的，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// interface{} != nil时 表示产生的自增id 创建时才返回
// error： 执行结果
func (c *CacheRows[T]) preLoad(ctx context.Context, cond TableConds, key, dataKey string, dataKeyValue interface{}, nc bool, dataInfo *utils.StructInfo) (*T, interface{}, error) {
	// 根据是否要创建数据，来判断使用什么锁
	if nc {
		// 不存在要创建数据
		unlock, err := c.redis.Lock(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
		if err != nil {
			return nil, nil, err
		}
		defer unlock()

		// 查询mysql是否存在这条数据
		_, err = c.getFromMySQL(ctx, c.tableInfo.T, c.tableInfo.Tags, cond.Eq(c.dataKeyField, dataKeyValue)) // 此处用get函数
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
		}

		// 加载
		t, err := c.mysqlToRedis(ctx, cond, key, dataKey, dataKeyValue)
		return t, incrValue, err
	} else {
		// 不用创建
		unlock, err := c.redis.TryLockWait(context.WithValue(context.TODO(), goredis.CtxKey_nolog, 1), "lock_"+key, time.Second*8)
		if err == nil && unlock != nil {
			defer unlock()
			// 加载
			t, err := c.mysqlToRedis(ctx, cond, key, dataKey, dataKeyValue)
			return t, nil, err
		}
		return nil, nil, nil
	}
}

func (c *CacheRows[T]) mysqlToRedis(ctx context.Context, cond TableConds, key, dataKey string, dataKeyValue interface{}) (*T, error) {
	// 先查询到要读取的数据
	t, err := c.getFromMySQL(ctx, c.tableInfo.T, c.tableInfo.Tags, cond.Eq(c.dataKeyField, dataKeyValue))
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

	dataKeys := []string{key, dataKey}
	data := t.(*T)
	allKeyData := allKeyT.([]*T)

	// 保存到redis中
	redisParams := make([]interface{}, 0, 1+1+len(allKeyData)*2+1+len(c.tableInfo.Tags)*2)
	redisParams = append(redisParams, c.expire)
	// 主key
	redisParams = append(redisParams, len(allKeyData)*2)
	for _, d := range allKeyData {
		tInfo, _ := utils.GetStructInfoByTag(d, DBTag)
		dataKeyValue := utils.ValueFmt(tInfo.Elemts[c.dataKeyFieldIndex])
		redisParams = append(redisParams, dataKeyValue)
		redisParams = append(redisParams, dataKey)
	}
	// 数据key
	tInfo, _ := utils.GetStructInfoByTag(data, DBTag)
	paramNumAt := len(redisParams) // 记录后面要写入数据个数的位置
	paramNum := 0
	redisParams = append(redisParams, &paramNum)
	for i, v := range tInfo.Elemts {
		vfmt := utils.ValueFmt(v)
		if vfmt == nil {
			continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
		}
		redisParams = append(redisParams, tInfo.Tags[i])
		redisParams = append(redisParams, vfmt)
		paramNum += 2
	}
	redisParams[paramNumAt] = paramNum

	cmd := c.redis.DoScript(ctx, rowsAddScript, dataKeys, redisParams...)
	if cmd.Err() != nil {
		c.redis.Del(ctx, dataKeys...) // 失败了，删除主键
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
		redisParams = append(redisParams, dataInfo.Tags[i])
		redisParams = append(redisParams, vfmt)
	}
	return redisParams
}
