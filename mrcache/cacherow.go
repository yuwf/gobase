package mrcache

// https://github.com/yuwf/gobase

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog"
)

// T 为数据库结构类型
// 使用场景：condFields查询条件具有唯一性，不唯一只能读取一条数据
// redis使用hash结构缓存这个结果, hash的field和mysql的field对应，value使用goredis.ValueToRedisArg格式化
// T<->MySQL T结构整体支持sql.Scanner和driver.Valuer接口，但程序会读写T结构的部分字段
// T<->Reids T结构整体不支持RedisUnmarshaler和RedisMarshaler接口，但T结构中每个字段可以实现RedisUnmarshaler和RedisMarshaler接口
type CacheRow[T any] struct {
	*Cache
}

// condFields：查询条件字段
func NewCacheRow[T any](redis *goredis.Redis, mysql *mysql.MySQL, tableName string, condFields []string, opts *CacheOptions) (*CacheRow[T], error) {
	opts.KeyPrefix = strings.TrimSpace(opts.KeyPrefix)
	opts.KeySuffix = strings.TrimSpace(opts.KeySuffix)
	if len(opts.KeyPrefix) == 0 {
		opts.KeyPrefix = "mrr"
	}
	cache, err := NewCache[T](redis, mysql, tableName, condFields, opts)
	if err != nil {
		return nil, err
	}

	cache.msgHeadLog = fmt.Sprintf("CacheRow %s", cache.tableName)
	c := &CacheRow[T]{
		Cache: cache,
	}
	return c, nil
}

// 读取数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：是T结构类型的指针
func (c *CacheRow[T]) Get(ctx context.Context, condValues []interface{}) (_rst_ *T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		if _err_ != nil && _err_ != ErrNullData {
			l.Err(_err_)
		}
		l.Interface(c.condFieldsLog, condValues).Interface("rst", utils.TruncatedLog(_rst_)).Msgf("%s Get", c.msgHeadLog)
	}, ErrNullData)()

	// 检查条件变量 生成key
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}
	return c.get(ctx, key, condValues, true, true)
}

// 只读取Redis缓存数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：是T结构类型的指针
func (c *CacheRow[T]) GetFromRedis(ctx context.Context, condValues []interface{}) (_rst_ *T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		if _err_ != nil && _err_ != ErrNullData {
			l.Err(_err_)
		}
		l.Interface(c.condFieldsLog, condValues).Interface("rst", utils.TruncatedLog(_rst_)).Msgf("%s GetFromRedis", c.msgHeadLog)
	}, ErrNullData)()

	// 检查条件变量 生成key
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}
	return c.get(ctx, key, condValues, true, false)
}

// 直接从数据库读取数据，外层可以结合GetsFromRedis
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：是T结构类型的指针
func (c *CacheRow[T]) GetFromSQL(ctx context.Context, condValues []interface{}) (_rst_ *T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		if _err_ != nil && _err_ != ErrNullData {
			l.Err(_err_)
		}
		l.Interface(c.condFieldsLog, condValues).Interface("rst", utils.TruncatedLog(_rst_)).Msgf("%s GetFromSQL", c.msgHeadLog)
	}, ErrNullData)()

	// 检查条件变量 生成key
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}
	return c.get(ctx, key, condValues, false, true)
}

// 读取数据
// condValuess：查询条件变量，要和condFields顺序和对应的类型一致
// 返回值：[]*T, _err_==nil时大小和condValuess一致
func (c *CacheRow[T]) Gets(ctx context.Context, condValuess ...[]interface{}) (_rst_ []*T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValuess).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("%s Gets", c.msgHeadLog)
	})()

	// 检查条件变量
	for _, condValues := range condValuess {
		if err := c.checkCondValues(condValues); err != nil {
			return nil, err
		}
	}
	return c.gets(ctx, condValuess, true, true)
}

// 只读取Redis缓存数据，不同的CacheRow他们Redis一致，也可以使用此方法
// condValuess：查询条件变量，每一个要和condFields顺序和对应的类型一致
// 返回值：[]*T, _err_==nil时大小顺序和condValuess一致，不存在的填充nil
func (c *CacheRow[T]) GetsFromRedis(ctx context.Context, condValuess ...[]interface{}) (_rst_ []*T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValuess).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("%s GetsFromRedis", c.msgHeadLog)
	})()

	// 检查条件变量
	for _, condValues := range condValuess {
		if err := c.checkCondValues(condValues); err != nil {
			return nil, err
		}
	}
	return c.gets(ctx, condValuess, true, false)
}

// 直接从数据库数据读取，外层可以结合GetsFromRedis使用优化性能
// condValuess：查询条件变量，要和condFields顺序和对应的类型一致
// 返回值：[]*T, _err_==nil时大小和condValuess一致
func (c *CacheRow[T]) GetsFromSQL(ctx context.Context, condValuess ...[]interface{}) (_rst_ []*T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValuess).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("%s GetsFromSQL", c.msgHeadLog)
	})()

	// 检查条件变量
	for _, condValues := range condValuess {
		if err := c.checkCondValues(condValues); err != nil {
			return nil, err
		}
	}
	return c.gets(ctx, condValuess, false, true)
}

// 从MySQL中读取条件值
// 强烈建议cond中的条件在MySQL中设计成覆盖索引，否则会很慢
// 返回值：[][]interface{}，每个元素是condFields对应的值
func (c *CacheRow[T]) GetCondValuessByQuery(ctx context.Context, cond TableConds) (_rst_ [][]interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Str("cond", cond.Log()).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("%s GetCondValuessByQuery", c.msgHeadLog)
	})()

	return c.getCondValuesFromMySQL(ctx, cond)
}

// 查询数据 每次先通过数据库查出条件来
// 强烈建议cond中的条件在MySQL中设计成覆盖索引，否则会很慢
// 返回值：[]*T
func (c *CacheRow[T]) GetByQuery(ctx context.Context, cond TableConds) (_rst_ []*T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Str("cond", cond.Log()).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("%s GetByQuery", c.msgHeadLog)
	})()

	// 先读取条件字段所有的值
	condValuess, err := c.getCondValuesFromMySQL(ctx, cond)
	if err != nil {
		return nil, err
	}
	if len(condValuess) == 0 {
		return make([]*T, 0), nil
	}

	res, err := c.gets(ctx, condValuess, true, true)
	// 防止有空数据，有可能上面查询出索引，数据库因其他情况删除了数据
	res = utils.DeleteOrdered(res, nil)
	return res, err
}

// 读取数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：是否存在
func (c *CacheRow[T]) Exist(ctx context.Context, condValues []interface{}) (_rst_ bool, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", _rst_).Msgf("%s Exist", c.msgHeadLog)
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return false, err
	}

	// 从Redis中读取
	rstV, err := c.redis.Exists(ctx, key).Result()
	if err == nil {
		if rstV == 1 {
			return true, nil
		}
	}

	// 其他情况不处理执行下面的预加载
	preData, _, err := c.preLoad(ctx, key, condValues, nil)
	if err != nil {
		if err == ErrNullData { // 空数据直接返回false
			return false, nil
		}
		return false, err
	}
	// 有预加载数据，说明存在
	if preData != nil {
		return true, nil
	}

	// 如果不是自己执行的预加载，这里重新读取下
	rstV, err = c.redis.Exists(ctx, key).Result()
	if err == nil {
		if rstV == 1 {
			return true, nil
		}
		return false, nil
	} else {
		return false, err
	}
}

// 添加数据，需要外部已经确保没有数据了调用此函数，直接添加数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// data：修改内容
// -     可以是【结构】或者【结构指针】或者【map[string]interface{}】
// -     结构中tag或者map中的filed的名称需要和T一致，可以是T的一部分
// -     若其中含有设置的自增字段、condFields，该字段不会写入
// 返回值
// _rst_ ： 是T结构类型的指针，修改后的值，设置ops.NoResp()时不返回值 优化性能
// _incr_： 自增ID，int64类型
// _err_ ： 操作失败
func (c *CacheRow[T]) Add(ctx context.Context, condValues []interface{}, data interface{}, ops *Options) (_rst_ *T, _incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("data", data).Err(_err_).Interface("rst", utils.TruncatedLog(_rst_)).Interface("incr", _incr_).Msgf("%s Add", c.msgHeadLog)
	})()

	if dataM, ok := data.(map[string]interface{}); ok {
		// 检查data数据
		modifyData, err := c.checkMapData(dataM)
		if err != nil {
			return nil, nil, err
		}
		return c.add(ctx, condValues, modifyData, ops)
	} else {
		// 检查data数据
		_, modifyData, err := c.checkStructData(data)
		if err != nil {
			return nil, nil, err
		}
		return c.add(ctx, condValues, modifyData, ops)
	}
}

// 删除数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：error
func (c *CacheRow[T]) Del(ctx context.Context, condValues []interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Msgf("%s Del", c.msgHeadLog)
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	cmd := c.redis.Del(ctx, key) // 删缓存
	if cmd.Err() != nil {
		return cmd.Err()
	}

	if cmd.Val() == 0 && GetPass(key) {
		return nil
	}

	err = c.delToMySQL(ctx, NewConds().Eqs(c.condFields, condValues)) // 删mysql
	if err != nil {
		return err
	}

	// 再延迟删除一次Redis
	antsPool.Submit(func() {
		c.redis.Del(ctx, key)
	})

	// 删除后 标记下数据pass
	SetPass(key)
	return nil
}

// 删除数据
// condValuess：查询条件变量，要和condFields顺序和对应的类型一致
// 返回值：error
func (c *CacheRow[T]) Dels(ctx context.Context, condValuess ...[]interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValuess).Err(_err_).Msgf("%s Dels", c.msgHeadLog)
	})()

	if len(condValuess) == 0 {
		return nil
	}

	var keys []string
	for _, condValues := range condValuess {
		// 检查条件变量
		key, err := c.checkCondValuesGenKey(condValues)
		if err != nil {
			return err
		}
		keys = append(keys, key)
	}

	return c.dels(ctx, keys, condValuess, true)
}

// 删除数据，每次先通过数据库查出条件来
// 强烈建议cond中的条件在MySQL中设计成覆盖索引，否则会很慢
// 返回值：error
func (c *CacheRow[T]) DelByQuery(ctx context.Context, cond TableConds) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Str("cond", cond.Log()).Err(_err_).Msgf("%s DelByQuery", c.msgHeadLog)
	})()

	// 先读取条件字段所有的值
	condValuess, err := c.getCondValuesFromMySQL(ctx, cond)
	if err != nil {
		return err
	}
	if len(condValuess) == 0 {
		return nil
	}
	var keys []string
	for _, condValues := range condValuess {
		keys = append(keys, c.genCondValuesKey(condValues))
	}

	return c.dels(ctx, keys, condValuess, true)
}

// 只Cache删除数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：error
func (c *CacheRow[T]) DelCache(ctx context.Context, condValues []interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Msgf("%s DelCache", c.msgHeadLog)
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	cmd := c.redis.Del(ctx, key) // 删缓存
	if cmd.Err() != nil {
		return cmd.Err()
	}

	return nil
}

// 只Cache删除数据
// condValuess：多个查询条件变量 每个condFields对应的值 顺序和对应的类型要一致
// 返回值：error
func (c *CacheRow[T]) DelsCache(ctx context.Context, condValuess ...[]interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValuess).Err(_err_).Msgf("%s DelsCache", c.msgHeadLog)
	})()

	var keys []string
	for _, condValues := range condValuess {
		// 检查条件变量
		key, err := c.checkCondValuesGenKey(condValues)
		if err != nil {
			return err
		}
		keys = append(keys, key)
	}

	return c.dels(ctx, keys, condValuess, false)
}

// 只删除Cache数据，每次先通过数据库查出条件来
// 强烈建议cond中的条件在MySQL中设计成覆盖索引，否则会很慢
// 返回值：error
func (c *CacheRow[T]) DelCacheByQuery(ctx context.Context, cond TableConds) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Str("cond", cond.Log()).Err(_err_).Msgf("%s DelCacheByQuery", c.msgHeadLog)
	})()

	// 先读取条件字段所有的值
	condValuess, err := c.getCondValuesFromMySQL(ctx, cond)
	if err != nil {
		return err
	}
	if len(condValuess) == 0 {
		return nil
	}
	var keys []string
	for _, condValues := range condValuess {
		keys = append(keys, c.genCondValuesKey(condValues))
	}

	return c.dels(ctx, keys, condValuess, false)
}

// 写数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// data：修改内容
// -     可以是【结构】或者【结构指针】或者【map[string]interface{}】
// -     结构中tag或者map中的filed的名称需要和T一致，可以是T的一部分
// -     若其中含有设置的自增字段、condFields，该字段不会修改
// 返回值
// _rst_ ： 是T结构类型的指针，修改后的值，设置ops.NoResp()时不返回值 优化性能
// _incr_： 自增ID，设置ops.Create()时如果新增才返回此值，int64类型
// _err_ ： 操作失败
func (c *CacheRow[T]) Set(ctx context.Context, condValues []interface{}, data interface{}, ops *Options) (_rst_ *T, _incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", utils.TruncatedLog(_rst_)).Interface("incr", _incr_).Msgf("%s Set", c.msgHeadLog)
	})()

	if dataM, ok := data.(map[string]interface{}); ok {
		// 检查data数据
		modifyData, err := c.checkMapData(dataM)
		if err != nil {
			return nil, nil, err
		}
		return c.set(ctx, condValues, modifyData, ops)
	} else {
		// 检查data数据
		_, modifyData, err := c.checkStructData(data)
		if err != nil {
			return nil, nil, err
		}
		return c.set(ctx, condValues, modifyData, ops)
	}
}

// 增量修改数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// data：修改内容
// -     可以是【结构】或者【结构指针】或者【map[string]interface{}】
// -     结构中tag或者map中的filed的名称需要和T一致，可以是T的一部分
// -     若其中含有设置的自增字段、condFields，该字段不会修改
// 返回值
// _rst_ ： 是T结构类型的指针，修改后的值，设置ops.NoResp()时不返回值 优化性能
// _incr_： 自增ID，设置ops.Create()时如果新增才返回此值，int64类型
// _err_ ： 操作失败
func (c *CacheRow[T]) Modify(ctx context.Context, condValues []interface{}, data interface{}, ops *Options) (_rst_ *T, _incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", utils.TruncatedLog(_rst_)).Interface("incr", _incr_).Msgf("%s Modify", c.msgHeadLog)
	})()

	if dataM, ok := data.(map[string]interface{}); ok {
		// 检查data数据
		modifyData, err := c.checkMapData(dataM)
		if err != nil {
			return nil, nil, err
		}
		return c.modify(ctx, condValues, modifyData, ops)
	} else {
		// 检查data数据
		_, modifyData, err := c.checkStructData(data)
		if err != nil {
			return nil, nil, err
		}
		return c.modify(ctx, condValues, modifyData, ops)
	}
}

// 增量修改数据 一定有返回值 返回的类型和data一致，填充修改后的值
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// data：修改内容
// -     可以是【结构】或者【结构指针】或者【map[string]interface{}】
// -     结构中tag或者map中的filed的名称需要和T一致，可以是T的一部分
// -     若其中含有设置的自增字段、condFields，该字段不会修改
// 返回值
// _rst_ ： 是data结构类型的指针，修改后的值
// _incr_： 自增ID，设置ops.Create()时如果新增才返回此值，int64类型
// _err_ ： 操作失败
func (c *CacheRow[T]) Modify2(ctx context.Context, condValues []interface{}, data interface{}, ops *Options) (_rst_ interface{}, _incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", _rst_).Interface("incr", _incr_).Msgf("%s Modify2", c.msgHeadLog)
	})()

	if dataM, ok := data.(map[string]interface{}); ok {
		// 检查data数据
		modifyData, err := c.checkMapData(dataM)
		if err != nil {
			return nil, nil, err
		}
		modifyData.RstMake(modifyData.tags)
		incrValue, err := c.modify2(ctx, condValues, modifyData, ops)
		if err != nil {
			return nil, nil, err
		}
		return modifyData.RstToMap(), incrValue, nil
	} else {
		// 检查data数据
		dataInfo, modifyData, err := c.checkStructData(data)
		if err != nil {
			return nil, nil, err
		}
		// 修改后的值
		dest := reflect.New(dataInfo.T).Interface()
		modifyData.RstMakeByT(dest, dataInfo.StructType)
		incrValue, err := c.modify2(ctx, condValues, modifyData, ops)
		if err != nil {
			return nil, nil, err
		}
		return dest, incrValue, nil
	}
}

// 字段jsonarray类型的添加数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// fieldValues：字段：添加的值列表, 鉴于redis的lua无法支持到int64和mysql的JSON_SEARCH不支持数字类型的查找
// 返回值
// _incr_： 自增ID, 设置ops.Create()时如果新增才返回此值，int64类型
// _err_ ： 操作失败
// mysql语句无法拼接处多个值添加避免重复能功能，所以这个接口是运行元素重复的
func (c *CacheRow[T]) JsonArrayAdd(ctx context.Context, condValues []interface{}, fieldValues map[string][]string, ops *Options) (_incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("fieldValues", fieldValues).Err(_err_).Msgf("%s JsonArrayAdd", c.msgHeadLog)
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 检查fieldValues类型是否OK
	err = c.checkJsonArrayFieldValues(fieldValues)
	if err != nil {
		return nil, err
	}

	// 修改
	err = c.jsonArraySave(ctx, key, condValues, fieldValues, nil, ops)
	if err == nil {
		return nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, err
	}
	// 预加载 尝试从数据库中读取
	var data map[string]interface{}
	if ops != nil && ops.noExistCreate {
		data = map[string]interface{}{}
		for tag, values := range fieldValues {
			data[tag], _ = json.Marshal(values)
		}
	}
	_, incrValue, err := c.preLoad(ctx, key, condValues, data)
	if err != nil {
		return nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return incrValue, nil
	}

	// 再次写数据
	err = c.jsonArraySave(ctx, key, condValues, fieldValues, nil, ops)
	if err == nil {
		return nil, nil
	} else {
		return nil, err
	}
}

// 字段jsonarray类型的删除数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// fieldValues：字段：删除的值列表, 鉴于redis的lua无法支持到int64和mysql的JSON_SEARCH不支持数字类型的查找
// 返回值
// _err_ ： 操作失败
func (c *CacheRow[T]) JsonArrayDel(ctx context.Context, condValues []interface{}, fieldValues map[string][]string, ops *Options) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("fieldValues", fieldValues).Err(_err_).Msgf("%s JsonArrayDel", c.msgHeadLog)
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	// 检查fieldValues类型是否OK
	err = c.checkJsonArrayFieldValues(fieldValues)
	if err != nil {
		return err
	}

	// 修改
	err = c.jsonArraySave(ctx, key, condValues, nil, fieldValues, ops)
	if err == nil {
		return nil
	} else if err == ErrNullData {
		if GetPass(key) {
			return nil
		}
		// 不处理执行下面的预加载
	} else {
		return err
	}
	_, _, err = c.preLoad(ctx, key, condValues, nil)
	if err != nil {
		if err == ErrNullData {
			return nil
		}
		return err
	}

	// 再次写数据
	err = c.jsonArraySave(ctx, key, condValues, nil, fieldValues, ops)
	if err == nil {
		return nil
	} else if err == ErrNullData {
		return nil // 数据项不存在 del操作返回nil
	} else {
		return err
	}
}
