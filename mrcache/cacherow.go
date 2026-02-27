package mrcache

// https://github.com/yuwf/gobase

import (
	"context"
	"reflect"
	"strings"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog"
)

// T 为数据库结构类型
// 使用场景：查询条件只对应一个结果 redis使用hash结构缓存这个结果
// redis：hash field和mysql的field对应，value使用goredis.ValueFmt格式化
type CacheRow[T any] struct {
	*Cache
}

// condFields：查询字段，不可为空， 条件具有唯一性，不唯一只能读取一条数据
func NewCacheRow[T any](redis *goredis.Redis, mysql *mysql.MySQL, tableName string, tableCount, tableIndex int, condFields []string) (*CacheRow[T], error) {
	cache, err := NewCache[T](redis, mysql, tableName, tableCount, tableIndex, condFields)
	if err != nil {
		return nil, err
	}
	cache.keyPrefix = "mrr"

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
		l.Interface(c.condFieldsLog, condValues).Interface("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRow %s Get", c.TableName())
	}, ErrNullData)()

	// 检查条件变量
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
		l.Interface(c.condFieldsLog, condValues).Interface("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRow %s GetFromRedis", c.TableName())
	}, ErrNullData)()

	// 检查条件变量
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
		l.Interface(c.condFieldsLog, condValues).Interface("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRow %s GetFromSQL", c.TableName())
	}, ErrNullData)()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}
	return c.get(ctx, key, condValues, false, true)
}

// redis: 是否从redis加载
func (c *CacheRow[T]) get(ctx context.Context, key string, condValues []interface{}, redis, proLoad bool) (_rst_ *T, _err_ error) {
	if redis {
		// 从Redis中读取
		redisParams := make([]interface{}, 0, 1+len(c.Tags))
		redisParams = append(redisParams, c.expire)
		redisParams = append(redisParams, c.RedisTagsInterface()...)

		dest := new(T)
		destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
		err := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowGetScript, []string{key}, redisParams...).BindValues(destInfo.Elemts)
		if err == nil {
			return dest, nil
		}
		// 如果后面不同sql中加载 如果有错直接返回
		if !proLoad && err != nil {
			return nil, err
		}
	}

	if proLoad {
		// 理执行下面的预加载
		preData, _, err := c.preLoad(ctx, key, condValues, nil)
		if err != nil {
			return nil, err
		}
		// 预加载的数据就是了
		if preData != nil {
			return preData, nil
		}

		// 如果不是自己执行的预加载，这里重新读取下，正常来说这种情况很少
		redisParams := make([]interface{}, 0, 1+len(c.Tags))
		redisParams = append(redisParams, c.expire)
		redisParams = append(redisParams, c.RedisTagsInterface()...)

		dest := new(T)
		destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
		err = c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowGetScript, []string{key}, redisParams...).BindValues(destInfo.Elemts)
		if err == nil {
			return dest, nil
		} else if goredis.IsNilError(err) {
			return nil, ErrNullData
		} else {
			return nil, err
		}
	}

	return nil, nil // 理论上不会走到这里了
}

// 读取数据
// condValuess：查询条件变量，要和condFields顺序和对应的类型一致
// 返回值：[]*T, _err_==nil时大小和condValuess一致
func (c *CacheRow[T]) Gets(ctx context.Context, condValuess ...[]interface{}) (_rst_ []*T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValuess).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRow %s Gets", c.TableName())
	})()

	for _, condValues := range condValuess {
		// 检查条件变量
		err := c.checkCondValues(condValues)
		if err != nil {
			return nil, err
		}
	}

	return c.gets(ctx, condValuess, true, true)
}

// 只读取Redis缓存数据，不同的CacheRow他们Redis一致，就可以使用此方法
// condValuess：查询条件变量，每一个要和condFields顺序和对应的类型一致
// 返回值：[]*T, _err_==nil时大小顺序和condValuess一致，不存在的填充nil
func (c *CacheRow[T]) GetMultiFromRedis(ctx context.Context, condValuess ...[]interface{}) (_rst_ []*T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValuess).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRow %s GetsFromRedis", c.TableName())
	})()

	for _, condValues := range condValuess {
		// 检查条件变量
		err := c.checkCondValues(condValues)
		if err != nil {
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
		l.Interface(c.condFieldsLog, condValuess).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRow %s GetsFromSQL", c.TableName())
	})()

	for _, condValues := range condValuess {
		// 检查条件变量
		err := c.checkCondValues(condValues)
		if err != nil {
			return nil, err
		}
	}

	return c.gets(ctx, condValuess, false, true)
}

// 读取数据
// condFieldValues：查询条件变量，field:value，强烈建议MySQL中要有这些字段的索引
// 每次先通过数据库查出条件来
// 返回值：[]*T
func (c *CacheRow[T]) GetsBySQLField(ctx context.Context, condFieldValues map[string]interface{}) (_rst_ []*T, _err_ error) {
	var condFields []string
	var condValues []interface{}
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface("["+strings.Join(condFields, ",")+"]", condValues).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRow %s GetsBySQLField", c.TableName())
	})()

	var err error
	condFields, condValues, err = c.checkFieldValues(condFieldValues)
	if err != nil {
		return nil, err
	}

	// 先读取条件字段所有的值
	t, err := c.getsFromMySQL(ctx, c.T, c.condFields, NewConds().eqs(condFields, condValues))
	if err != nil {
		return nil, err
	}
	datas := t.([]*T)
	if len(datas) == 0 {
		return make([]*T, 0), nil
	}

	condValuess := make([][]interface{}, 0, len(datas))
	for _, data := range datas {
		tInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
		condValues := make([]interface{}, 0, len(c.condFields))
		for i := 0; i < len(c.condFields); i++ {
			condValues = append(condValues, goredis.ValueFmt(tInfo.Elemts[c.condFieldsIndex[i]]))
		}
		condValuess = append(condValuess, condValues)
	}

	// 防止有空数据，有可能上面查询出索引，数据库因其他情况删除了数据
	res, err := c.gets(ctx, condValuess, true, true)
	res = utils.DeleteOrdered(res, nil)
	return res, err
}

// 返回值和condValuess大小顺序一致
// redis: 是否从redis加载
// proLoad: 是否从mysql加载
func (c *CacheRow[T]) gets(ctx context.Context, condValuess [][]interface{}, redis, proLoad bool) ([]*T, error) {
	if len(condValuess) == 0 {
		return make([]*T, 0), nil
	}

	// 查询结构
	type Cond[T any] struct {
		key  string
		cmd  *goredis.RedisCommond // Redis执行的命令
		data *T                    // 绑定的对象
	}
	conds := make([]*Cond[T], len(condValuess))
	for i, condValues := range condValuess {
		key := c.genCondValuesKey(condValues)
		cond := &Cond[T]{
			key: key,
		}
		conds[i] = cond
	}

	if redis {
		// redis中读取，key分布不一样，用管道读取
		pipeline := c.redis.NewPipeline()
		for i := range conds {
			redisParam := append([]interface{}{"hmget", conds[i].key}, c.RedisTagsInterface()...)
			conds[i].cmd = pipeline.Do2(goredis.CtxNonilErrIgnore(ctx), redisParam...)
		}
		_, err := pipeline.Exec(goredis.CtxNonilErrIgnore(ctx))
		if err == nil || goredis.IsNilError(err) {
			for _, cond := range conds {
				if cond.cmd.Cmd.Err() != nil {
					continue
				}
				dest := new(T)
				destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
				err := cond.cmd.BindValues(destInfo.Elemts)
				if err == nil {
					cond.data = dest
				}
			}
		}
		// 如果后面不同sql中加载 如果有错直接返回
		if !proLoad && err != nil {
			return nil, err
		}
	}

	if proLoad {
		// 未加载的数据 执行下面的预加载
		needload := map[string][]interface{}{}
		for i, cond := range conds {
			if cond.data == nil {
				needload[cond.key] = condValuess[i]
			}
		}
		if len(needload) > 0 {
			preDatas, err := c.preLoads(ctx, needload)
			if err != nil {
				return nil, err
			}
			for k, data := range preDatas {
				// 找到对应的cond
				for _, cond := range conds {
					if cond.key == k {
						cond.data = data
						break
					}
				}
			}
		}
	}

	// 收集结果
	res := make([]*T, len(condValuess))
	for i, cond := range conds {
		res[i] = cond.data
	}
	return res, nil
}

// 读取数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：是否存在
func (c *CacheRow[T]) Exist(ctx context.Context, condValues []interface{}) (_rst_ bool, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", _rst_).Msgf("CacheRow %s Exist", c.TableName())
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
		l.Interface(c.condFieldsLog, condValues).Interface("data", data).Err(_err_).Interface("rst", utils.TruncatedLog(_rst_)).Interface("incr", _incr_).Msgf("CacheRow %s Add", c.TableName())
	})()

	if dataM, ok := data.(map[string]interface{}); ok {
		// 检查data数据
		err := c.checkMapData(dataM)
		if err != nil {
			return nil, nil, err
		}
		return c.add(ctx, condValues, dataM, ops)
	} else {
		// 检查data数据
		dataInfo, err := c.checkStructData(data)
		if err != nil {
			return nil, nil, err
		}
		return c.add(ctx, condValues, dataInfo.TagElemsMap(), ops)
	}
}

func (c *CacheRow[T]) add(ctx context.Context, condValues []interface{}, data map[string]interface{}, ops *Options) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}

	incrValue, err := c.addToMySQL(ctx, condValues, data)
	if err != nil {
		return nil, nil, err
	}
	DelPass(key)

	nr := ops != nil && ops.noResp
	if nr {
		// 不需要返回值
		return nil, incrValue, nil
	}

	rst, err := c.get(ctx, key, condValues, false, true)
	if err != nil {
		return nil, nil, err
	}
	return rst, incrValue, nil
}

// 删除数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：error
func (c *CacheRow[T]) Del(ctx context.Context, condValues []interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Msgf("CacheRow %s Del", c.TableName())
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

	err = c.delToMySQL(ctx, NewConds().eqs(c.condFields, condValues)) // 删mysql
	if err != nil {
		return err
	}

	// 删除后 标记下数据pass
	SetPass(key)
	return nil
}

// 删除数据
// condValuess：查询条件变量，要和condFields顺序和对应的类型一致
// 返回值：error
func (c *CacheRow[T]) Dels(ctx context.Context, condValuess ...[]interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValuess).Err(_err_).Msgf("CacheRow %s DelsCache", c.TableName())
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

	cmd := c.redis.Del(ctx, keys...) // 删缓存
	if cmd.Err() != nil {
		return cmd.Err()
	}

	if cmd.Val() == 0 && GetPasss(keys) {
		return nil
	}

	err := c.delToMySQL(ctx, NewConds().Ins(c.condFields, condValuess...)) // 删mysql
	if err != nil {
		return err
	}

	// 删除后 标记下数据pass
	for _, key := range keys {
		SetPass(key)
	}
	return nil
}

// 删除数据
// condFieldValues：查询条件变量，field:value，强烈建议MySQL中要有这些字段的索引
// 每次先通过数据库查出条件来
// 返回值：error
func (c *CacheRow[T]) DelsBySQLField(ctx context.Context, condFieldValues map[string]interface{}) (_err_ error) {
	var condFields []string
	var condValues []interface{}
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface("["+strings.Join(condFields, ",")+"]", condValues).Err(_err_).Msgf("CacheRow %s DelsBySQLField", c.TableName())
	})()

	var err error
	condFields, condValues, err = c.checkFieldValues(condFieldValues)
	if err != nil {
		return err
	}

	cond := NewConds().eqs(condFields, condValues)

	// 先读取条件字段所有的值
	t, err := c.getsFromMySQL(ctx, c.T, c.condFields, cond)
	if err != nil {
		return err
	}
	datas := t.([]*T)
	if len(datas) == 0 {
		return nil
	}

	var keys []string
	for _, data := range datas {
		tInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
		condValues := make([]interface{}, 0, len(c.condFields))
		for i := 0; i < len(c.condFields); i++ {
			condValues = append(condValues, goredis.ValueFmt(tInfo.Elemts[c.condFieldsIndex[i]]))
		}
		keys = append(keys, c.genCondValuesKey(condValues))
	}

	cmd := c.redis.Del(ctx, keys...) // 删缓存
	if cmd.Err() != nil {
		return cmd.Err()
	}
	err = c.delToMySQL(ctx, cond) // 删mysql
	if err != nil {
		return err
	}
	return nil
}

// 只Cache删除数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：error
func (c *CacheRow[T]) DelCache(ctx context.Context, condValues []interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Msgf("CacheRow %s DelCache", c.TableName())
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
		l.Interface(c.condFieldsLog, condValuess).Err(_err_).Msgf("CacheRow %s DelsCache", c.TableName())
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

	cmd := c.redis.Del(ctx, keys...) // 删缓存
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

// 只Cache删除数据
// condFieldValues：查询条件变量，field:value，强烈建议MySQL中要有这些字段的索引
// 每次先通过数据库查出条件来
// 返回值：error
func (c *CacheRow[T]) DelsCacheBySQLField(ctx context.Context, condFieldValues map[string]interface{}) (_err_ error) {
	var condFields []string
	var condValues []interface{}
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface("["+strings.Join(condFields, ",")+"]", condValues).Err(_err_).Msgf("CacheRow %s DelsCacheBySQLField", c.TableName())
	})()

	var err error
	condFields, condValues, err = c.checkFieldValues(condFieldValues)
	if err != nil {
		return err
	}

	// 先读取条件字段所有的值
	t, err := c.getsFromMySQL(ctx, c.T, c.condFields, NewConds().eqs(condFields, condValues))
	if err != nil {
		return err
	}
	datas := t.([]*T)
	if len(datas) == 0 {
		return nil
	}

	var keys []string
	for _, data := range datas {
		tInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
		condValues := make([]interface{}, 0, len(c.condFields))
		for i := 0; i < len(c.condFields); i++ {
			condValues = append(condValues, goredis.ValueFmt(tInfo.Elemts[c.condFieldsIndex[i]]))
		}
		keys = append(keys, c.genCondValuesKey(condValues))
	}

	cmd := c.redis.Del(ctx, keys...) // 删缓存
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
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
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", utils.TruncatedLog(_rst_)).Interface("incr", _incr_).Msgf("CacheRow %s Set", c.TableName())
	})()

	if dataM, ok := data.(map[string]interface{}); ok {
		// 检查data数据
		err := c.checkMapData(dataM)
		if err != nil {
			return nil, nil, err
		}
		return c.set(ctx, condValues, dataM, ops)
	} else {
		// 检查data数据
		dataInfo, err := c.checkStructData(data)
		if err != nil {
			return nil, nil, err
		}
		return c.set(ctx, condValues, dataInfo.TagElemsMap(), ops)
	}
}

func (c *CacheRow[T]) set(ctx context.Context, condValues []interface{}, data map[string]interface{}, ops *Options) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}
	nr := ops != nil && ops.noResp

	var dest *T
	if nr {
		err = c.setSave(ctx, key, condValues, data)
	} else {
		dest, err = c.setGetTSave(ctx, key, condValues, data)
	}
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, key, condValues, utils.If(ops != nil && ops.noExistCreate, data, nil))
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		if nr {
			return nil, incrValue, nil
		}
		return preData, incrValue, nil
	}

	// 再次写数据
	if nr {
		err = c.setSave(ctx, key, condValues, data)
	} else {
		dest, err = c.setGetTSave(ctx, key, condValues, data)
	}
	if err == nil {
		return dest, nil, nil
	} else {
		return nil, nil, err
	}
}

// 没有返回值
func (c *CacheRow[T]) setSave(ctx context.Context, key string, condValues []interface{}, data map[string]interface{}) error {
	// 加锁
	unlock, err := c.saveLock(ctx, key)
	if err != nil {
		return err
	}
	mysqlUnlock := false
	defer func() {
		if !mysqlUnlock {
			unlock()
		}
	}()

	// redis参数
	redisParams := c.redisSetParam("null", data)
	cmd := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowModifyScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		// 同步mysql
		mysqlUnlock = true
		err := c.saveToMySQL(ctx, NewConds().eqs(c.condFields, condValues), data, key, func(err error) {
			defer unlock()
			if err != nil {
				c.redis.Del(ctx, key) // mysql错了 要删缓存
			}
		})
		if err != nil {
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
func (c *CacheRow[T]) setGetTSave(ctx context.Context, key string, condValues []interface{}, data map[string]interface{}) (*T, error) {
	// 加锁
	unlock, err := c.saveLock(ctx, key)
	if err != nil {
		return nil, err
	}
	mysqlUnlock := false
	defer func() {
		if !mysqlUnlock {
			unlock()
		}
	}()

	// redis参数
	redisParams := c.redisSetGetParam("null", c.Tags, data, false)
	cmd := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowModifyGetScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
		err := cmd.BindValues(destInfo.Elemts)
		if err == nil {
			// 同步mysql
			mysqlUnlock = true
			err := c.saveToMySQL(ctx, NewConds().eqs(c.condFields, condValues), data, key, func(err error) {
				defer unlock()
				if err != nil {
					c.redis.Del(ctx, key) // mysql错了 要删缓存
				}
			})
			if err != nil {
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
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", utils.TruncatedLog(_rst_)).Interface("incr", _incr_).Msgf("CacheRow %s Modify", c.TableName())
	})()

	if dataM, ok := data.(map[string]interface{}); ok {
		// 检查data数据
		err := c.checkMapData(dataM)
		if err != nil {
			return nil, nil, err
		}
		return c.modify(ctx, condValues, dataM, ops)
	} else {
		// 检查data数据
		dataInfo, err := c.checkStructData(data)
		if err != nil {
			return nil, nil, err
		}
		return c.modify(ctx, condValues, dataInfo.TagElemsMap(), ops)
	}
}

func (c *CacheRow[T]) modify(ctx context.Context, condValues []interface{}, data map[string]interface{}, ops *Options) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}
	nr := ops != nil && ops.noResp

	// 按c.Tags的顺序构造 有顺序
	modifydata := &ModifyData{data: data}
	for i, tag := range c.Tags {
		if v, ok := data[tag]; ok {
			modifydata.tags = append(modifydata.tags, tag)
			modifydata.values = append(modifydata.values, v)
			// 填充c.Fields的类型
			modifydata.rsts = append(modifydata.rsts, reflect.New(c.Fields[i].Type).Elem())
		}
	}

	var dest *T
	if nr {
		err = c.modifyGetSave(ctx, key, condValues, modifydata)
	} else {
		dest, err = c.modifyGetTSave(ctx, key, condValues, modifydata)
	}
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, key, condValues, utils.If(ops != nil && ops.noExistCreate, data, nil))
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		if nr {
			return nil, incrValue, nil
		}
		return preData, incrValue, nil
	}

	// 再次写数据，这种情况很少
	if nr {
		err = c.modifyGetSave(ctx, key, condValues, modifydata)
	} else {
		dest, err = c.modifyGetTSave(ctx, key, condValues, modifydata)
	}
	if err == nil {
		return dest, nil, nil
	} else {
		return nil, nil, err
	}
}

// 增量修改数据 返回的类型和data一致，填充修改后的值
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
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", _rst_).Interface("incr", _incr_).Msgf("CacheRow %s Modify2", c.TableName())
	})()

	if dataM, ok := data.(map[string]interface{}); ok {
		// 检查data数据
		err := c.checkMapData(dataM)
		if err != nil {
			return nil, nil, err
		}

		// 按c.Tags的顺序构造 有顺序
		modifydata := &ModifyData{data: dataM}
		for i, tag := range c.Tags {
			if v, ok := dataM[tag]; ok {
				modifydata.tags = append(modifydata.tags, tag)
				modifydata.values = append(modifydata.values, v)
				// 填充map的值类型，如v=nil 在map中是没有类型的，用c.Fields的
				if v != nil {
					modifydata.rsts = append(modifydata.rsts, reflect.New(reflect.TypeOf(v)).Elem())
				} else {
					modifydata.rsts = append(modifydata.rsts, reflect.New(c.Fields[i].Type).Elem())
				}
			}
		}

		incrValue, err := c.modify2(ctx, condValues, dataM, modifydata, ops)
		if err != nil {
			return nil, nil, err
		}

		return modifydata.TagsRstsMap(), incrValue, nil
	} else {
		// 检查data数据
		dataInfo, err := c.checkStructData(data)
		if err != nil {
			return nil, nil, err
		}

		// 修改后的值
		res := reflect.New(dataInfo.T).Interface()
		resInfo, _ := utils.GetStructInfoByTag(res, DBTag)

		dataM := dataInfo.TagElemsMap()

		// 按c.Tags的顺序构造 有顺序
		modifydata := &ModifyData{data: dataM}
		for _, tag := range c.Tags {
			if v, ok := dataM[tag]; ok {
				modifydata.tags = append(modifydata.tags, tag)
				modifydata.values = append(modifydata.values, v)
				// 填充res对应字段
				modifydata.rsts = append(modifydata.rsts, resInfo.Elemts[resInfo.FindIndexByTag(tag)])
			}
		}

		incrValue, err := c.modify2(ctx, condValues, dataM, modifydata, ops)
		if err != nil {
			return nil, nil, err
		}

		return res, incrValue, nil
	}
}

func (c *CacheRow[T]) modify2(ctx context.Context, condValues []interface{}, data map[string]interface{}, modifydata *ModifyData, ops *Options) (_incr_ interface{}, _err_ error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 写数据
	err = c.modifyGetSave(ctx, key, condValues, modifydata)
	if err == nil {
		return nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, key, condValues, utils.If(ops != nil && ops.noExistCreate, data, nil))
	if err != nil {
		return nil, err
	}
	// 返回了自增，数据添加已经完成了，从预加载数据中拷贝返回值
	if incrValue != nil {
		dInfo, _ := utils.GetStructInfoByStructType(preData, c.StructType)
		modifydata.RstsFrom(dInfo)
		return incrValue, nil
	}

	// 再次写数据
	err = c.modifyGetSave(ctx, key, condValues, modifydata)
	if err == nil {
		return nil, nil
	} else {
		return nil, err
	}
}

// 不要返回值
// 会填充modifydata.rsts
func (c *CacheRow[T]) modifyGetSave(ctx context.Context, key string, condValues []interface{}, modifydata *ModifyData) error {
	// 加锁
	unlock, err := c.saveLock(ctx, key)
	if err != nil {
		return err
	}
	mysqlUnlock := false
	defer func() {
		if !mysqlUnlock {
			unlock()
		}
	}()

	// redis参数
	redisParams := c.redisSetGetParam("null", modifydata.tags, modifydata.data, true)
	cmd := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowModifyGetScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		err := cmd.BindValues(modifydata.rsts)
		if err == nil {
			// 同步mysql
			mysqlUnlock = true
			err = c.saveToMySQL(ctx, NewConds().eqs(c.condFields, condValues), modifydata.TagsRstsMap(), key, func(err error) {
				defer unlock()
				if err != nil {
					c.redis.Del(ctx, key) // mysql错了 要删缓存
				}
			})
			if err != nil {
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

// 返回值*T
// 会填充modifydata.rsts
func (c *CacheRow[T]) modifyGetTSave(ctx context.Context, key string, condValues []interface{}, modifydata *ModifyData) (*T, error) {
	// 加锁
	unlock, err := c.saveLock(ctx, key)
	if err != nil {
		return nil, err
	}
	mysqlUnlock := false
	defer func() {
		if !mysqlUnlock {
			unlock()
		}
	}()

	// redis参数
	redisParams := c.redisSetGetParam("null", c.Tags, modifydata.data, true)
	cmd := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowModifyGetScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
		err := cmd.BindValues(destInfo.Elemts)
		if err == nil {
			// 同步mysql，把T结构的值 拷贝到新创建的resInfo中再保存，否则会保存整个T结构
			modifydata.RstsFrom(destInfo)
			mysqlUnlock = true
			err = c.saveToMySQL(ctx, NewConds().eqs(c.condFields, condValues), modifydata.TagsRstsMap(), key, func(err error) {
				defer unlock()
				if err != nil {
					c.redis.Del(ctx, key) // mysql错了 要删缓存
				}
			})
			if err != nil {
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

func (c *CacheRow[T]) preLoad(ctx context.Context, key string, condValues []interface{}, ncData map[string]interface{}) (*T, interface{}, error) {
	if ncData == nil {
		// 如果不想创建 又设置了pass
		if GetPass(key) {
			return nil, nil, ErrNullData
		}
	}
	// 加锁
	unlock, err := c.preLoadLock(ctx, key)
	if err != nil || unlock == nil {
		return nil, nil, err
	}
	defer unlock()

	var incrValue interface{}
	cond := NewConds().eqs(c.condFields, condValues)

	// 查询到要读取的数据
	t, err := c.getFromMySQL(ctx, c.T, c.Tags, cond)
	if ncData != nil {
		// 不存在要创建数据
		if err != nil {
			if err == ErrNullData {
				// 创建数据
				incrValue, err = c.addToMySQL(ctx, condValues, ncData)
				if err != nil {
					return nil, nil, err
				}
				DelPass(key)
				// 重新加载下
				t, err = c.getFromMySQL(ctx, c.T, c.Tags, cond)
				if err != nil {
					return nil, nil, err
				}
			} else {
				return nil, nil, err
			}
		}
	} else {
		// 不用创建
		if err != nil {
			if err == ErrNullData {
				SetPass(key)
			}
			return nil, nil, err
		}
	}
	data := t.(*T)

	err = c.addToRedis(ctx, key, data)
	if err != nil {
		return nil, nil, err
	}

	return data, incrValue, nil
}

// 预加载 确保写到Redis中再返回
// 返回值
// map[string]*T, 如果加锁失败后，会从Redis中直接读取下最新的，这个是和preLoad不一样的地方
// error： 执行结果
func (c *CacheRow[T]) preLoads(ctx context.Context, condValuess map[string][]interface{}) (map[string]*T, error) {
	// 先判断是否设置了pass
	queryCondValuess := make(map[string][]interface{}, len(condValuess))
	for k, v := range condValuess {
		if GetPass(k) {
		} else {
			queryCondValuess[k] = v
		}
	}
	if len(queryCondValuess) == 0 {
		return make(map[string]*T, 0), nil
	}

	condValuess2 := make([][]interface{}, 0, len(queryCondValuess))
	for _, v := range queryCondValuess {
		condValuess2 = append(condValuess2, v)
	}
	t, err := c.getsFromMySQL(ctx, c.T, c.Tags, NewConds().Ins(c.condFields, condValuess2...))
	if err != nil {
		return nil, err
	}
	datas := t.([]*T)

	rst := make(map[string]*T)
	// 考虑到存入redis要加锁，所以这里只好逐条添加到redis
	for _, data := range datas {
		tInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
		condValues := make([]interface{}, 0, len(c.condFields))
		for i := 0; i < len(c.condFields); i++ {
			condValues = append(condValues, goredis.ValueFmt(tInfo.Elemts[c.condFieldsIndex[i]]))
		}
		key := c.genCondValuesKey(condValues)

		preData, _ := c.preToRedis(ctx, key, data)
		if preData != nil {
			rst[key] = data
		} else {
			// 重新读取下
			redisParams := make([]interface{}, 0, 1+len(c.Tags))
			redisParams = append(redisParams, c.expire)
			redisParams = append(redisParams, c.RedisTagsInterface()...)

			dest := new(T)
			destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType) // 这里不用判断err了
			err := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowGetScript, []string{key}, redisParams...).BindValues(destInfo.Elemts)
			if err == nil {
				rst[key] = data
			} else if !goredis.IsNilError(err) {
				return nil, err
			}
		}
	}
	for k := range queryCondValuess {
		if _, ok := rst[k]; !ok {
			SetPass(k)
		}
	}
	return rst, nil
}

// 如果存Redis成功了，存储成功了，返回自己
// *T： 因为preLoadLock是加锁失败等待，!= nil 表示是本逻辑执行了加载，否则没有执行加载，外出需要重新操作下
func (c *CacheRow[T]) preToRedis(ctx context.Context, key string, data *T) (*T, error) {
	// 加锁
	unlock, err := c.preLoadLock(ctx, key)
	if err != nil || unlock == nil {
		return nil, err
	}
	defer unlock()

	err = c.addToRedis(ctx, key, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *CacheRow[T]) addToRedis(ctx context.Context, key string, data *T) error {
	// 加锁
	tInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
	redisParams := make([]interface{}, 0, 1+2*len(tInfo.Tags))
	redisParams = append(redisParams, c.expire)
	for i, v := range tInfo.Elemts {
		vfmt := goredis.ValueFmt(v)
		if vfmt == nil {
			continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
		}
		redisParams = append(redisParams, c.RedisTags[i])
		redisParams = append(redisParams, vfmt)
	}
	cmd := c.redis.DoScript(ctx, rowAddScript, []string{key}, redisParams...)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}
