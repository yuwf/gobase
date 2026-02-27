package mrcache

// https://github.com/yuwf/gobase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/mysql"
	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog"
)

// T 为数据库结构类型
// 使用场景：查询条件condFields对应多个结果 每个结果用keyFields来唯一定位
// condFields 和 keyFields 数据类型只能为基本的数据类型，condFields和keyFields对应的数据要唯一，最好有唯一索引
// 索引key：存储所有的keyValuesStr，set结构
// keyValuesStr: keyValues格式化的字符串值，多个keyValue用:连接
// dataKey：存储一条mysql数据，存储方式和CacheRow一样，dataKey命名：key_keyValuesStr
// 使用数据时，都判断数据是否存在，不存在就尝试加载下，加载时判断是否设置了pass，防止击穿到mysql
type CacheRows[T any] struct {
	*Cache
}

// condFields：查询字段，不可为空， 查询的数据有多条，每条数据可以用唯一的keyFields来定位
func NewCacheRows[T any](redis *goredis.Redis, mysql *mysql.MySQL, tableName string, tableCount, tableIndex int, condFields []string, keyFields []string) (*CacheRows[T], error) {
	cache, err := NewCache[T](redis, mysql, tableName, tableCount, tableIndex, condFields)
	if err != nil {
		return nil, err
	}
	cache.keyPrefix = "mrrs"

	if len(keyFields) == 0 {
		return nil, fmt.Errorf("dataKeyFields can not empty in %s", cache.T.String())
	}
	// 验证dataKeyField 只能是基本的数据int 和 string 类型
	var keyFields_ []string
	var keyFieldsIndex_ []int
	for _, f := range keyFields {
		idx := cache.FindIndexByTag(f)
		if idx == -1 {
			err := fmt.Errorf("tag:%s not find in %s", f, cache.T.String())
			return nil, err
		}
		// 只能是基本的数据int 和 string 类型
		if !cache.IsBaseType(cache.Fields[idx].Type) {
			err := fmt.Errorf("tag:%s(%s) as keyFields type error", f, cache.T.String())
			return nil, err
		}
		keyFields_ = append(keyFields_, f)
		keyFieldsIndex_ = append(keyFieldsIndex_, idx)
	}
	cache.keyFields = keyFields_
	cache.keyFieldsIndex = keyFieldsIndex_
	cache.keyFieldsLog = "[" + strings.Join(keyFields_, ",") + "]"

	c := &CacheRows[T]{
		Cache: cache,
	}
	return c, nil
}

// dataKey是有 索引key和keyValuesStr组合成的
func (c *CacheRows[T]) genDataKey(key, keyValuesStr string) string {
	return key + "_" + keyValuesStr
}

func (c *CacheRows[T]) genKeyValuesStrByTInfo(dataInfo *utils.StructValue) string {
	var keyValues []interface{}
	for _, index := range c.keyFieldsIndex {
		keyValues = append(keyValues, dataInfo.Elemts[index].Interface())
	}
	return c.genKeyValuesStr(keyValues)
}

// 读取符合condValues的全部数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：是T结构类型的指针列表
func (c *CacheRows[T]) GetAll(ctx context.Context, condValues []interface{}) (_rst_ []*T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRows %s GetAll", c.TableName())
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 从Redis中读取
	dest, err := c.redisGetAll(ctx, key)
	if err == nil {
		return dest, nil
	}

	// 其他情况不处理执行下面的预加载
	preData, err := c.preLoadAll(ctx, key, condValues)
	if err != nil {
		return nil, err
	}
	if preData != nil { // 执行了预加载
		return preData, nil
	}

	// 如果不是自己执行的预加载，这里重新读取下
	dest, err = c.redisGetAll(ctx, key)
	if err == nil {
		return dest, nil
	} else if err == ErrNullData {
		return make([]*T, 0), nil
	} else {
		return nil, err
	}
}

// 只读取Redis缓存数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：是T结构类型的指针列表
func (c *CacheRows[T]) GetAllFromRedis(ctx context.Context, condValues []interface{}) (_rst_ []*T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRows %s GetAllFromRedis", c.TableName())
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 从Redis中读取
	dest, err := c.redisGetAll(ctx, key)
	if err == nil {
		return dest, nil
	}

	return nil, err
}

// 读取符合condValues的全部数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：是T结构类型的指针列表
func (c *CacheRows[T]) GetAllFromSQL(ctx context.Context, condValues []interface{}) (_rst_ []*T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRows %s GetAllFromSQL", c.TableName())
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 其他情况不处理执行下面的预加载
	preData, err := c.preLoadAll(ctx, key, condValues)
	if err != nil {
		return nil, err
	}
	if preData != nil { // 执行了预加载
		return preData, nil
	}

	// 如果不是自己执行的预加载，这里重新读取下
	dest, err := c.redisGetAll(ctx, key)
	if err == nil {
		return dest, nil
	} else if err == ErrNullData {
		return make([]*T, 0), nil
	} else {
		return nil, err
	}
}

// 只读取Redis缓存数据 读取符合condValues的全部数据，不同的CacheRows他们Redis一致，就可以使用此方法，这个函数没有对应的GetAllMultiFromSQL，外部可以配合GetAllFromSQL使用
// condValuess：查询条件变量，每一个要和condFields顺序和对应的类型一致
// 返回值：[][]*T, _err_==nil时大小顺序和condValuess一致，不存在的填充nil
func (c *CacheRows[T]) GetAllMultiFromRedis(ctx context.Context, condValuess ...[]interface{}) (_rst_ [][]*T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValuess).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRows %s GetAllMultiFromRedis", c.TableName())
	})()

	keys := []string{}
	for _, condValues := range condValuess {
		// 检查条件变量
		key, err := c.checkCondValuesGenKey(condValues)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}

	redisParams := make([]interface{}, 0, 1+len(c.Tags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, c.RedisTagsInterface()...)

	// 查询结构
	type Cond[T any] struct {
		key  string
		cmd  *goredis.RedisCommond // Redis执行的命令
		data []*T                  // 绑定的对象
	}
	conds := make([]*Cond[T], 0, len(condValuess))

	pipeline := c.redis.NewPipeline()
	for i, _ := range condValuess {
		cond := &Cond[T]{
			key: keys[i],
			cmd: pipeline.Script2(goredis.CtxNonilErrIgnore(ctx), rowsGetAllScript, []string{keys[i]}, redisParams...),
		}
		conds = append(conds, cond)
	}
	_, err := pipeline.ExecNoNil(goredis.CtxNonilErrIgnore(ctx))
	if err == nil || goredis.IsNilError(err) {
		for _, cond := range conds {
			if cond.cmd.Cmd.Err() != nil {
				continue
			}
			reply := make([][]interface{}, 0)
			err := cond.cmd.BindSlice(&reply)
			var elemetErr error
			if err == nil {
				res := make([]*T, 0, len(reply))
				for _, r := range reply {
					dest := new(T)
					destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType) // 这里不用判断err了
					for i, v := range r {
						if v == nil {
							continue
						}
						err := goredis.InterfaceToValue(v, destInfo.Elemts[i])
						if err != nil {
							elemetErr = err
							break
						}
					}
					res = append(res, dest)
				}
				if elemetErr == nil {
					cond.data = res
				}
			}
		}
	}

	// 收集结果
	res := make([][]*T, len(condValuess))
	for i, cond := range conds {
		res[i] = cond.data
	}
	return res, nil
}

// 读取符合condValues的部分数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// dataKeyValues 填充dataKeyField类型的值，要查询的值
// 返回值：是T结构类型的指针列表
func (c *CacheRows[T]) Gets(ctx context.Context, condValues []interface{}, keyValuess [][]interface{}) (_rst_ []*T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface(c.keyFieldsLog, keyValuess).Err(_err_).Array("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRows %s Gets", c.TableName())
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	return c.gets(ctx, key, condValues, keyValuess)
}

func (c *CacheRows[T]) gets(ctx context.Context, key string, condValues []interface{}, keyValuess [][]interface{}) (_rst_ []*T, _err_ error) {
	if len(keyValuess) == 0 {
		return make([]*T, 0), nil
	}

	keyValuesStrs := make([]string, 0, len(keyValuess))
	// 检测keyValuess的类型和值
	for _, keyValues := range keyValuess {
		keyVauesStr, err := c.checkKeyValuesGenStr(keyValues)
		if err != nil {
			return nil, err
		}
		keyValuesStrs = append(keyValuesStrs, keyVauesStr)
	}

	// 从Redis中读取
	dest, err := c.redisGets(ctx, key, keyValuesStrs)
	if err == nil {
		return dest, nil
	}

	// 其他情况不处理执行下面的预加载
	preData, err := c.preLoads(ctx, key, condValues, keyValuesStrs, keyValuess)
	if err != nil {
		return nil, err
	}
	if preData != nil { // 执行了预加载
		return preData, nil
	}

	// 如果不是自己执行的预加载，这里必须递归调用，因为不知道别的preLoads有没有加载自己的dataKeyValues
	return c.gets(ctx, key, condValues, keyValuess)
}

// 读取一条数据 会返回空错误
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// dataKeyValue：数据key的值
// 返回值：是T结构类型的指针列表
func (c *CacheRows[T]) Get(ctx context.Context, condValues []interface{}, keyValues []interface{}) (_rst_ *T, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		if _err_ != nil && _err_ != ErrNullData {
			l.Err(_err_)
		}
		l.Interface(c.condFieldsLog, condValues).Interface(c.keyFieldsLog, keyValues).Interface("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRows %s Get", c.TableName())
	}, ErrNullData)()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 数据key的类型和值
	keyValuesStr, err := c.checkKeyValuesGenStr(keyValues)
	if err != nil {
		return nil, err
	}

	return c.get(ctx, key, condValues, keyValuesStr, keyValues, false)
}

// 第一步是否直接跳过Redis加载
func (c *CacheRows[T]) get(ctx context.Context, key string, condValues []interface{}, keyValuesStr string, keyValues []interface{}, ingnoreRedis bool) (_rst_ *T, _err_ error) {
	// 从Redis中读取
	if !ingnoreRedis {
		redisParams := make([]interface{}, 0, 2+len(c.Tags))
		redisParams = append(redisParams, c.expire)
		redisParams = append(redisParams, keyValuesStr)
		redisParams = append(redisParams, c.RedisTagsInterface()...)

		cmd := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowsGetScript, []string{key}, redisParams...)
		if cmd.Cmd.Err() == nil {
			dest := new(T)
			destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
			err := cmd.BindValues(destInfo.Elemts)
			if err == nil {
				return dest, nil
			}
		}
	}

	// 其他情况不处理执行下面的预加载
	preData, _, err := c.preLoad(ctx, key, condValues, keyValuesStr, keyValues, nil)
	if err != nil {
		return nil, err
	}
	// 预加载的数据就是新增的
	if preData != nil {
		return preData, nil
	}

	// 如果不是自己执行的预加载，这里重新读取下，正常来说这种情况很少
	redisParams := make([]interface{}, 0, 2+len(c.Tags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, keyValuesStr)
	redisParams = append(redisParams, c.RedisTagsInterface()...)

	cmd := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowsGetScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
		err := cmd.BindValues(destInfo.Elemts)
		if err == nil {
			return dest, nil
		} else {
			return nil, err
		}
	} else if goredis.IsNilError(cmd.Cmd.Err()) {
		return nil, ErrNullData
	} else {
		return nil, cmd.Cmd.Err()
	}
}

// 读取一条数据 会返回空错误
// condFieldValues：查询条件变量，field:value，数据要有唯一性，强烈建议MySQL中要有这些字段的索引
// 每次先通过数据库查出条件来
// 返回值：是T结构类型的指针列表
func (c *CacheRows[T]) GetBySQLField(ctx context.Context, condFieldValues map[string]interface{}) (_rst_ *T, _err_ error) {
	var condFields []string
	var condValues []interface{}
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface("["+strings.Join(condFields, ",")+"]", condValues).Err(_err_).Interface("rst", utils.TruncatedLog(_rst_)).Msgf("CacheRows %s GetBySQLField", c.TableName())
	})()

	var err error
	condFields, condValues, err = c.checkFieldValues(condFieldValues)
	if err != nil {
		return nil, err
	}

	// 先读取条件字段和key字段所有的值
	fields := append(c.condFields, c.keyFields...)
	t, err := c.getFromMySQL(ctx, c.T, fields, NewConds().eqs(condFields, condValues))
	if err != nil {
		return nil, err
	}
	data := t.(*T)

	// 解析出condValues和keyValues
	tInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
	condValues_ := make([]interface{}, 0, len(c.condFields))
	for i := 0; i < len(c.condFields); i++ {
		condValues_ = append(condValues_, goredis.ValueFmt(tInfo.Elemts[c.condFieldsIndex[i]]))
	}
	keyValues_ := make([]interface{}, 0, len(c.keyFields))
	for i := 0; i < len(c.keyFields); i++ {
		keyValues_ = append(keyValues_, goredis.ValueFmt(tInfo.Elemts[c.keyFieldsIndex[i]]))
	}

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues_)
	if err != nil {
		return nil, err
	}

	// 数据key的类型和值
	keyValuesStr, err := c.checkKeyValuesGenStr(keyValues_)
	if err != nil {
		return nil, err
	}

	return c.get(ctx, key, condValues_, keyValuesStr, keyValues_, false)
}

// 读取数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// keyValues：keyFields对应的值 顺序和对应的类型要一致
// 返回值：是否存在
func (c *CacheRows[T]) Exist(ctx context.Context, condValues []interface{}, keyValues []interface{}) (_rst_ bool, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface(c.keyFieldsLog, keyValues).Err(_err_).Interface("rst", _rst_).Msgf("CacheRows %s Exist", c.TableName())
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return false, err
	}

	// 数据key的类型和值
	keyValuesStr, err := c.checkKeyValuesGenStr(keyValues)
	if err != nil {
		return false, err
	}

	// 从Redis中读取
	redisParams := make([]interface{}, 0, 2)
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, keyValuesStr)

	rstV := 0
	err = c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowsExistScript, []string{key}, redisParams...).Bind(&rstV)
	if err == nil {
		if rstV == 1 {
			return true, nil
		}
		return false, nil
	}

	// 其他情况不处理执行下面的预加载
	preData, _, err := c.preLoad(ctx, key, condValues, keyValuesStr, keyValues, nil)
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
	err = c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowsExistScript, []string{key}, redisParams...).Bind(&rstV)
	if err == nil {
		if rstV == 1 {
			return true, nil
		}
		return false, nil
	} else if goredis.IsNilError(err) {
		return false, nil
	}
	return false, err
}

// 添加数据，需要外部已经确保没有数据了调用此函数，直接添加数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// data：修改内容
// -     可以是【结构】或者【结构指针】或者【map[string]interface{}】，内部必须要有keyFields字段
// -     结构中tag或者map中的filed的名称需要和T一致，可以是T的一部分
// -     若其中含有设置的自增字段、condFields、keyFields，该字段不会写入
// 返回值
// _rst_ ： 是T结构类型的指针，修改后的值，设置ops.NoResp()时不返回值 优化性能
// _incr_： 自增ID，int64类型
// _err_ ： 操作失败
func (c *CacheRows[T]) Add(ctx context.Context, condValues []interface{}, data interface{}, ops *Options) (_rst_ *T, _incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("data", data).Err(_err_).Interface("rst", utils.TruncatedLog(_rst_)).Interface("incr", _incr_).Msgf("CacheRows %s Add", c.TableName())
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

func (c *CacheRows[T]) add(ctx context.Context, condValues []interface{}, data map[string]interface{}, ops *Options) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}

	// 判断data中是否含有keyFields字段
	keyValuesStr, keyValues, err := c.checkKeyValuesGenStrByMap(data)
	if err != nil {
		return nil, nil, err
	}

	incrValue, err := c.addToMySQL(ctx, condValues, data)
	if err != nil {
		return nil, nil, err
	}
	DelPass(key)
	DelPass(c.genDataKey(key, keyValuesStr))

	// 添加后 不用考虑缓存，缓存的逻辑是不存在时都会重新读下数据库

	nr := ops != nil && ops.noResp
	if nr {
		// 不需要返回值
		return nil, incrValue, nil
	}

	rst, err := c.get(ctx, key, condValues, keyValuesStr, keyValues, true)
	if err != nil {
		return nil, nil, err
	}
	return rst, incrValue, nil
}

// 删除全部数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：error
func (c *CacheRows[T]) DelAll(ctx context.Context, condValues []interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Msgf("CacheRows %s DelAll", c.TableName())
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	cmd := c.redis.DoScript(ctx, rowsDelAllScript, []string{key})
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
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// keyValues：keyFields对应的值 顺序和对应的类型要一致
// 返回值：error
func (c *CacheRows[T]) Del(ctx context.Context, condValues []interface{}, keyValues []interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface(c.keyFieldsLog, keyValues).Err(_err_).Msgf("CacheRows %s Del", c.TableName())
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	// 数据key的类型和值
	keyValuesStr, err := c.checkKeyValuesGenStr(keyValues)
	if err != nil {
		return err
	}
	dataKey := c.genDataKey(key, keyValuesStr)

	cmd := c.redis.DoScript(ctx, rowsDelsScript, []string{key}, keyValuesStr)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	if cmd.Val() == 0 && (GetPass(key) || GetPass(dataKey)) {
		return nil
	}

	err = c.delToMySQL(ctx, NewConds().eqs(c.condFields, condValues).eqs(c.keyFields, keyValues)) // 删mysql
	if err != nil {
		return err
	}

	// 删除后 标记下数据pass
	SetPass(dataKey)
	return nil
}

// 删除数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// keyValues：keyFields对应的值 顺序和对应的类型要一致
// 返回值：error
func (c *CacheRows[T]) Dels(ctx context.Context, condValues []interface{}, keyValuess [][]interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface(c.keyFieldsLog, keyValuess).Err(_err_).Msgf("CacheRows %s Del", c.TableName())
	})()

	if len(keyValuess) == 0 {
		return
	}

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	keyValuesStrs := make([]interface{}, 0, len(keyValuess))
	dataKeys := make([]string, 0, len(keyValuess))
	// 检测keyValuess的类型和值
	for _, keyValues := range keyValuess {
		keyValuesStr, err := c.checkKeyValuesGenStr(keyValues)
		if err != nil {
			return err
		}
		keyValuesStrs = append(keyValuesStrs, keyValuesStr)
		dataKeys = append(dataKeys, c.genDataKey(key, keyValuesStr))
	}

	cmd := c.redis.DoScript(ctx, rowsDelsScript, []string{key}, keyValuesStrs...)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	if cmd.Val() == 0 && (GetPass(key) || GetPasss(dataKeys)) {
		return nil
	}

	err = c.delToMySQL(ctx, NewConds().eqs(c.condFields, condValues).Ins(c.keyFields, keyValuess...)) // 删mysql
	if err != nil {
		return err
	}

	// 删除后 标记下数据pass
	for _, key := range dataKeys {
		SetPass(key)
	}
	return nil
}

// 只Cache删除数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：error
func (c *CacheRows[T]) DelAllCache(ctx context.Context, condValues []interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Msgf("CacheRows %s DelAllCache", c.TableName())
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	cmd := c.redis.DoScript(ctx, rowsDelAllScript, []string{key})
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

// 只Cache删除数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// keyValues：keyFields对应的值 顺序和对应的类型要一致
// 返回值：error
func (c *CacheRows[T]) DelCache(ctx context.Context, condValues []interface{}, keyValues []interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface(c.keyFieldsLog, keyValues).Err(_err_).Msgf("CacheRows %s DelCache", c.TableName())
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	// 数据key的类型和值
	keyValuesStr, err := c.checkKeyValuesGenStr(keyValues)
	if err != nil {
		return err
	}

	cmd := c.redis.DoScript(ctx, rowsDelsScript, []string{key}, keyValuesStr)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

// 写数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// data：修改内容
// -     可以是【结构】或者【结构指针】或者【map[string]interface{}】，内部必须要有keyFields字段
// -     结构中tag或者map中的filed的名称需要和T一致，可以是T的一部分
// -     若其中含有设置的自增字段、condFields、keyFields，该字段不会修改
// 返回值
// _rst_ ： 是T结构类型的指针，修改后的值，设置ops.NoResp()时不返回值 优化性能
// _incr_： 自增ID，设置ops.Create()时如果新增才返回此值，int64类型
// _err_ ： 操作失败
func (c *CacheRows[T]) Set(ctx context.Context, condValues []interface{}, data interface{}, ops *Options) (_rst_ *T, _incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("data", data).Err(_err_).Interface("rst", utils.TruncatedLog(_rst_)).Interface("incr", _incr_).Msgf("CacheRows %s Set", c.TableName())
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

func (c *CacheRows[T]) set(ctx context.Context, condValues []interface{}, data map[string]interface{}, ops *Options) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}

	// 判断data中是否含有keyFields字段
	keyValuesStr, keyValues, err := c.checkKeyValuesGenStrByMap(data)
	if err != nil {
		return nil, nil, err
	}
	nr := ops != nil && ops.noResp

	var dest *T
	if nr {
		err = c.setSave(ctx, key, condValues, keyValuesStr, keyValues, data)
	} else {
		dest, err = c.setGetTSave(ctx, key, condValues, keyValuesStr, keyValues, data)
	}
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}
	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, key, condValues, keyValuesStr, keyValues, utils.If(ops != nil && ops.noExistCreate, data, nil))
	if err != nil {
		return nil, nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return preData, incrValue, nil
	}

	// 再次写数据
	if nr {
		err = c.setSave(ctx, key, condValues, keyValuesStr, keyValues, data)
	} else {
		dest, err = c.setGetTSave(ctx, key, condValues, keyValuesStr, keyValues, data)
	}
	if err == nil {
		return dest, nil, nil
	} else {
		return nil, nil, err
	}
}

// 没有返回值
func (c *CacheRows[T]) setSave(ctx context.Context, key string, condValues []interface{}, keyValuesStr string, keyValues []interface{}, data map[string]interface{}) error {
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

	// 获取Redis参数
	redisParams := c.redisSetParam(keyValuesStr, data)
	cmd := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowsModifyScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		// 同步mysql，添加上数据key字段
		mysqlUnlock = true
		err := c.saveToMySQL(ctx, NewConds().eqs(c.condFields, condValues).eqs(c.keyFields, keyValues), data, key, func(err error) {
			defer unlock()
			if err != nil {
				c.redis.Del(ctx, c.genDataKey(key, keyValuesStr)) // mysql错了 删除数据键
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
func (c *CacheRows[T]) setGetTSave(ctx context.Context, key string, condValues []interface{}, keyValuesStr string, keyValues []interface{}, data map[string]interface{}) (*T, error) {
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

	// 获取Redis参数
	redisParams := c.redisSetGetParam(keyValuesStr, c.Tags, data, false)
	cmd := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowsModifyGetScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
		err := cmd.BindValues(destInfo.Elemts)
		if err == nil {
			// 同步mysql，添加上数据key字段
			mysqlUnlock = true
			err := c.saveToMySQL(ctx, NewConds().eqs(c.condFields, condValues).eqs(c.keyFields, keyValues), data, key, func(err error) {
				defer unlock()
				if err != nil {
					c.redis.Del(ctx, c.genDataKey(key, keyValuesStr)) // mysql错了 删除数据键
				}
			})
			if err != nil {
				return nil, err
			}
			return dest, nil
		} else {
			// 绑定失败 redis中的数据和mysql不一致了，删除key返回错误
			c.redis.Del(ctx, c.genDataKey(key, keyValuesStr)) // 删除数据键
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
// -     可以是【结构】或者【结构指针】或者【map[string]interface{}】，内部必须要有keyFields字段
// -     结构中tag或者map中的filed的名称需要和T一致，可以是T的一部分
// -     若其中含有设置的自增字段、condFields、keyFields，该字段不会修改
// 返回值
// _rst_ ： 是T结构类型的指针，修改后的值，设置ops.NoResp()时不返回值 优化性能
// _incr_： 自增ID，设置ops.Create()时如果新增才返回此值，int64类型
// _err_ ： 操作失败
func (c *CacheRows[T]) Modify(ctx context.Context, condValues []interface{}, data interface{}, ops *Options) (_rst_ *T, _incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("data", data).Err(_err_).Interface("rst", utils.TruncatedLog(_rst_)).Interface("incr", _incr_).Msgf("CacheRows %s Modify", c.TableName())
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

func (c *CacheRows[T]) modify(ctx context.Context, condValues []interface{}, data map[string]interface{}, ops *Options) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}
	// 判断data中是否含有keyFields字段
	keyValuesStr, keyValues, err := c.checkKeyValuesGenStrByMap(data)
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
		err = c.modifyGetSave(ctx, key, condValues, keyValuesStr, keyValues, modifydata)
	} else {
		dest, err = c.modifyGetTSave(ctx, key, condValues, keyValuesStr, keyValues, modifydata)
	}
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, key, condValues, keyValuesStr, keyValues, utils.If(ops != nil && ops.noExistCreate, data, nil))
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
		err = c.modifyGetSave(ctx, key, condValues, keyValuesStr, keyValues, modifydata)
	} else {
		dest, err = c.modifyGetTSave(ctx, key, condValues, keyValuesStr, keyValues, modifydata)
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
// -     可以是【结构】或者【结构指针】或者【map[string]interface{}】，内部必须要有keyFields字段
// -     结构中tag或者map中的filed的名称需要和T一致，可以是T的一部分
// -     若其中含有设置的自增字段、condFields、keyFields，该字段不会修改
// 返回值
// _rst_ ： 是data结构类型的指针，修改后的值
// _incr_： 自增ID，设置ops.Create()时如果新增才返回此值，int64类型
// _err_ ： 操作失败
func (c *CacheRows[T]) Modify2(ctx context.Context, condValues []interface{}, data interface{}, ops *Options) (_rst_ interface{}, _incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", _rst_).Interface("incr", _incr_).Msgf("CacheRows %s Modify2", c.TableName())
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

func (c *CacheRows[T]) modify2(ctx context.Context, condValues []interface{}, data map[string]interface{}, modifydata *ModifyData, ops *Options) (interface{}, error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 判断data中是否含有keyFields字段
	keyValuesStr, keyValues, err := c.checkKeyValuesGenStrByMap(data)
	if err != nil {
		return nil, err
	}

	// 写数据
	err = c.modifyGetSave(ctx, key, condValues, keyValuesStr, keyValues, modifydata)
	if err == nil {
		return nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, key, condValues, keyValuesStr, keyValues, utils.If(ops != nil && ops.noExistCreate, data, nil))
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
	err = c.modifyGetSave(ctx, key, condValues, keyValuesStr, keyValues, modifydata)
	if err == nil {
		return nil, nil
	} else {
		return nil, err
	}
}

// 不要返回值
// 会填充modifydata.rsts
func (c *CacheRows[T]) modifyGetSave(ctx context.Context, key string, condValues []interface{}, keyValuesStr string, keyValues []interface{}, modifydata *ModifyData) error {
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

	// 获取Redis参数
	redisParams := c.redisSetGetParam(keyValuesStr, modifydata.tags, modifydata.data, true)
	cmd := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowsModifyGetScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		err := cmd.BindValues(modifydata.rsts)
		if err == nil {
			// 同步mysql
			mysqlUnlock = true
			err = c.saveToMySQL(ctx, NewConds().eqs(c.condFields, condValues).eqs(c.keyFields, keyValues), modifydata.TagsRstsMap(), key, func(err error) {
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
			// 绑定失败 redis中的数据和mysql不一致了，删除dataKey返回错误
			c.redis.Del(ctx, c.genDataKey(key, keyValuesStr))
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
func (c *CacheRows[T]) modifyGetTSave(ctx context.Context, key string, condValues []interface{}, keyValuesStr string, keyValues []interface{}, modifydata *ModifyData) (*T, error) {
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
	redisParams := c.redisSetGetParam(keyValuesStr, c.Tags, modifydata.data, true)
	cmd := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowsModifyGetScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
		err := cmd.BindValues(destInfo.Elemts)
		if err == nil {
			// 同步mysql，把T结构的值 拷贝到新创建的resInfo中再保存，否则会保存整个T结构
			modifydata.RstsFrom(destInfo)
			mysqlUnlock = true
			err = c.saveToMySQL(ctx, NewConds().eqs(c.condFields, condValues).eqs(c.keyFields, keyValues), modifydata.TagsRstsMap(), key, func(err error) {
				defer unlock()
				if err != nil {
					c.redis.Del(ctx, c.genDataKey(key, keyValuesStr)) // mysql错了 要删缓存
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

// 预加载所有数据，确保写到Redis中 不会返回空错误
// key：主key
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值
// []*T： 因为preLoadLock是加锁失败等待，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// error： 执行结果
func (c *CacheRows[T]) preLoadAll(ctx context.Context, key string, condValues []interface{}) ([]*T, error) {
	// 先判断是否设置了pass
	if GetPass(key) {
		return make([]*T, 0), nil
	}
	// 加锁
	unlock, err := c.preLoadLock(ctx, key)
	if err != nil || unlock == nil {
		return nil, err
	}
	defer unlock()

	cond := NewConds().eqs(c.condFields, condValues)
	// 加载
	all, err := c.getsFromMySQL(ctx, c.T, c.Tags, cond)
	if err != nil {
		return nil, err
	}
	allData := all.([]*T)
	if len(allData) == 0 {
		SetPass(key)
		return allData, nil
	}

	// 记录redis相关的key，后面删除使用
	allKeys := make([]string, 0, 1+len(allData))
	allKeys = append(allKeys, key) // 第一个key是数据索引

	// redis参数
	redisParams := make([]interface{}, 0, 1+len(allData)*(2+2*len(c.Tags)))
	redisParams = append(redisParams, c.expire)
	for _, data := range allData {
		dataInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
		sli := make([]interface{}, 0, 2*len(dataInfo.Tags))
		for i, v := range dataInfo.Elemts {
			vfmt := goredis.ValueFmt(v)
			if vfmt == nil {
				continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
			}
			sli = append(sli, c.RedisTags[i])
			sli = append(sli, vfmt)
		}
		keyValuesStr := c.genKeyValuesStrByTInfo(dataInfo)
		allKeys = append(allKeys, c.genDataKey(key, keyValuesStr))
		redisParams = append(redisParams, keyValuesStr)
		redisParams = append(redisParams, len(sli))
		redisParams = append(redisParams, sli...)
	}

	cmd := c.redis.DoScript(ctx, rowsAddScript, []string{key}, redisParams...)
	if cmd.Err() != nil {
		c.redis.Del(ctx, allKeys...) // 失败了，删除所有键
		return nil, cmd.Err()
	}
	return allData, nil
}

// 预加载多条数据，确保写到Redis中 不会返回空错误
// key：主key
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// dataKeyValues 要加在的数据key
// 返回值
// *T： 因为preLoadLock是加锁失败等待，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// error： 执行结果
func (c *CacheRows[T]) preLoads(ctx context.Context, key string, condValues []interface{}, keyValuesStrs []string, keyValuess [][]interface{}) ([]*T, error) {
	// 先判断是否设置了pass
	queryDataKeys := make([]string, 0, len(keyValuesStrs))
	for i, keyValuesStr := range keyValuesStrs {
		dataKey := c.genDataKey(key, keyValuesStr)
		if GetPass(dataKey) {
			keyValuesStrs = append(keyValuesStrs[:i], keyValuesStrs[i+1:]...)
			keyValuess = append(keyValuess[:i], keyValuess[i+1:]...)
		} else {
			queryDataKeys = append(queryDataKeys, dataKey)
		}
	}
	if len(queryDataKeys) == 0 {
		return make([]*T, 0), nil
	}

	// 加锁
	unlock, err := c.preLoadLock(ctx, key)
	if err != nil || unlock == nil {
		return nil, err
	}
	defer unlock()

	cond := NewConds().eqs(c.condFields, condValues)
	// 查询到要读取的数据
	all, err := c.getsFromMySQL(ctx, c.T, c.Tags, cond.Ins(c.keyFields, keyValuess...))
	if err != nil {
		return nil, err
	}
	allData := all.([]*T)
	if len(allData) == 0 {
		SetPasss(queryDataKeys)
		return allData, nil
	}
	allDataInfo := make(map[string]*utils.StructValue, len(allData))
	for _, data := range allData {
		dataInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
		keyValuesStr := c.genKeyValuesStrByTInfo(dataInfo)
		allDataInfo[keyValuesStr] = dataInfo
	}

	// 保存到redis中
	// 查询所有的keyValue，只读取keyFields对应的值，最好控制好正常覆盖索引就可以读取所有的数了
	allKey, err := c.getsFromMySQL(ctx, c.T, c.keyFields, cond)
	if err != nil {
		return nil, err
	}
	allKeyValues := allKey.([]*T)
	allKeyValuesInfo := make([]*utils.StructValue, len(allKeyValues))
	for i, data := range allKeyValues {
		dataInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
		allKeyValuesInfo[i] = dataInfo
	}

	// 记录redis相关的key，后面删除使用
	allKeys := make([]string, 0, 1+len(allData))
	allKeys = append(allKeys, key) // 第一个key是数据索引

	// redis参数
	redisParams := make([]interface{}, 0, 1+2*len(allKeyValues)+len(allData)*2*len(c.Tags))
	redisParams = append(redisParams, c.expire)

	for _, dataInfoKey := range allKeyValuesInfo {
		keyValuesStr := c.genKeyValuesStrByTInfo(dataInfoKey)
		// 查找查询的allData中是否有这个数据
		sli := make([]interface{}, 0, 2*len(dataInfoKey.Tags))
		if dataInfo, ok := allDataInfo[keyValuesStr]; ok {
			for i, v := range dataInfo.Elemts {
				vfmt := goredis.ValueFmt(v)
				if vfmt == nil {
					continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
				}
				sli = append(sli, c.RedisTags[i])
				sli = append(sli, vfmt)
			}
			allKeys = append(allKeys, c.genDataKey(key, keyValuesStr))
		}

		redisParams = append(redisParams, keyValuesStr)
		redisParams = append(redisParams, len(sli))
		redisParams = append(redisParams, sli...)
	}

	// 未查询到的数据设置pass
	for _, dataKey := range queryDataKeys {
		if !utils.Contains(allKeys, dataKey) {
			SetPass(dataKey)
		}
	}

	cmd := c.redis.DoScript(ctx, rowsAddScript, []string{key}, redisParams...)
	if cmd.Err() != nil {
		c.redis.Del(ctx, allKeys...) // 失败了，删除所有键
		return nil, cmd.Err()
	}
	return allData, nil
}

// 预加载一条数据，确保写到Redis中，会返回空错误
// key：主key
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// dataKeyValue：数据key的值
// ncInfo：!= nil 表示不存在是否创建
// 返回值
// *T： 因为preLoadLock是加锁失败等待，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// interface{} != nil时 表示产生的自增id 创建时才返回
// error： 执行结果
func (c *CacheRows[T]) preLoad(ctx context.Context, key string, condValues []interface{}, keyValuesStr string, keyValues []interface{}, ncData map[string]interface{}) (*T, interface{}, error) {
	// 先判断是否设置了pass,
	dataKey := c.genDataKey(key, keyValuesStr)
	if ncData == nil {
		// 如果不想创建 又设置了pass
		if GetPass(key) || GetPass(dataKey) {
			return nil, nil, ErrNullData
		}
	}

	// 加锁
	unlock, err := c.preLoadLock(ctx, dataKey)
	if err != nil || unlock == nil {
		return nil, nil, err
	}
	defer unlock()

	var incrValue interface{}
	cond := NewConds().eqs(c.condFields, condValues)

	// 查询到要读取的数据
	t, err := c.getFromMySQL(ctx, c.T, c.Tags, cond.eqs(c.keyFields, keyValues))
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
				DelPass(dataKey)
				// 重新加载下
				t, err = c.getFromMySQL(ctx, c.T, c.Tags, cond.eqs(c.keyFields, keyValues))
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
				SetPass(dataKey)
			}
			return nil, nil, err
		}
	}

	data := t.(*T)

	// 保存到redis中
	// 查询所有的keyValue，只读取keyFields对应的值，最好控制好正常覆盖索引就可以读取所有的数了
	allKey, err := c.getsFromMySQL(ctx, c.T, c.keyFields, cond)
	if err != nil {
		return nil, nil, err
	}
	allKeyValues := allKey.([]*T)
	allKeyValuesInfo := make([]*utils.StructValue, len(allKeyValues))
	for i, data := range allKeyValues {
		dataInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
		allKeyValuesInfo[i] = dataInfo
	}

	// redis参数
	redisParams := make([]interface{}, 0, 1+2*len(allKeyValues)+2*len(c.Tags))
	redisParams = append(redisParams, c.expire)

	for _, dataInfoKey := range allKeyValuesInfo {
		keyValuesStrTemp := c.genKeyValuesStrByTInfo(dataInfoKey)
		// 查找查询的allData中是否有这个数据
		sli := make([]interface{}, 0, 2*len(dataInfoKey.Tags))
		if keyValuesStrTemp == keyValuesStr {
			dataInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
			for i, v := range dataInfo.Elemts {
				vfmt := goredis.ValueFmt(v)
				if vfmt == nil {
					continue // 空的不填充，redis处理空会写成string类型，后续incr会出错
				}
				sli = append(sli, c.RedisTags[i])
				sli = append(sli, vfmt)
			}
		}

		redisParams = append(redisParams, keyValuesStrTemp)
		redisParams = append(redisParams, len(sli))
		redisParams = append(redisParams, sli...)
	}

	cmd := c.redis.DoScript(ctx, rowsAddScript, []string{key}, redisParams...)
	if cmd.Err() != nil {
		c.redis.Del(ctx, dataKey) // 失败了，删除所有键
		return nil, nil, cmd.Err()
	}
	return data, incrValue, nil
}

// 解析Redis数据，reply的长度就是数据的数量
// 按T来解析
func (c *CacheRows[T]) redisGetAll(ctx context.Context, key string) ([]*T, error) {
	redisParams := make([]interface{}, 0, 1+len(c.Tags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, c.RedisTagsInterface()...)

	reply := make([][]interface{}, 0)
	err := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowsGetAllScript, []string{key}, redisParams...).BindSlice(&reply)
	if err == nil {
		res := make([]*T, 0, len(reply))
		for _, r := range reply {
			dest := new(T)
			destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType) // 这里不用判断err了
			for i, v := range r {
				if v == nil {
					continue
				}
				err := goredis.InterfaceToValue(v, destInfo.Elemts[i])
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

// 解析Redis数据，reply的长度就是数据的数量
// 按T来解析
func (c *CacheRows[T]) redisGets(ctx context.Context, key string, keyValuesStrs []string) ([]*T, error) {
	redisParams := make([]interface{}, 0, 1+len(c.Tags))
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, len(keyValuesStrs))
	for _, keyValuesStr := range keyValuesStrs {
		redisParams = append(redisParams, keyValuesStr)
	}
	redisParams = append(redisParams, c.RedisTagsInterface()...)

	reply := make([][]interface{}, 0)
	err := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowsGetsScript, []string{key}, redisParams...).BindSlice(&reply)
	if err == nil {
		res := make([]*T, 0, len(reply))
		for _, r := range reply {
			dest := new(T)
			destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType) // 这里不用判断err了
			for i, v := range r {
				if v == nil {
					continue
				}
				err := goredis.InterfaceToValue(v, destInfo.Elemts[i])
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

// 字段jsonarray类型的添加数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// dataKeyValue：数据key的值
// fieldValues：字段：添加的值列表, 鉴于redis的lua无法支持到int64和mysql的JSON_SEARCH不支持数字类型的查找
// 返回值
// _incr_： 自增ID, 设置ops.Create()时如果新增才返回此值，int64类型
// _err_ ： 操作失败
// mysql语句无法拼接处多个值添加避免重复能功能，所以这个接口是运行元素重复的
func (c *CacheRows[T]) JsonArrayFieldAdd(ctx context.Context, condValues []interface{}, keyValues []interface{}, fieldValues map[string][]string, ops *Options) (_incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface(c.keyFieldsLog, keyValues).Interface("fieldValues", fieldValues).Err(_err_).Msgf("CacheRows %s JsonArrayFieldAdd", c.TableName())
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 数据key的类型和值
	keyValuesStr, err := c.checkKeyValuesGenStr(keyValues)
	if err != nil {
		return nil, err
	}

	// 检查fieldValues类型是否OK
	err = c.checkJsonArrayFieldValues(fieldValues)
	if err != nil {
		return nil, err
	}

	// 修改
	err = c.jsonArrayFieldSave(ctx, key, condValues, keyValuesStr, keyValues, fieldValues, nil, ops)
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
		// 填充key字段
		for i, v := range c.keyFields {
			data[v] = keyValues[i]
		}
	}
	_, incrValue, err := c.preLoad(ctx, key, condValues, keyValuesStr, keyValues, data)
	if err != nil {
		return nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return incrValue, nil
	}

	// 再次写数据
	err = c.jsonArrayFieldSave(ctx, key, condValues, keyValuesStr, keyValues, fieldValues, nil, ops)
	if err == nil {
		return nil, nil
	} else {
		return nil, err
	}
}

// 字段jsonarray类型的删除数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// dataKeyValue：数据key的值
// fieldValues：字段：删除的值列表, 鉴于redis的lua无法支持到int64和mysql的JSON_SEARCH不支持数字类型的查找
// 返回值
// _err_ ： 操作失败
func (c *CacheRows[T]) JsonArrayFieldDel(ctx context.Context, condValues []interface{}, keyValues []interface{}, fieldValues map[string][]string, ops *Options) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface(c.keyFieldsLog, keyValues).Interface("fieldValues", fieldValues).Err(_err_).Msgf("CacheRows %s JsonArrayFieldDel", c.TableName())
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	// 数据key的类型和值
	keyValuesStr, err := c.checkKeyValuesGenStr(keyValues)
	if err != nil {
		return err
	}
	dataKey := c.genDataKey(key, keyValuesStr)

	// 检查fieldValues类型是否OK
	err = c.checkJsonArrayFieldValues(fieldValues)
	if err != nil {
		return err
	}

	// 修改
	err = c.jsonArrayFieldSave(ctx, key, condValues, keyValuesStr, keyValues, nil, fieldValues, ops)
	if err == nil {
		return nil
	} else if err == ErrNullData {
		if GetPass(key) || GetPass(dataKey) {
			return nil
		}
		// 不处理执行下面的预加载
	} else {
		return err
	}
	_, _, err = c.preLoad(ctx, key, condValues, keyValuesStr, keyValues, nil)
	if err != nil {
		if err == ErrNullData {
			return nil
		}
		return err
	}

	// 再次写数据
	err = c.jsonArrayFieldSave(ctx, key, condValues, keyValuesStr, keyValues, nil, fieldValues, ops)
	if err == nil {
		return nil
	} else if err == ErrNullData {
		return nil // 数据项不存在 del操作返回nil
	} else {
		return err
	}
}

// 没有返回值
func (c *CacheRows[T]) jsonArrayFieldSave(ctx context.Context, key string, condValues []interface{}, keyValuesStr string, keyValues []interface{}, add, del map[string][]string, ops *Options) error {
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

	duplicate := utils.If(ops != nil && ops.jsonArrayDuplicate, true, false)
	// 获取Redis参数
	addFields, delFields, redisParams := c.redisJsonArrayParam(keyValuesStr, add, del, duplicate)

	cmd := c.redis.DoScript2(goredis.CtxNonilErrIgnore(ctx), rowsJsonArrayModifyScript, []string{key}, redisParams...)
	if cmd.Cmd.Err() == nil {
		rst := [][]string{}
		err := cmd.BindSlice(&rst)
		if err != nil {
			return err
		}
		if len(addFields)+len(delFields) != len(rst) {
			return errors.New("未知错误")
		}
		add_ := map[string][]string{}
		del_ := map[string][]string{}
		index := 0
		for _, fields := range addFields {
			add_[fields] = rst[index]
			index++
		}
		for _, fields := range delFields {
			del_[fields] = rst[index]
			index++
		}

		// 同步mysql，添加上数据key字段
		mysqlUnlock = true
		err = c.jsonArrayToMySQL(ctx, NewConds().eqs(c.condFields, condValues).eqs(c.keyFields, keyValues), add_, del_, key, func(err error) {
			defer unlock()
			if err != nil {
				c.redis.Del(ctx, c.genDataKey(key, keyValuesStr)) // mysql错了 删除数据键
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
