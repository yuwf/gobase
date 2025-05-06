package mrcache

// https://github.com/yuwf/gobase

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/mysql"
)

// T 为数据库结构类型 TF为指定字段类型
// 使用场景：查询的是一个指定的字段，查询条件对应多个结果，使用hash中kv存储每一条数据
// redis：hash field和mysql的field对应，value使用goredis.ValueFmt格式化
// dataKey对应mysql的一个字段，查询条件和datakey对应的数据要唯一，最好有唯一索引
type CacheColumn[T any, TF any] struct {
	*Cache
	TF reflect.Type // TF的reflect类型

	dataValueField      string // 指定查询的字段tag，区分大小写
	dataValueFieldIndex int    // value在tableInfo中的索引
}

type CacheColumnData[TF any] struct {
	Key   interface{} // dataKeyField
	Value *TF         // dataValueField
}

func NewCacheColumn[T any, TF any](redis *goredis.Redis, mysql *mysql.MySQL, tableName string, tableCount, tableIndex int, condFields, keyFields []string, dataValueField string) (*CacheColumn[T, TF], error) {
	cache, err := NewCache[T](redis, mysql, tableName, tableCount, tableIndex, condFields)
	if err != nil {
		return nil, err
	}
	cache.keyPrefix = "mrc"

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
		if !cache.IsBaseType(idx) {
			err := fmt.Errorf("tag:%s(%s) as keyFields type error", f, cache.T.String())
			return nil, err
		}
		keyFields_ = append(keyFields_, f)
		keyFieldsIndex_ = append(keyFieldsIndex_, idx)
	}
	cache.keyFields = keyFields_
	cache.keyFieldsIndex = keyFieldsIndex_
	cache.keyFieldsLog = "[" + strings.Join(keyFields_, ",") + "]"

	// 验证dataValueField是否存在
	dataValueFieldIndex := cache.FindIndexByTag(dataValueField)
	if dataValueFieldIndex == -1 {
		err := fmt.Errorf("tag:%s not find in %s", dataValueField, cache.T.String())
		return nil, err
	}

	c := &CacheColumn[T, TF]{
		Cache:               cache,
		TF:                  reflect.TypeOf((*TF)(nil)).Elem(),
		dataValueField:      dataValueField,
		dataValueFieldIndex: dataValueFieldIndex,
	}
	return c, nil
}

/* 需要照着CacheRows改造下才能使用功能 去掉了dataKeyField
// 读取数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：dataKeyValue:dataValueValue
func (c *CacheColumn[T, TF]) GetAll(ctx context.Context, condValues []interface{}) (_rst_ []*CacheColumnData[TF], _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", _rst_).Msg("CacheColumn GetAll")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 先读取所有keyDataValues
	alldataKeyValues, err := c.getsFromMySQL(ctx, c.Fields[c.dataKeyFieldIndex].Type, []string{c.dataKeyField}, NewConds().eqs(c.condFields, condValues))
	if err != nil {
		return nil, err
	}
	alldataKeyValuesVO := reflect.ValueOf(alldataKeyValues)
	dataKeyValues := make([]interface{}, 0, alldataKeyValuesVO.Len())
	for i := 0; i < alldataKeyValuesVO.Len(); i++ {
		dataKeyValues = append(dataKeyValues, alldataKeyValuesVO.Index(i).Elem().Interface())
	}

	return c.gets(ctx, key, condValues, dataKeyValues)
}

// 读取一条数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：列表
func (c *CacheColumn[T, TF]) Gets(ctx context.Context, condValues []interface{}, dataKeyValues []interface{}) (_rst_ []*CacheColumnData[TF], _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Interface("rst", _rst_).Msg("CacheColumn Gets")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 检测 dataKeyValue的类型和值
	for _, dataKeyValue := range dataKeyValues {
		err := c.checkFiledValue(c.dataKeyFieldIndex, dataKeyValue)
		if err != nil {
			return nil, err
		}
	}

	return c.gets(ctx, key, condValues, dataKeyValues)
}

func (c *CacheColumn[T, TF]) gets(ctx context.Context, key string, condValues []interface{}, dataKeyValues []interface{}) ([]*CacheColumnData[TF], error) {
	if len(dataKeyValues) == 0 {
		return make([]*CacheColumnData[TF], 0), nil
	}

	// 从Redis中读取
	needLoad := dataKeyValues
	res := make([]*CacheColumnData[TF], 0)
	reply := make([]interface{}, 0)
	err := c.redis.Do2(ctx, append([]interface{}{"hmget", key}, dataKeyValues...)...).BindSlice(&reply)
	if err == nil {
		needLoad = []interface{}{}
		for i := 0; i < len(reply); i++ {
			r := reply[i]
			if r != nil {
				d := new(TF)
				err := goredis.InterfaceToValue(r, reflect.ValueOf(d))
				if err == nil {
					res = append(res, &CacheColumnData[TF]{
						Key:   dataKeyValues[i],
						Value: d,
					})
					continue
				}
			}
			if GetPass(key + c.fmtBaseType(dataKeyValues[i])) {
				continue
			}
			needLoad = append(needLoad, dataKeyValues[i])
		}
		if len(needLoad) == 0 {
			return res, nil
		}
	} else if GetPass(key) {
		return res, nil
	}

	// 预加载 尝试从数据库中读取
	preData, err := c.preLoads(ctx, key, condValues, needLoad)
	if err != nil {
		return nil, err
	}
	if preData != nil { // 执行了预加载
		res = append(res, preData...)
		return res, nil
	}

	// 如果不是自己执行的预加载，这里必须递归调用，因为不知道别的preLoads有没有加载自己的dataKeyValues
	return c.gets(ctx, key, condValues, dataKeyValues)
}

// 读取一条数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// dataKeyValue：数据key的值
// 返回值：是T结构类型的指针列表
func (c *CacheColumn[T, TF]) Get(ctx context.Context, condValues []interface{}, dataKeyValue interface{}) (_rst_ *TF, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("dataKeyValue", dataKeyValue).Interface("rst", _rst_).Err(_err_).Msg("CacheColumn Get")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 数据key的类型和值
	err = c.checkFiledValue(c.dataKeyFieldIndex, dataKeyValue)
	if err != nil {
		return nil, err
	}

	return c.get(ctx, key, condValues, dataKeyValue, false)
}

// 第一步是否直接跳过Redis加载
func (c *CacheColumn[T, TF]) get(ctx context.Context, key string, condValues []interface{}, dataKeyValue interface{}, ingnoreRedis bool) (_rst_ *TF, _err_ error) {
	if !ingnoreRedis {
		// 从Redis中读取
		dest := new(TF)
		destVo := reflect.ValueOf(dest)
		err := c.redis.Do2(ctx, "HGET", key, dataKeyValue).BindValue(destVo)
		if err == nil {
			return dest, nil
		} else if goredis.IsNilError(err) && (GetPass(key) || GetPass(key+c.fmtBaseType(dataKeyValue))) {
			return nil, ErrNullData
		}
	}

	// 其他情况不处理执行下面的预加载
	preData, _, err := c.preLoad(ctx, key, condValues, dataKeyValue, nil)
	if err != nil {
		return nil, err
	}
	// 预加载的数据就是新增的
	if preData != nil {
		return preData, nil
	}

	// 如果不是自己执行的预加载，这里重新读取下
	dest := new(TF)
	destVo := reflect.ValueOf(dest)
	err = c.redis.Do2(ctx, "HGET", key, dataKeyValue).BindValue(destVo)
	if err == nil {
		return dest, nil
	} else if goredis.IsNilError(err) {
		return nil, ErrNullData
	} else {
		return nil, err
	}
}

// 读取数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// dataKeyValue：数据key的值
// 返回值：是否存在
func (c *CacheColumn[T, TF]) Exist(ctx context.Context, condValues []interface{}, dataKeyValue interface{}) (_rst_ bool, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("dataKeyValue", dataKeyValue).Interface("rst", _rst_).Err(_err_).Msg("CacheColumn Exist")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return false, err
	}

	// 数据key的类型和值
	err = c.checkFiledValue(c.dataKeyFieldIndex, dataKeyValue)
	if err != nil {
		return false, err
	}

	// 从Redis中读取
	rstV, err := c.redis.HExists(ctx, key, c.fmtBaseType(dataKeyValue)).Result()
	if err == nil {
		if rstV {
			return true, nil
		} else if GetPass(key) || GetPass(key+c.fmtBaseType(dataKeyValue)) {
			return false, nil
		}
	}

	// 其他情况不处理执行下面的预加载
	preData, _, err := c.preLoad(ctx, key, condValues, dataKeyValue, nil)
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
	rstV, err = c.redis.HExists(ctx, key, c.fmtBaseType(dataKeyValue)).Result()
	if err == nil {
		if rstV {
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
// 返回值
// _incr_： 自增ID，int64类型
// _err_ ： 操作失败
func (c *CacheColumn[T, TF]) Add(ctx context.Context, condValues []interface{}, dataKeyValue interface{}, data *TF) (_incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("data", data).Err(_err_).Interface("incr", _incr_).Msg("CacheColumn Add")
	})()

	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	fmtData := goredis.ValueFmt(reflect.ValueOf(data))
	if fmtData == nil {
		return nil, errors.New("data invalid")
	}

	dataSQL := map[string]interface{}{c.dataKeyField: dataKeyValue, c.dataValueField: fmtData}
	incrValue, err := c.addToMySQL(ctx, condValues, dataSQL)
	if err != nil {
		return nil, err
	}
	DelPass(key)
	DelPass(key + c.fmtBaseType(dataKeyValue))

	// 不需要返回值
	return incrValue, nil
}

// 删除全部数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：error
func (c *CacheColumn[T, TF]) DelAll(ctx context.Context, condValues []interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Msg("CacheColumn DelAll")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	cmd := c.redis.Del(ctx, key)
	if cmd.Err() != nil {
		return cmd.Err()
	}

	err = c.delToMySQL(ctx, NewConds().eqs(c.condFields, condValues)) // 删mysql
	if err != nil {
		return err
	}

	return nil
}

// 删除数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// dataKeyValue：数据key的值
// 返回值：error
func (c *CacheColumn[T, TF]) Del(ctx context.Context, condValues []interface{}, dataKeyValue interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("dataKeyValue", dataKeyValue).Err(_err_).Msg("CacheColumn Del")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	// 数据key的类型和值
	err = c.checkFiledValue(c.dataKeyFieldIndex, dataKeyValue)
	if err != nil {
		return err
	}

	cmd := c.redis.HDel(ctx, key, c.fmtBaseType(dataKeyValue))
	if cmd.Err() != nil {
		return cmd.Err()
	}

	err = c.delToMySQL(ctx, NewConds().eqs(c.condFields, condValues).Eq(c.dataKeyField, dataKeyValue)) // 删mysql
	if err != nil {
		return err
	}

	return nil
}

// 只Cache删除数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// 返回值：error
func (c *CacheColumn[T, TF]) DelAllCache(ctx context.Context, condValues []interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Err(_err_).Msg("CacheColumn DelAllCache")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	cmd := c.redis.Del(ctx, key)
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

// 只Cache删除数据
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// dataKeyValue：数据key的值
// 返回值：error
func (c *CacheColumn[T, TF]) DelCache(ctx context.Context, condValues []interface{}, dataKeyValue interface{}) (_err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("dataKeyValue", dataKeyValue).Err(_err_).Msg("CacheColumn DelCache")
	})()

	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return err
	}

	// 数据key的类型和值
	err = c.checkFiledValue(c.dataKeyFieldIndex, dataKeyValue)
	if err != nil {
		return err
	}

	cmd := c.redis.Del(ctx, key, c.fmtBaseType(dataKeyValue))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return nil
}

// 写数据 ctx: CtxKey_NEC
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致
// data：修改内容
// -     可以是结构或者结构指针，内部的数据是要保存的数据
// -     data.tags名称需要和T一致，可以是T的一部分
// -     若data.tags中含有设置的自增字段 或者 条件字段 会忽略set
// 返回值
// _incr_： 自增ID，ctx含有【CtxKey_NEC】时如果新增数据，int64类型
// _err_ ： 操作失败
func (c *CacheColumn[T, TF]) Set(ctx context.Context, condValues []interface{}, dataKeyValue []interface{}, data *TF) (_incr_ interface{}, _err_ error) {
	defer c.logContext(&ctx, &_err_, func(l *zerolog.Event) {
		l.Interface(c.condFieldsLog, condValues).Interface("data", data).Err(_err_).Interface("incr", _incr_).Msg("CacheColumn Set")
	})()

	/// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 数据key的类型和值
	err = c.checkFiledValue(c.dataKeyFieldIndex, dataKeyValue)
	if err != nil {
		return nil, err
	}

	fmtData := goredis.ValueFmt(reflect.ValueOf(data))
	if fmtData == nil {
		return nil, errors.New("data invalid")
	}

	err = c.setSave(ctx, key, condValues, dataKeyValue, fmtData)
	if err == nil {
		return nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, err
	}
	// 预加载 尝试从数据库中读取
	_, incrValue, err := c.preLoad(ctx, key, condValues, dataKeyValue, utils.If(ctx.Value(CtxKey_NEC) != nil, fmtData, nil))
	if err != nil {
		return nil, err
	}
	// 返回了自增，数据添加已经完成了，预加载的数据就是新增的
	if incrValue != nil {
		return incrValue, nil
	}

	// 再次写数据
	err = c.setSave(ctx, key, condValues, dataKeyValue, fmtData)
	if err == nil {
		return nil, nil
	} else {
		return nil, err
	}
}

// 没有返回值
func (c *CacheColumn[T, TF]) setSave(ctx context.Context, key string, condValues []interface{}, dataKeyValue interface{}, fmtData interface{}) error {
	// 加锁
	unlock, err := c.saveLock(ctx, key+c.fmtBaseType(dataKeyValue))
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
	redisParams := make([]interface{}, 0, 3)
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, dataKeyValue)
	redisParams = append(redisParams, fmtData)
	cmd := c.redis.DoScript(ctx, columnSetScript, []string{key}, redisParams...)
	if cmd.Err() == nil {
		// 同步mysql
		dataSQL := map[string]interface{}{c.dataValueField: fmtData}
		mysqlUnlock = true
		err := c.saveToMySQL(ctx, NewConds().eqs(c.condFields, condValues).Eq(c.dataKeyField, dataKeyValue), dataSQL, key, func(err error) {
			defer unlock()
			if err != nil {
				c.redis.HDel(ctx, key, c.fmtBaseType(dataKeyValue)) // mysql错了 要删缓存
			}
		})
		if err != nil {
			return err
		}
		return nil
	} else {
		if goredis.IsNilError(cmd.Err()) {
			return ErrNullData
		}
		return cmd.Err()
	}
}

// 预加载多条数据，确保写到Redis中
// key：主key
// condValues：查询条件变量 condFields对应的值 顺序和对应的类型要一致值
// dataKeyValues 要加在的数据key
// 返回值
// *TF： 因为preLoadLock是加锁失败等待，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// error： 执行结果
func (c *CacheColumn[T, TF]) preLoads(ctx context.Context, key string, condValues []interface{}, dataKeyValues []interface{}) ([]*CacheColumnData[TF], error) {
	// 加锁
	unlock, err := c.preLoadLock(ctx, key)
	if err != nil || unlock == nil {
		return nil, err
	}
	defer unlock()

	// 查询到要读取的数据
	t, err := c.getsFromMySQL(ctx, c.T, []string{c.dataKeyField, c.dataValueField}, NewConds().eqs(c.condFields, condValues).In(c.dataKeyField, dataKeyValues...))
	if err != nil {
		return nil, err
	}
	datas := t.([]*T)
	if len(datas) == 0 {
		SetPass(key)
		return nil, nil
	}

	// 保存到redis中
	res := make([]*CacheColumnData[TF], 0)
	redisParams := make([]interface{}, 0, 1+2*len(datas))
	redisParams = append(redisParams, c.expire)
	for _, data := range datas {
		tInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
		redisParams = append(redisParams, goredis.ValueFmt(tInfo.Elemts[c.dataKeyFieldIndex]))
		redisParams = append(redisParams, goredis.ValueFmt(tInfo.Elemts[c.dataValueFieldIndex]))

		d := new(TF)
		err := goredis.InterfaceToValue(tInfo.Elemts[c.dataValueFieldIndex].Interface(), reflect.ValueOf(d))
		if err != nil {
			return nil, err
		}
		res = append(res, &CacheColumnData[TF]{
			Key:   tInfo.Elemts[c.dataKeyFieldIndex].Interface(),
			Value: d,
		})
	}
	cmd := c.redis.DoScript(ctx, columnAddScript, []string{key}, redisParams...)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return res, nil
}

// 预加载一条数据，确保写到Redis中
// key：主key
// dataKeyValue 这条数据的值
// ncInfo：!= nil 表示不存在是否创建
// 返回值
// *T： 因为preLoadLock是加锁失败等待，!= nil 表示是本逻辑执行了加载，否则没有执行加载
// interface{} != nil时 表示产生的自增id 创建时才返回
// error： 执行结果
func (c *CacheColumn[T, TF]) preLoad(ctx context.Context, key string, condValues []interface{}, dataKeyValue interface{}, ncData interface{}) (*TF, interface{}, error) {
	// 加锁
	unlock, err := c.preLoadLock(ctx, key)
	if err != nil || unlock == nil {
		return nil, nil, err
	}
	defer unlock()

	var incrValue interface{}
	cond := NewConds().eqs(c.condFields, condValues)

	// 查询到要读取的数据
	t, err := c.getFromMySQL(ctx, c.TF, []string{c.dataValueField}, cond.Eq(c.dataKeyField, dataKeyValue))
	if ncData != nil {
		// 不存在要创建数据
		if err != nil {
			if err == ErrNullData {
				// 创建数据
				dataSQL := map[string]interface{}{c.dataKeyField: dataKeyValue, c.dataValueField: ncData}
				incrValue, err = c.addToMySQL(ctx, condValues, dataSQL)
				if err != nil {
					return nil, nil, err
				}
				DelPass(key)
				DelPass(key + c.fmtBaseType(dataKeyValue))
				// 重新加载下
				t, err = c.getFromMySQL(ctx, c.TF, []string{c.dataKeyField}, cond.Eq(c.dataKeyField, dataKeyValue))
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
				SetPass(key + c.fmtBaseType(dataKeyValue))
			}
			return nil, nil, err
		}
	}

	data := t.(*TF)

	// 保存到redis中
	redisParams := make([]interface{}, 0, 3)
	redisParams = append(redisParams, c.expire)
	redisParams = append(redisParams, dataKeyValue)
	redisParams = append(redisParams, goredis.ValueFmt(reflect.ValueOf(data)))
	cmd := c.redis.DoScript(ctx, columnAddScript, []string{key}, redisParams...)
	if cmd.Err() != nil {
		return nil, nil, cmd.Err()
	}
	return data, incrValue, nil
}
*/
