package mrcache

// https://github.com/yuwf/gobase

import (
	"context"
	"errors"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/utils"

	"github.com/redis/go-redis/v9"
)

// redis: 是否从redis加载
// proLoad: 是否从mysql加载
func (c *CacheRow[T]) get(ctx context.Context, key string, condValues []interface{}, redis, proLoad bool) (_rst_ *T, _err_ error) {
	if redis {
		// 从Redis中读取
		dest := new(T)
		destInfo, _ := utils.GetStructInfoByStructType(dest, c.StructType)
		err := c.redis.Script(ctx, rowGetScript, []string{key}, c.redisGetParam()...).BindValues(destInfo.Elemts)
		if err == nil {
			return dest, nil
		}
		// 如果后面不从sql中加载 如果有错直接返回
		if !proLoad {
			if goredis.IsNil(err) {
				return nil, ErrNullData
			}
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

		return nil, ErrNullData
	}

	return nil, nil // 理论上不会走到这里了
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
		conds[i] = &Cond[T]{
			key: c.genCondValuesKey(condValues),
		}
	}

	if redis {
		// redis中读取，key分布不一样，用管道读取
		pipeline := c.redis.NewPipeline()
		for i := range conds {
			conds[i].cmd = pipeline.Script(ctx, rowGetScript, []string{conds[i].key}, c.redisGetParam()...)
		}
		pipeline.Exec(ctx) // 不关心他的返回值
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

// 预加载
// ncData: !=nil时表示不存在要创建数据
// 返回值：data: 预加载的数据（如果为空表示已经预加载了），incrValue: 增量值，err: 错误
func (c *CacheRow[T]) preLoad(ctx context.Context, key string, condValues []interface{}, ncData map[string]interface{}) (*T, interface{}, error) {
	if ncData == nil {
		// 如果不想创建 又设置了pass
		if GetPass(key) {
			return nil, nil, ErrNullData
		}
	}

	var incrValue interface{}
	cond := NewConds().Eqs(c.condFields, condValues)

	// 查询到要读取的数据
	t, err := c.getFromMySQL(ctx, c.T, c.Tags, cond)
	if ncData != nil {
		// 不存在要创建数据
		if err != nil {
			if err == ErrNullData {
				// 创建数据
				incrValue, err = c.addToMySQL(ctx, condValues, nil, ncData)
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
				SetPass(key) // 不存在 标记下
			}
			return nil, nil, err
		}
	}

	data := t.(*T)
	rst, err := c.preToRedis(ctx, key, data)
	if err != nil {
		return nil, nil, err
	}
	return rst, incrValue, nil
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

	rst, err := c.preToRediss(ctx, datas)
	if err != nil {
		return nil, err
	}
	for k := range queryCondValuess {
		if _, ok := rst[k]; !ok {
			SetPass(k) // 不存在 标记下
		}
	}
	return rst, nil
}

// 数据保存到Redis
// 如果Redis存在返回存在的数据,如果不存在返回当前保存的数据
func (c *CacheRow[T]) preToRedis(ctx context.Context, key string, data *T) (*T, error) {
	tInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
	redisParams := make([]interface{}, 0, 1+2*len(tInfo.Tags))
	redisParams = append(redisParams, c.expire)
	for i, v := range tInfo.Elemts {
		vfmt := goredis.ValueToRedisArg(v)
		if vfmt == nil {
			continue // 空的不填充，redis处理空会写成string类型，如果是int类型，后续incr会出错
		}
		redisParams = append(redisParams, c.RedisTags[i])
		redisParams = append(redisParams, vfmt)
	}
	cmd := c.redis.Script(ctx, rowAddScript, []string{key}, redisParams...)
	if cmd.Err() != nil {
		if goredis.IsNil(cmd.Err()) {
			// 从redis中读取，这种概率比较低，直接读取吧
			return c.get(ctx, key, nil, true, false)
		}
		return nil, cmd.Err()
	}
	return data, nil
}

// 数据保存到Redis
// 如果Redis存在返回存在的数据,如果不存在返回当前保存的数据
func (c *CacheRow[T]) preToRediss(ctx context.Context, datas []*T) (map[string]*T, error) {
	type Result struct {
		key  string
		data *T
		cmd  *goredis.RedisCommond
	}
	rst := make(map[string]*Result)

	pipeline := c.redis.NewPipeline()
	for _, data := range datas {
		tInfo, _ := utils.GetStructInfoByStructType(data, c.StructType)
		condValues := make([]interface{}, 0, len(c.condFields))
		for i := 0; i < len(c.condFields); i++ {
			condValues = append(condValues, goredis.ValueToRedisArg(tInfo.Elemts[c.condFieldsIndex[i]]))
		}
		key := c.genCondValuesKey(condValues)

		redisParams := make([]interface{}, 0, 1+2*len(tInfo.Tags))
		redisParams = append(redisParams, c.expire)
		for i, v := range tInfo.Elemts {
			vfmt := goredis.ValueToRedisArg(v)
			if vfmt == nil {
				continue // 空的不填充，redis处理空会写成string类型，如果是int类型，后续incr会出错
			}
			redisParams = append(redisParams, c.RedisTags[i])
			redisParams = append(redisParams, vfmt)
		}
		rst[key] = &Result{
			key:  key,
			data: data,
			cmd:  pipeline.Script(ctx, rowAddScript, []string{key}, redisParams...),
		}
	}
	pipeline.Exec(ctx)

	// 处理结果
	rst2 := make(map[string]*T)
	for k, cmd := range rst {
		if cmd.cmd.Err() != nil {
			if goredis.IsNil(cmd.cmd.Err()) {
				// 数据已经存在了，从redis中读取，这种概率比较低，直接读取吧
				t, err := c.get(ctx, k, nil, true, false)
				if err != nil {
					return nil, err
				}
				rst2[k] = t
			} else {
				return nil, cmd.cmd.Err()
			}
		} else {
			rst2[k] = cmd.data
		}
	}
	return rst2, nil
}

func (c *CacheRow[T]) add(ctx context.Context, condValues []interface{}, modifyData *ModifyData, ops *Options) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}

	incrValue, err := c.addToMySQL(ctx, condValues, nil, modifyData.data)
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

func (c *CacheRow[T]) set(ctx context.Context, condValues []interface{}, modifyData *ModifyData, ops *Options) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}
	nr := ops != nil && ops.noResp

	var dest *T
	if nr {
		err = c.modifySave(ctx, key, condValues, modifyData, false)
	} else {
		dest = new(T)
		modifyData.RstMakeByT(dest, c.StructType)
		err = c.modifySaveGet(ctx, key, condValues, modifyData, false)
	}
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, key, condValues, utils.If(ops != nil && ops.noExistCreate, modifyData.data, nil))
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
		err = c.modifySave(ctx, key, condValues, modifyData, false)
	} else {
		dest = new(T)
		modifyData.RstMakeByT(dest, c.StructType)
		err = c.modifySaveGet(ctx, key, condValues, modifyData, false)
	}
	if err == nil {
		return dest, nil, nil
	} else {
		return nil, nil, err
	}
}

func (c *CacheRow[T]) dels(ctx context.Context, keys []string, condValuess [][]interface{}, delMysql bool) error {
	// 合并相同slot的key
	keyss := map[int][]string{}
	for _, key := range keys {
		slot := goredis.Slot(key)
		keyss[slot] = append(keyss[slot], key)
	}

	// key分布不一样，用管道删除
	cmds := make([]*redis.IntCmd, 0, len(keyss))
	pipeline := c.redis.NewPipeline()
	for _, keys := range keyss {
		cmds = append(cmds, pipeline.Del(ctx, keys...)) // 删缓存
	}
	_, err := pipeline.Exec(ctx)
	if err != nil {
		return err
	}

	if delMysql {
		delNum := int64(0)
		for _, cmd := range cmds {
			delNum += cmd.Val()
		}
		if delNum == 0 && GetPasss(keys) {
			return nil
		}

		err = c.delToMySQL(ctx, NewConds().Ins(c.condFields, condValuess...)) // 删mysql
		if err != nil {
			return err
		}

		// 再延迟删除一次Redis
		antsPool.Submit(func() {
			for _, keys := range keyss {
				pipeline.Del(ctx, keys...)
			}
			pipeline.Exec(ctx)
		})

		// 删除后 标记下数据pass
		for _, key := range keys {
			SetPass(key)
		}
	}
	return nil
}

func (c *CacheRow[T]) modify(ctx context.Context, condValues []interface{}, modifyData *ModifyData, ops *Options) (*T, interface{}, error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, nil, err
	}
	nr := ops != nil && ops.noResp

	var dest *T
	if nr {
		err = c.modifySave(ctx, key, condValues, modifyData, true)
	} else {
		dest = new(T)
		modifyData.RstMakeByT(dest, c.StructType)
		err = c.modifySaveGet(ctx, key, condValues, modifyData, true)
	}
	if err == nil {
		return dest, nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, key, condValues, utils.If(ops != nil && ops.noExistCreate, modifyData.data, nil))
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
		err = c.modifySave(ctx, key, condValues, modifyData, true)
	} else {
		dest = new(T)
		modifyData.RstMakeByT(dest, c.StructType)
		err = c.modifySaveGet(ctx, key, condValues, modifyData, true)
	}
	if err == nil {
		return dest, nil, nil
	} else {
		return nil, nil, err
	}
}

func (c *CacheRow[T]) modify2(ctx context.Context, condValues []interface{}, modifyData *ModifyData, ops *Options) (_incr_ interface{}, _err_ error) {
	// 检查条件变量
	key, err := c.checkCondValuesGenKey(condValues)
	if err != nil {
		return nil, err
	}

	// 写数据
	err = c.modifySaveGet(ctx, key, condValues, modifyData, true)
	if err == nil {
		return nil, nil
	} else if err == ErrNullData {
		// 不处理执行下面的预加载
	} else {
		return nil, err
	}

	// 预加载 尝试从数据库中读取
	preData, incrValue, err := c.preLoad(ctx, key, condValues, utils.If(ops != nil && ops.noExistCreate, modifyData.data, nil))
	if err != nil {
		return nil, err
	}
	// 返回了自增，数据添加已经完成了，从预加载数据中拷贝返回值
	if incrValue != nil {
		dInfo, _ := utils.GetStructInfoByStructType(preData, c.StructType)
		modifyData.RstFill(dInfo)
		return incrValue, nil
	}

	// 再次写数据
	err = c.modifySaveGet(ctx, key, condValues, modifyData, true)
	if err == nil {
		return nil, nil
	} else {
		return nil, err
	}
}

// 没有返回值
func (c *CacheRow[T]) modifySave(ctx context.Context, key string, condValues []interface{}, modifyData *ModifyData, numIncr bool) error {
	if c.asyncToMysql {
		redisParams := c.redisModifyParam(modifyData.data, numIncr, "null") // redis参数
		cmd := c.redis.Script(ctx, rowModifyScript, []string{key}, redisParams...)
		if cmd.Cmd.Err() == nil {
			//将key记录到脏数据列表中
			c.addDirtyKey(ctx, key)
			return nil
		} else {
			if goredis.IsNil(cmd.Cmd.Err()) {
				return ErrNullData
			}
			return cmd.Cmd.Err()
		}
	} else {
		// 同步mysql，需要锁
		unlock, err := c.saveLock(ctx, key)
		if err != nil {
			return err
		}
		defer unlock()

		// 需要redis的返回值，同步到mysql
		modifyData.RstMake(modifyData.tags)
		redisParams := c.redisModifyGetParam(modifyData.rstTags, modifyData.data, numIncr) // redis参数
		err = c.redis.Script(ctx, rowModifyGetScript, []string{key}, redisParams...).BindValues(modifyData.rstValues)
		if err == nil {
			err := c.saveToMySQL(ctx, NewConds().Eqs(c.condFields, condValues), modifyData.RstToMap())
			if err != nil {
				c.redis.Del(ctx, key) // mysql错了 要删缓存
				return err
			}
			return nil
		} else {
			if goredis.IsNil(err) {
				return ErrNullData
			}
			return err
		}
	}
}

// 填充modifyData的返回值，modifyData需要创建好返回值相关的参数
func (c *CacheRow[T]) modifySaveGet(ctx context.Context, key string, condValues []interface{}, modifyData *ModifyData, numIncr bool) error {
	redisParams := c.redisModifyGetParam(modifyData.rstTags, modifyData.data, numIncr) // redis参数
	if c.asyncToMysql {
		err := c.redis.Script(ctx, rowModifyGetScript, []string{key}, redisParams...).BindValues(modifyData.rstValues)
		if err == nil {
			//将key记录到脏数据列表中
			c.addDirtyKey(ctx, key)
			return nil
		} else {
			if goredis.IsNil(err) {
				return ErrNullData
			}
			return err
		}
	} else {
		// 同步mysql，需要锁
		unlock, err := c.saveLock(ctx, key)
		if err != nil {
			return err
		}
		defer unlock()

		err = c.redis.Script(ctx, rowModifyGetScript, []string{key}, redisParams...).BindValues(modifyData.rstValues)
		if err == nil {
			err := c.saveToMySQL(ctx, NewConds().Eqs(c.condFields, condValues), modifyData.RstToMap())
			if err != nil {
				c.redis.Del(ctx, key) // mysql错了 要删缓存
				return err
			}
			return nil
		} else {
			if goredis.IsNil(err) {
				return ErrNullData
			}
			return err
		}
	}
}

// 没有返回值
func (c *CacheRow[T]) jsonArraySave(ctx context.Context, key string, condValues []interface{}, add, del map[string][]string, ops *Options) error {
	duplicate := utils.If(ops != nil && ops.jsonArrayDuplicate, true, false)
	// 获取Redis参数
	addFields, delFields, redisParams := c.redisJsonArrayParam(add, del, duplicate)
	if c.asyncToMysql {
		cmd := c.redis.Script(ctx, rowJsonArrayModifyScript, []string{key}, redisParams...)
		if cmd.Cmd.Err() == nil {
			//将key记录到脏数据列表中
			c.addDirtyKey(ctx, key)
			return nil
		} else {
			if goredis.IsNil(cmd.Cmd.Err()) {
				return ErrNullData
			}
			return cmd.Cmd.Err()
		}
	} else {
		// 同步mysql，需要锁
		unlock, err := c.saveLock(ctx, key)
		if err != nil {
			return err
		}
		defer unlock()

		cmd := c.redis.Script(ctx, rowJsonArrayModifyScript, []string{key}, redisParams...)
		if cmd.Cmd.Err() == nil {
			rst := [][]string{}
			err := cmd.Bind(&rst)
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
			err = c.jsonArrayToMySQL(ctx, NewConds().Eqs(c.condFields, condValues), add_, del_)
			if err != nil {
				c.redis.Del(ctx, key) // mysql错了 要删缓存
				return err
			}
			return nil
		} else {
			if goredis.IsNil(cmd.Cmd.Err()) {
				return ErrNullData
			}
			return cmd.Cmd.Err()
		}
	}
}
