package mrcache

// https://github.com/yuwf/gobase

import (
	"context"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/yuwf/gobase/goredis"
	"github.com/yuwf/gobase/utils"

	"github.com/panjf2000/ants"
	"github.com/rs/zerolog/log"
)

// 脏数据异步处理

var asyncCacheLock sync.Mutex
var asyncCache = []*Cache{}

// 异步保存任务独占一个协程池，防止和其他业务协程池抢占资源
var antsPool *ants.Pool = utils.DefaultAntsPool()

func init() {
	go loopAsync()
}

func addAsyncCache(cache *Cache) {
	asyncCacheLock.Lock()
	defer asyncCacheLock.Unlock()
	asyncCache = append(asyncCache, cache)
}

func loopAsync() {
	for {
		runAsyncSave()
		time.Sleep(time.Millisecond * 500)
	}
}

func runAsyncSave() {
	defer utils.HandlePanic()
	asyncCacheLock.Lock()
	defer asyncCacheLock.Unlock()

	traceId := utils.GetTraceID(context.Background())
	uuid := utils.LocalIPString() + "-" + strconv.Itoa(os.Getpid()) + "-" + utils.RandString(16)

	for _, cache := range asyncCache {
		ctx := utils.CtxSetTrace(context.Background(), traceId, "cache_async_save:"+cache.tableName)
		dirtyKeys := []string{}
		keys := []string{cache.dirtyKey, cache.dirtyKeyProcessing, cache.dirtyKeyLastCheck}
		cache.redis.Script(utils.CtxSetNolog(ctx), dirtyKeyGetScript, keys, cache.asyncMaxCount, cache.asyncTimeout, uuid).Bind(&dirtyKeys)

		for _, key := range dirtyKeys {
			antsPool.Submit(func() {
				cache.asyncSave(ctx, key, uuid)
			})
		}
	}
}

// 添加一个脏数据key到脏数据列表中
func (c *Cache) addDirtyKey(ctx context.Context, key string) {
	// 异步写入即可，不影响业务逻辑
	antsPool.Submit(func() {
		c.redis.Script(utils.CtxSetNolog(ctx), dirtyKeyAddScript, []string{c.dirtyKey}, key)
	})
}

type dirtyData struct {
	c       *Cache
	Version int64
	Data    map[string]interface{} // value需要按照tag的结构来填充
}

func (d *dirtyData) RedisUnmarshal(reply any) error {
	values := reply.([]interface{})
	d.Version = values[0].(int64)
	fv := values[1].([]interface{})
	d.Data = make(map[string]interface{})
	for i := 0; i+1 < len(fv); i += 2 {
		at := d.c.GetTagIndexByRedisTag(fv[i].(string))
		if at == -1 {
			continue // 只有一种情况，Redis中有数据，但数据库结构中没有这个tag，可能是结构更新了，Redis还没更新，或者改变了tag的名字，忽略
		}
		v := reflect.New(d.c.Fields[at].Type).Elem()
		err := goredis.ReplyToValue(fv[i+1], v)
		if err != nil {
			return err // 解析失败，下面会输出错误日志，然后等超时继续读取
		}
		d.Data[d.c.Tags[at]] = v.Interface()
	}
	return nil
}

func (c *Cache) asyncSave(ctx context.Context, key string, uuid string) {
	if DBNolog {
		ctx = utils.CtxSetNolog(ctx)
	}
	// 读取数据
	data := dirtyData{c: c}
	fields := []interface{}{}
	for _, tag := range c.condFields {
		fields = append(fields, c.GetRedisTagByTag(tag))
	}
	err := c.redis.Script(ctx, dirtyDataGetScript, []string{key}, fields...).Bind(&data)
	if err != nil && !goredis.IsNil(err) {
		utils.LogCtx(log.Error(), ctx).Err(err).Msgf("%s DirtyData error", c.msgHeadLog)
		return
	}
	if len(data.Data) > 0 {
		var condValues []interface{}
		for _, field := range c.condFields {
			condValues = append(condValues, data.Data[field])
			delete(data.Data, field)
		}
		// 保存到mysql
		err = c.saveToMySQL(ctx, NewConds().Eqs(c.condFields, condValues), data.Data)
		if err != nil {
			return // mysql保存失败，正在处理的脏数据列表不清理，等待超时，同时也防止了频繁的尝试
		}
	} else {
		// 脏数据为空了，连续存数据的情况下，可能会出现脏数据为空的情况，直接清理脏数据列表
	}

	// 清空脏标记
	ctx = utils.CtxSetNolog(ctx)
	c.redis.Script(ctx, dirtyKeyDoneScript, []string{c.dirtyKeyProcessing}, key, uuid)
	c.redis.Script(ctx, dirtyDataDoneScript, []string{key}, data.Version)
}
