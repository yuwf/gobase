package mrcache

// https://github.com/yuwf/gobase

import (
	"errors"
	"sync"
	"time"
)

// 是否屏蔽DB层的日志，上层没有CtxKey_nolog前提下才生效
var DBNolog = true

// 读写单条数据时，不存在返回空错误，读取多条数据时不会返回空错误
var ErrNullData = errors.New("null")

// 结构中存储的tag名
const DBTag = "db"

// 存储Redis使用的tag，只有在初始化的NewCacheRow 或者 NewCacheRows是解析出来，为了减少Redis中field可能太长的问题
// 如果不设置默认是用的DBTag
const RedisTag = "redis"

var Expire = 36 * 3600 // 支持修改

var IncrementKey = "_mrcache_increment_" // 自增key，hash结构，field使用table名

type Options struct {
	noResp             bool // 不需要返回值，有时为了优化性能不需要返回值
	noExistCreate      bool // 不存在就创建
	jsonArrayDuplicate bool // JsonArray去重
}

func NewOptions() *Options {
	return &Options{}
}

func NoRespOptions() *Options {
	return NewOptions().NoResp()
}

func CreateOptions() *Options {
	return NewOptions().Create()
}

func (o *Options) NoResp() *Options {
	o.noResp = true
	return o
}
func (o *Options) Create() *Options {
	o.noExistCreate = true
	return o
}

func (o *Options) JsonArrayDuplicate() *Options {
	o.jsonArrayDuplicate = true
	return o
}

// 查询数据不存在的缓存，防止缓存穿透
// 这里的防的是穿透到mysql，redis层没防穿透逻辑
var passCache sync.Map
var passExpire = int64(8 * 1000)

func GetPass(key string) bool {
	passTime, ok := passCache.Load(key)
	if !ok {
		return false
	}
	if time.Now().UnixMilli()-passTime.(int64) >= passExpire {
		passCache.Delete(key)
		return false
	}
	return true
}

// 返回是否全部标记了
func GetPasss(keys []string) bool {
	for _, key := range keys {
		passTime, ok := passCache.Load(key)
		if !ok {
			return false
		}
		if time.Now().UnixMilli()-passTime.(int64) >= passExpire {
			passCache.Delete(key)
			return false
		}
	}
	return true
}

func SetPass(key string) {
	passCache.Store(key, time.Now().UnixMilli())
}

func SetPasss(keys []string) {
	for _, key := range keys {
		passCache.Store(key, time.Now().UnixMilli())
	}
}

func DelPass(key string) {
	passCache.Delete(key)
}
