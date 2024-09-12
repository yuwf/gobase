package mrcache

// https://github.com/yuwf/gobase

import (
	"errors"
	"sync"
	"time"
)

var ErrNullData = errors.New("null") // 空数据，即没有数据

// 结构中存储的tag名
const DBTag = "db"

// 存储Redis使用的tag，只有在初始化的NewCacheRow 或者 NewCacheRows是解析出来，为了减少Redis中field可能太长的问题
// 如果不设置默认是用的DBTag
const RedisTag = "redis"

var Expire = 36 * 3600 // 支持修改

var IncrementKey = "_mrcache_increment_" // 自增key，hash结构，field使用table名

// 查询数据不存在的缓存，防止缓存穿透
var passCache sync.Map

func GetPass(key string) bool {
	passTime, ok := passCache.Load(key)
	if !ok {
		return false
	}
	if time.Now().Unix() - passTime.(int64) >= 8 {
		passCache.Delete(key)
		return false
	}
	return true
}

func SetPass(key string) {
	passCache.Store(key, time.Now().Unix())
}

func DelPass(key string) {
	passCache.Delete(key)
}
