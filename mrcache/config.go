package mrcache

// https://github.com/yuwf/gobase

import (
	"errors"
	"sync"
)

var ErrNullData = errors.New("null") // 空数据，即没有数据

// 结构中存储的tag名
const DBTag = "db"

// Key前缀
var KeyPrefix = ""

var Expire = 36 * 3600 // 支持修改

var IncrementKey = "_mrcache_increment_" // 自增key，hash结构，field使用table名

// 查询数据不存在的缓存，防止缓存穿透
var passCache sync.Map

func IsPass(key string) bool {
	return false //  逻辑不合理 待完善
	// passTime, ok := passCache.Load(key)
	// if !ok {
	// 	return false
	// }
	// if passTime.(int64) >= time.Now().Unix()+30 {
	// 	passCache.Delete(key)
	// 	return false
	// }
	// return true
}

func SetPass(key string) {
	//passCache.Store(key, time.Now().Unix())
}
