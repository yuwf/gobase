package goredis

// https://github.com/yuwf/gobase

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

// redis扩展脚本

//go:embed json.lua
var LuaJsonScript string

//go:embed md5.lua
var LuaMd5Script string

type RedisScript struct {
	name   string
	script *redis.Script
}

func NewScript(src string) *RedisScript {
	ret := &RedisScript{
		script: redis.NewScript(src),
	}
	return ret
}

func NewScriptWithName(name, src string) *RedisScript {
	ret := &RedisScript{
		name:   name,
		script: redis.NewScript(src),
	}
	return ret
}
