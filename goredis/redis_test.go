package goredis

// https://github.com/yuwf

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	_ "github.com/yuwf/gobase/log"
)

var cfg = &Config{
	Addrs: []string{"127.0.0.1:6379"},
}

var ccfg = &Config{
	Addrs:  []string{"47.112.182.246:6400", "47.112.182.246:6401", "47.112.182.246:6402"},
	Passwd: "clust@redis2023",
}

func BenchmarkRedis(b *testing.B) {
	redis, _ := NewRedis(cfg)
	if redis == nil {
		return
	}

	var v int

	type Test struct {
		F1 int `redis:"f1"`
		F2 int `redis:"f2"`
	}
	t1 := &Test{
		F1: 1,
		F2: 2,
	}
	t2 := &Test{}

	//var v int
	vcmd := redis.Do(context.TODO(), "get", "tttt")
	fmt.Println(GetFirstKeyPos(vcmd))

	pipe := redis.NewPipeline()
	pipe.Do(context.TODO(), "SET", "fdasdfd", "sdfsdfd", "PX", 10, "NX")
	pipe.Do(context.TODO(), "SET", "fdasdfd", "sdfsdfd", "PX", 10, "NX")
	pipe.HMSetObj(context.TODO(), "pipett", t1)
	pipe.HMGetObj(context.TODO(), "pipett", t2)
	pipe.Do(context.WithValue(context.TODO(), CtxKey_nonilerr, 1), "get", "sadfasdfasdf")
	pipe.Exec(context.TODO())

	redis.Set(context.TODO(), "tttt", "123", 0)

	//redis.Do(context.TODO(), "set", "tttt")

	redis.Do2(context.TODO(), "get", "tttt").Bind(&v)

	redis.HMSetObj(context.TODO(), "ht1", t1)
	redis.HMGetObj(context.TODO(), "ht1", t2)

	script := NewScript(`
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("SET", KEYS[1], ARGV[1])
		return redis.call("GET", KEYS[1])
	`)
	//var s string
	t := redis.DoScript(context.TODO(), script, []string{"script"}, "script---")
	fmt.Println(t.String())
}

func BenchmarkLock(b *testing.B) {
	redis, _ := NewRedis(cfg)
	if redis == nil {
		return
	}
	redis.Lock(context.TODO(), "testkey", time.Second*10)
	redis.Lock(context.TODO(), "testkey", time.Second*10)
	time.Sleep(time.Second * 10)
	fun, _ := redis.Lock(context.TODO(), "testkey", time.Second*10)
	fun()
	redis.Lock(context.TODO(), "testkey", time.Second*10)
}

func BenchmarkClusterRedis(b *testing.B) {
	redis, _ := NewRedis(ccfg)
	if redis == nil {
		return
	}

	redis.Set(context.TODO(), "tttt", "123", 0)

	//redis.Do(context.TODO(), "set", "tttt")

	//var v int
	vcmd := redis.Do(context.TODO(), "get", "tttt")
	fmt.Println(GetFirstKeyPos(vcmd))

	redis.Pipeline()

	type Test struct {
		F1 int `redis:"f1"`
		F2 int `redis:"f2"`
	}
	t1 := &Test{
		F1: 1,
		F2: 2,
	}
	t2 := &Test{}

	redis.HMSetObj(context.TODO(), "ht1", t1)
	redis.HMGetObj(context.TODO(), "ht1", t2)
}

func BenchmarkRedisWatchRegister(b *testing.B) {
	redis, _ := NewRedis(cfg)
	if redis == nil {
		return
	}

	infos := []*RegistryInfo{
		&RegistryInfo{
			RegistryName:   "Name",
			RegistryID:     "456",
			RegistryAddr:   "192.168.0.1",
			RegistryPort:   123,
			RegistryScheme: "tcp",
		},
		&RegistryInfo{
			RegistryName: "Name",
			RegistryID:   "123",
			RegistryAddr: "192.168.0.1",
			RegistryPort: 123,
		},
	}
	r := redis.CreateRegisterEx("testregister", infos)
	r.Reg()

	time.Sleep(time.Second * 3)
	r.DeReg()
	time.Sleep(time.Second * 3)
	r.Reg()
	//time.Sleep(time.Second * 1)
	//r.DeReg()
	//time.Sleep(time.Second * 1)
	//r.Reg()
}

func BenchmarkRedisWatchServices(b *testing.B) {
	redis, _ := NewRedis(cfg)
	if redis == nil {
		return
	}

	redis.WatchServices("testregister", nil, func(infos []*RegistryInfo) {
		log.Info().Interface("infos", infos).Msg("WatchServices")
	})

	select {}
}
