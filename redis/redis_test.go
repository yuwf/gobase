package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/yuwf/gobase/log"

	"github.com/rs/zerolog/log"
)

var cfg = &Config{
	Addr: "127.0.0.1:6379",
}

func BenchmarkRedis(b *testing.B) {
	redis, err := InitDefaultRedis(cfg)
	if err != nil {
		return
	}
	ok, err := redis.DoCmdString(context.TODO(), "SET", "__key123__", "123")
	println(ok, err)
	v, err := redis.DoCmdInt(context.TODO(), "GET", "__key123__")
	println(v, err)
}

func BenchmarkRedisHMSetObj(b *testing.B) {
	redis, _ := NewRedis(cfg)
	if redis == nil {
		return
	}

	type Child struct {
		C1 int `json:"c1"`
		C2 int `json:"c2"`
	}

	type Test struct {
		F1 int            `redis:"f1"`
		F2 int            `redis:"f2"`
		F3 []int          `redis:"f3"`
		F4 map[string]int `redis:"f4"`
		C1 Child          `redis:"fc1"`
		C2 *Child         `redis:"fc2"`
	}
	t1 := &Test{
		F1: 1,
		F2: 2,
		//F3: []int{1, 2, 3},
		F4: map[string]int{"123": 123, "456": 456},
		C1: Child{C1: 1},
		C2: &Child{C2: 1},
	}

	t2 := &Test{}
	fmt.Printf("%v\n", t1)
	redis.HMSetObj(context.TODO(), "ht1", t1)
	redis.HMGetObj(context.TODO(), "ht1", t2)
	fmt.Printf("%v\n", t2)

	t3 := &Test{}
	pipe := redis.NewPipeline()
	pipe.HMSetObj("pipett", t1)
	pipe.HMGetObj("pipett", t3)
	pipe.Do(context.TODO())
	fmt.Printf("%v\n", t3)
}

func BenchmarkRedisScript(b *testing.B) {
	redis, err := InitDefaultRedis(cfg)
	if err != nil {
		return
	}
	script := NewScript(1, `
		--更新最大数
		local value = redis.call("GET", KEYS[1])
		if not value or tonumber(value) < tonumber(ARGV[1]) then
			redis.call("SET", KEYS[1], ARGV[1])
			return 1
		end
		return 0
	`)
	ok, err := redis.DoScript(context.TODO(), script, "__key123__", "12faskfjlkasjffffffffffffffffffffdddddddlksfjdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkdkd3", "fasfas", "asf", "1", "2", "3", "4", "5", "6")
	println(ok, err)
	ok, err = redis.DoScript(context.WithValue(context.TODO(), CtxKey_nolog, 123), script, "__key123__", "456")
	println(ok, err)
	v, err := redis.DoCmdInt(context.TODO(), "GET", "__key123__")
	println(v, err)
}

func BenchmarkRedisLock(b *testing.B) {
	redis, err := InitDefaultRedis(cfg)
	if err != nil {
		return
	}
	unlock2, err2 := redis.TryLock(context.TODO(), "__lock__", time.Second*30)
	//unlock2()
	if err2 == nil {
		defer unlock2()
	}

	unlock, err := redis.Lock(context.TODO(), "__lock__", time.Second*50)
	unlock()
}

func BenchmarkRedisFmt(b *testing.B) {
	redis, err := InitDefaultRedis(cfg)
	if err != nil {
		return
	}
	type Test struct {
		Time int64 `json:"t"` // 时间戳 秒
		Type int   `json:"type"`
	}
	//t := &Test{
	//	Time: 123456789,
	//	Type: 987654321,
	//}
	//data, _ := json.Marshal(t)

	redis.Do(context.TODO(), "SCAN", "0")
}

func BenchmarkRedisWatchRegister(b *testing.B) {
	redis, err := InitDefaultRedis(cfg)
	if err != nil {
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
	//r.Reg()
	//time.Sleep(time.Second * 1)
	//r.DeReg()
	//time.Sleep(time.Second * 1)
	//r.Reg()
}

func BenchmarkRedisWatchServices(b *testing.B) {
	redis, err := InitDefaultRedis(cfg)
	if err != nil {
		return
	}

	redis.WatchServices("testregister", nil, func(infos []*RegistryInfo) {
		log.Info().Interface("infos", infos).Msg("WatchServices")
	})

	select {}
}
