package redis

import (
	"context"
	"testing"
	"time"

	_ "gobase/log"

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

func BenchmarkPipeline(b *testing.B) {
	redis, err := InitDefaultRedis(cfg)
	if err != nil {
		return
	}
	type Test struct {
		F1 int `redis:"f1"`
		F2 int `redis:"f2"`
	}
	var t Test
	t.F1 = 1230
	t.F2 = 4560
	var rt1 Test
	var rt2 Test
	redis.HMSetObj(context.TODO(), "testmap", &t)
	redis.HMGetObj(context.TODO(), "testmap", &rt1)

	pipeline := redis.NewPipeline()
	pipeline.HMGetObj("testmap", &rt1)
	pipeline.Cmd("HMGET", "testmap").HMGetBindObj(&rt2)
	cmds, err := pipeline.DoEx(context.TODO())

	log.Info().Interface("cmds", cmds).Msg("Pipeline")
	//redis.HMSetObj(context.TODO(), "testmap2", &t)
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
	ok, err = redis.DoScript(context.WithValue(context.TODO(), "nolog", 123), script, "__key123__", "456")
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
