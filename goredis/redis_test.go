package goredis

// https://github.com/yuwf/gobase

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	_ "github.com/yuwf/gobase/log"

	"github.com/rs/zerolog/log"
)

var cfg = &Config{
	Addrs:  []string{"127.0.0.1:6379"},
	Passwd: "",
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

	//var v int
	vcmd := redis.Do(context.TODO(), "get", "tttt")
	fmt.Println(GetFirstKeyPos(vcmd))

	pipe := redis.NewPipeline()
	pipe.Do(context.TODO(), "SET", "fdasdfd", "sdfsdfd", "PX", 10, "NX")
	pipe.Do(context.TODO(), "SET", "fdasdfd", "sdfsdfd", "PX", 10, "NX")

	pipe.Do(context.WithValue(context.TODO(), CtxKey_nonilerr, 1), "get", "sadfasdfasdf")
	pipe.Exec(context.TODO())

	redis.Set(context.TODO(), "tttt", "123", 0)

	//redis.Do(context.TODO(), "set", "tttt")

	redis.Do2(context.TODO(), "get", "tttt").Bind(&v)

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
	fmt.Println(t.Text())
}

func BenchmarkPipelineScript(b *testing.B) {
	redis, _ := NewRedis(cfg)
	if redis == nil {
		return
	}

	pipe := redis.NewPipeline()

	script := NewScript(`
		redis.call("SET", KEYS[1], ARGV[1])
		return redis.call("GET", KEYS[1])
	`)

	var rst string
	//pipe.Script2(context.TODO(), script, []string{"script"}, "script---").Bind(&rst)
	cmd := pipe.Script(context.TODO(), script, []string{"script"}, "script---")

	pipe.Exec(context.TODO())
	rst, _ = cmd.Text()

	fmt.Println(rst)
}

func BenchmarkRedisFMT(b *testing.B) {
	redis, _ := NewRedis(cfg)
	if redis == nil {
		return
	}
	type Head struct {
		HF string `json:"UID,omitempty"`
	}
	type Test struct {
		F1  int                    `redis:"f1"`
		F11 *int                   `redis:"f11"`
		F2  float32                `redis:"f2"`
		F22 *float32               `redis:"f22"`
		F3  string                 `redis:"f3"`
		F33 *string                `redis:"f33"`
		F4  []byte                 `redis:"f4"`
		F44 []byte                 `redis:"f44"`
		F5  chan interface{}       `redis:"f5"`
		F6  [6]int                 `redis:"f6"`
		F7  interface{}            `redis:"f7"`
		F77 interface{}            `redis:"f77"`
		F8  map[string]interface{} `redis:"f8"`
		F88 map[string]interface{} `redis:"f88"`
		F9  Head                   `redis:"f9"`
		F99 *Head                  `redis:"f99"`
	}

	t1 := &Test{
		F1:  5,
		F2:  0,
		F3:  "test1 test2",
		F4:  []byte{'t', 'e', 's', 't', '1', '0', 't', 't'},
		F5:  make(chan interface{}),
		F7:  &Head{HF: "123"},
		F8:  map[string]interface{}{"k": "v"},
		F99: &Head{HF: "123"},
	}

	t2 := &Test{}

	redis.HMSetObj(context.TODO(), "fmtt", t1)
	redis.HMGetObj(context.TODO(), "fmtt", t2)

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
		F1 int            `redis:"f1,ff"`
		F2 int            `redis:"f2"`
		F3 []int          `redis:"f3"`
		F4 map[string]int `redis:"f4"`
		C1 Child          `redis:"fc1"`
		C2 *Child         `redis:"fc2"`
	}
	t1 := &Test{
		F1: 1,
		F2: 2,
		F3: []int{1, 2, 3},
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
	pipe.HMSetObj(context.TODO(), "pipett", t1)
	pipe.HMGetObj(context.TODO(), "pipett", t3)
	pipe.Exec(context.TODO())
	fmt.Printf("%v\n", t3)
}

func BenchmarkTryLockWait(b *testing.B) {
	redis, _ := NewRedis(cfg)
	if redis == nil {
		return
	}
	redis.Lock(context.TODO(), "testkey", time.Second*5)
	fun, err := redis.TryLockWait(context.TODO(), "testkey", time.Second*10)
	fmt.Printf("%p %v\n", fun, err)
}

func BenchmarkKeyLockWait(b *testing.B) {
	redis, _ := NewRedis(cfg)
	if redis == nil {
		return
	}
	//f, _ := redis.Lock(context.TODO(), "testkeylock", time.Second*5)
	//go func() {
	//	time.Sleep(time.Second*3)
	//	f()
	//	fmt.Println("delete")
	//}()
	//fun, err := redis.KeyLockWait(context.TODO(), "testkey", "testkeylock", time.Second*10)

	fun, err := redis.KeyLockWait(context.TODO(), "testkey2", "testkeylock2", time.Second*20)
	fmt.Printf("%p %v\n", fun, err)
	time.Sleep(time.Second * 5)
	if fun != nil {
		fun()
	}
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
		{
			RegistryName:   "Name",
			RegistryID:     "123",
			RegistryAddr:   "192.168.0.1",
			RegistryPort:   123,
			RegistryScheme: "tcp",
		},
		{
			RegistryName: "Name",
			RegistryID:   "456",
			RegistryAddr: "192.168.0.1",
			RegistryPort: 456,
		},
	}
	r := redis.CreateRegisters("testregister", infos)
	r.Reg()

	time.Sleep(time.Second * 10)
	r.Add(&RegistryInfo{
		RegistryName: "Name",
		RegistryID:   "789",
		RegistryAddr: "192.168.0.1",
		RegistryPort: 789,
	})
	time.Sleep(time.Second * 5)
	r.Remove(&RegistryInfo{
		RegistryName: "Name",
		RegistryID:   "456",
		RegistryAddr: "192.168.0.1",
		RegistryPort: 456,
	})
	time.Sleep(time.Second * 5)
	r.DeReg()
	time.Sleep(time.Second * 5)
	r.Reg()
	time.Sleep(time.Second * 5)
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

func BenchmarkInterfaceToValue(b *testing.B) {
	type S struct {
		F1  int         `json:"f1"`
		Fs  string      `json:"fs"`
		Fbs []byte      `json:"fbs"`
		Fi  interface{} `json:"fi"`
		// Fc  chan interface{} `json:"fc"` 不支持json化
	}
	s := S{
		F1:  1,
		Fs:  "fss",
		Fbs: []byte{'1', '2', '3'},
		Fi:  map[string]string{"11": "22"},
		// Fc: make(chan interface{}),
	}
	var arr = [...]byte{'a', 'b', 'c'}
	var str string

	//
	sli1 := []byte{'a', 'b'}
	sli2 := []byte{}
	err := InterfaceToValue(sli1, reflect.ValueOf(&sli2))
	fmt.Println(err, sli2)

	// Array to String
	err = InterfaceToValue(&arr, reflect.ValueOf(&str))
	fmt.Println(err, str)

	// Struct to
	err = InterfaceToValue(&s, reflect.ValueOf(&str))
	fmt.Println(err, str)
	ss := S{}
	err = InterfaceToValue(str, reflect.ValueOf(&ss))
	fmt.Println(err, ss)

	var i *int
	err = InterfaceToValue(str, reflect.ValueOf(&i).Elem())
	fmt.Println(err, ss)
	i = nil
	InterfaceToValue(str, reflect.ValueOf(i)) // 这种写法会崩溃

}
