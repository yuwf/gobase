package goredis

// https://github.com/yuwf/gobase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "github.com/yuwf/gobase/log"
	"github.com/yuwf/gobase/utils"

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

	pipe.Do(context.TODO(), "get", "sadfasdfasdf")
	pipe.Exec(context.TODO())

	redis.Set(context.TODO(), "tttt", "123", 0)

	//redis.Do(context.TODO(), "set", "tttt")

	redis.Cmd(context.TODO(), "get", "tttt").Bind(&v)

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
	t := redis.Script(context.TODO(), script, []string{"script"}, "script---")
	fmt.Println(t.Text())
}

func BenchmarkSubscribe(b *testing.B) {
	redis, _ := NewRedis(cfg)
	if redis == nil {
		return
	}
	pubsub := redis.CreateSubscribe(context.TODO())
	pubsub.Subscribe(context.TODO(), "test")
	go func() {
		time.Sleep(time.Second * 1)
		redis.Publish(context.TODO(), "test", "aaa")
		time.Sleep(time.Second * 5)
		redis.Publish(context.TODO(), "test", "close")
	}()
	ch := pubsub.Channel()
	for msg := range ch {
		fmt.Println(msg)
		if msg.Payload == "close" {
			pubsub.Close()
			fmt.Println(pubsub.Receive(context.TODO()))
		}
	}

	pubsub2 := redis.CreateSubscribe(context.TODO())
	pubsub2.Subscribe(context.TODO(), "test")
	go func() {
		time.Sleep(time.Second * 1)
		redis.Publish(context.TODO(), "test", "aaa")
		time.Sleep(time.Second * 5)
		redis.Publish(context.TODO(), "test", "close")
	}()
	for {
		msg, err := pubsub2.ReceiveMessage(context.TODO())
		fmt.Println(msg, err)
		if err == nil {
			break
		}
		if msg.Payload == "close" {
			pubsub2.Close()
		}
	}
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

func BenchmarkRedisJson(b *testing.B) {
	redis, _ := NewRedis(cfg)
	if redis == nil {
		return
	}
	type Head struct {
		HF string `json:"UID,omitempty"`
	}
	type Test struct {
		F1  int                    `json:"f1,omitempty"`
		F11 *int                   `json:"f11,omitempty"`
		F2  float32                `json:"f2,omitempty"`
		F22 *float32               `json:"f22,omitempty"`
		F3  string                 `json:"f3,omitempty"`
		F33 *string                `json:"f33,omitempty"`
		F4  []byte                 `json:"f4,omitempty"`
		F44 []byte                 `json:"f44,omitempty"`
		F6  [6]int                 `json:"f6,omitempty"`
		F7  interface{}            `json:"f7,omitempty"`
		F77 interface{}            `json:"f77,omitempty"`
		F8  map[string]interface{} `json:"f8,omitempty"`
		F88 map[string]interface{} `json:"f88,omitempty"`
		F9  Head                   `json:"f9,omitempty"`
		F99 *Head                  `json:"f99,omitempty"`
	}

	t1 := &Test{
		F1:  5,
		F2:  0,
		F3:  "test1 test2",
		F4:  []byte{'t', 'e', 's', 't', '1', '0', 't', 't'},
		F7:  &Head{HF: "123"},
		F8:  map[string]interface{}{"k": "v"},
		F99: &Head{HF: "123"},
	}

	t2 := &Test{}

	redis.SetJson(context.TODO(), "json_test", t1)
	redis.HSetJson(context.TODO(), "json_test_dic", "f", t1)

	rv := map[string]*Test{}
	redis.Cmd(context.TODO(), "hgetall", "json_test_dic").BindJsonObjMap(&rv)

	pipeline := redis.NewPipeline()
	pipeline.SetJson(context.TODO(), "json_test", t1)
	pipeline.HSetJson(context.TODO(), "json_test_dic", "f", t1)
	rv2 := map[string]*Test{}
	pipeline.Cmd(context.TODO(), "hgetall", "json_test_dic").BindJsonObjMap(&rv2)
	pipeline.Exec(context.TODO())

	redis.HMGetObj(context.TODO(), "fmtt", t2)

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
			Name:   "Name",
			ID:     "123",
			Addr:   "192.168.0.1",
			Port:   123,
			Scheme: "tcp",
		},
		{
			Name: "Name",
			ID:   "456",
			Addr: "192.168.0.1",
			Port: 456,
		},
	}
	r := redis.CreateRegisters("testregister", infos)
	r.Reg()

	time.Sleep(time.Second * 10)
	r.Add(&RegistryInfo{
		Name: "Name",
		ID:   "789",
		Addr: "192.168.0.1",
		Port: 789,
	})
	time.Sleep(time.Second * 5)
	r.Remove(&RegistryInfo{
		Name: "Name",
		ID:   "456",
		Addr: "192.168.0.1",
		Port: 456,
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

	redis.WatchServices("testregister", nil, func(ctx context.Context, infos []*RegistryInfo) {
		utils.LogCtx(log.Info(), ctx).Interface("infos", infos).Msg("WatchServices")
	})

	select {}
}

func BenchmarkRedisWatchServicesExist(b *testing.B) {
	redis, _ := NewRedis(cfg)
	if redis == nil {
		return
	}

	watch, _ := redis.WatchServices("testregister", nil, func(ctx context.Context, infos []*RegistryInfo) {
		utils.LogCtx(log.Info(), ctx).Interface("infos", infos).Msg("WatchServices")
	})

	go func() {
		time.Sleep(time.Second * 10)
		watch.Close()
	}()

	select {}
}

// 道具基础信息，按kid%100尾号分表
type TestItem struct {
	Kid        int64     `db:"kid" json:"kid,omitempty"`                            // 主键ID，也是道具ID
	CreateTime time.Time `db:"create_time" json:"create_time,omitempty" redis:"ct"` // 创建时间
	Tid        int32     `db:"tid" json:"tid,omitempty"`                            // 模板ID
	Num        int32     `db:"num" json:"num,omitempty"`                            // 道具数量
	BindKid    int64     `db:"bind_kid" json:"bind_kid,omitempty"`                  // 拥有者kid 0表示未绑定
}

func (i *TestItem) RedisUnmarshal(reply any) error {
	switch r := reply.(type) {
	case string:
		return json.Unmarshal([]byte(r), i)
	case []byte:
		return json.Unmarshal(r, i)
	default:
		return errors.New("invalid type")
	}
}

func (i *TestItem) RedisMarshal() (interface{}, error) {
	return json.Marshal(i)
}

func BenchmarkReplyToValue(b *testing.B) {
	redis, _ := NewRedis(cfg)
	if redis == nil {
		return
	}

	type Test struct {
		F1 int `redis:"f1"`
		F2 int `redis:"f2"`
	}

	// string to slice
	var sli1 = []byte{}
	var sli2 []byte
	var sli3 []byte
	var test Test
	var inter interface{}
	redis.Set(context.TODO(), "_test_str_", "aabb", 0)
	redis.Set(context.TODO(), "_test_str_empty_", "", 0)
	err1 := redis.Cmd(context.TODO(), "get", "_test_str_").Bind(&sli1)
	err2 := redis.Cmd(context.TODO(), "get", "_test_str_").Bind(&sli2)
	err3 := redis.Cmd(context.TODO(), "get", "_test_str_empty_").Bind(&sli3)      // 空字符串
	err4 := redis.Cmd(context.TODO(), "get", "_test_str_empty_").Bind(any(&test)) // 空字符串 会绑定失败
	err5 := redis.Cmd(context.TODO(), "get", "_test_str_").Bind(&inter)           // inter直接绑定值
	fmt.Println(err1, sli1, err2, sli2, err3, sli3, err4, test, err5, inter)

	var map1 = map[string]int64{"a": 1, "b": 2}
	var map2 map[string]int64
	var inters []interface{}
	var map3 map[interface{}]interface{}
	redis.HMSet(context.TODO(), "_test_ht_", "f1", 1, "f2", 2)
	err1 = redis.Cmd(context.TODO(), "hgetall", "_test_ht_").Bind(&map1)
	err2 = redis.Cmd(context.TODO(), "hgetall", "_test_ht_").Bind(&map2)
	err3 = redis.Cmd(context.TODO(), "hgetall", "_test_ht_").Bind(&inters)
	err4 = redis.Cmd(context.TODO(), "hgetall", "_test_ht_").Bind(&map3)
	fmt.Println(err1, map1, err2, map2, err3, inters, err4, map3)
}
