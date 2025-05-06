package utils

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/petermattis/goid"
)

func BenchmarkDelete(b *testing.B) {
	t := []int{30}
	t2 := Delete(t, 20)
	fmt.Println(t, t2)
}

func BenchmarkRandStr(b *testing.B) {
	fmt.Println(RandString(32))
	time.Sleep(time.Second * 10)
}

func BenchmarkAnts(b *testing.B) {
	for i := 0; i < 20; i++ {
		n := i
		Submit(func() {
			fmt.Println(n)
		})
	}
	time.Sleep(time.Second * 10)
}

func BenchmarkSequence(b *testing.B) {
	var seq Sequence
	for i := 0; i < 20; i++ {
		n := i
		seq.Submit(func() {
			if n == 10 {
				panic("no") // 不会输出10
			}
			time.Sleep(time.Second)
			fmt.Println(n)
		})
	}
	time.Sleep(time.Minute)
}

func GetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func BenchmarkGID(b *testing.B) {
	entry := time.Now()
	for i := 0; i < 1000000; i++ {
		GetGID()
	}
	fmt.Println("1=", time.Since(entry))

	entry = time.Now()
	for i := 0; i < 1000000; i++ {
		goid.Get()
	}
	fmt.Println("2=", time.Since(entry))
}

func BenchmarkGroupSequence1(b *testing.B) {
	var seq GroupSequence
	for i := 0; i < 20; i++ {
		n := i
		seq.Submit(RandString(10), func() {
			fmt.Println(goid.Get(), n)
		})
	}
	time.Sleep(time.Second * 10)
}

func BenchmarkGroupSequence2(b *testing.B) {
	var seq GroupSequence
	for i := 0; i < 20; i++ {
		n := i
		seq.Submit("my", func() {
			if n == 10 {
				panic("no") // 不会输出10
			}
			time.Sleep(time.Second)
			fmt.Println("my", n)
		})
	}
	for i := 0; i < 20; i++ {
		n := i
		seq.Submit("my2", func() {
			time.Sleep(time.Second * 2)
			fmt.Println("    my2", n)
		})
	}
	time.Sleep(time.Minute)
}

func BenchmarkGroupSequenceDebug(b *testing.B) {
	var seq GroupSequence
	key := RandString(10)
	wg := sync.WaitGroup{}
	wg.Add(1)
	seq.Submit(key, func() {
		fmt.Println(goid.Get(), "begin")
		time.Sleep(time.Second * 4)
		fmt.Println(goid.Get(), "end")
		wg.Done()
	})
	wg.Wait()
	seq.Submit(key, func() {
		fmt.Println(goid.Get(), "do it")
	})
	time.Sleep(time.Second * 10)
}

func BenchmarkProcess(b *testing.B) {
	SubmitProcess(func() {
		time.Sleep(time.Second * 10)
		fmt.Println("Sleep Over")
		//Exit(0)
	})
	time.Sleep(time.Second)
	//ExitWait()
	Exit(0)
}

func BenchmarkLocalIP(b *testing.B) {
	LocalIPString()
}

func BenchmarkStructReflect(b *testing.B) {
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

	t := Test{
		F1:  5,
		F2:  0,
		F3:  "test1 test2",
		F4:  []byte{'t', 'e', 's', 't', '1', '0', 't', 't'},
		F5:  make(chan interface{}),
		F7:  &Head{HF: "123"},
		F8:  map[string]interface{}{"k": "v"},
		F99: &Head{HF: "123"},
	}

	sInfo, err := GetStructInfoByTag(t, "redis")
	v := sInfo.TagsSlice()

	fmt.Println(len(v), v, err)
}

func BenchmarkNTP(b *testing.B) {
	for i := 0; i < 1000; i++ {
		NtpTime()
		fmt.Println(ntpServers[0].Addr, ntpServers[0].avgTime)
	}
}

func BenchmarkStruct(b *testing.B) {
	type Test struct {
		Id         int       `db:"Id" json:"Id,omitempty"`                             //自增住建  不可为空
		CreateTime time.Time `db:"create_time" redis:"ct" json:"CreateTime,omitempty"` //用户ID  redis 记录ct
		UpdateTime time.Time `db:"update_time" redis:"ut" json:"UpdateTime,omitempty"` //用户ID  redis 记录ut
		UID        int       `db:"UID" redis:"U" json:"UID,omitempty"`                 //用户ID  redis 记录U
		Type       int       `db:"Type" json:"Type,omitempty"`                         //用户ID  不可为空
		Name       string    `db:"Name" json:"Name,omitempty"`                         //名字  不可为空
		Age        int       `db:"Age" json:"Age,omitempty"`                           //年龄
		Mark       *string   `db:"Mark" json:"Mark,omitempty"`                         //标记 可以为空
	}

	st, _ := GetStructTypeByTag[Test]("db")

	t := &Test{Id: 100, Name: "abc"}
	ts, _ := GetStructInfoByStructType(t, st)
	t.Age = 123
	fmt.Println(ts.ElemsSlice())

	entry := time.Now()
	for i := 0; i < 100000; i++ {
		t := &Test{Id: 100, Name: "abc"}
		st.InstanceElemsSlice(t)
	}
	fmt.Println(time.Since(entry))

	entry = time.Now()
	for i := 0; i < 100000; i++ {
		t := &Test{Id: 100, Name: "abc"}
		st.InstanceElemsSliceUnSafe(unsafe.Pointer(t))
	}
	fmt.Println(time.Since(entry))

	entry = time.Now()
	for i := 0; i < 100000; i++ {
		t := &Test{Id: 100, Name: "abc"}
		ts, _ := GetStructInfoByStructType(t, st)
		ts.ElemsSlice()
	}
	fmt.Println(time.Since(entry))

	entry = time.Now()
	for i := 0; i < 100000; i++ {
		t := &Test{Id: 100, Name: "abc"}
		ts, _ := GetStructInfoByTag(t, "db")
		ts.ElemsSlice()
	}
	fmt.Println(time.Since(entry))

}
