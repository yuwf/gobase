package loader

// https://github.com/yuwf/gobase

import (
	"testing"
	"time"

	_ "github.com/yuwf/gobase/log"
)

type ConfTest struct {
	Server_Name string `json:"server_name"`
	Server_Id   int    `json:"server_id"`
}

var confTest JsonLoader[ConfTest]

func BenchmarkRedis(b *testing.B) {

	watch, _ := NewLocalWatch()

	watch.ListenFile("F:/ProjectAir_Server/datatable/tt/test.json", &confTest, false)
	watch.ListenFile("F:/ProjectAir_Server/datatable/tt/test.json", &confTest, false)

	time.Sleep(time.Second * 5)

	watch.Close()

	watch.ListenFile("F:/ProjectAir_Server/datatable/tt/test.json", &confTest, false)

	time.Sleep(time.Hour)

}
