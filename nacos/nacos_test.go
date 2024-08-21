package nacos

import (
	"testing"
	"time"

	"github.com/yuwf/gobase/loader"

	_ "github.com/yuwf/gobase/log"

	"github.com/rs/zerolog/log"
)

func BenchmarkGetConfig(b *testing.B) {
	cfg := Config{
		NamespaceId: "",
		Username:    "nacos",
		Password:    "nacos",
		Addrs:       []string{"localhost:8848"},
	}
	_, err := InitDefaultClient(&cfg)
	if err != nil {
		return
	}
	//defaultClient.DelConfig("test", "test")
	var l loader.StrLoader
	defaultClient.LoadConfig("test", "test", &l)
}

func BenchmarkSearchConfig(b *testing.B) {
	cfg := Config{
		NamespaceId: "",
		Username:    "nacos",
		Password:    "nacos",
		Addrs:       []string{"localhost:8848"},
	}
	_, err := InitDefaultClient(&cfg)
	if err != nil {
		return
	}
	// 带完善测试
	var l loader.StrLoaders
	defaultClient.LoadSearchConfig("m*", "test", &l)
	l.SaveDir("tt")
}

func BenchmarkListenConfig(b *testing.B) {
	cfg := Config{
		NamespaceId: "",
		Username:    "nacos",
		Password:    "nacos",
		Addrs:       []string{"localhost:8848"},
	}
	_, err := InitDefaultClient(&cfg)
	if err != nil {
		return
	}
	//defaultClient.DelConfig("test", "test")

	var l loader.StrLoader
	defaultClient.ListenConfig("test", "test", &l, true)
	//time.Sleep(time.Second * 10)
	//defaultClient.CancelListenConfig("test", "test")
	time.Sleep(time.Hour)
}

func BenchmarkRegister(b *testing.B) {
	cfg := Config{
		NamespaceId: "",
		Username:    "nacos",
		Password:    "nacos",
		Addrs:       []string{"localhost:8848"},
	}
	client, err := CreateClient(&cfg)
	if err != nil {
		return
	}
	t1 := &RegistryConfig{
		Ip:          "127.0.0.1",
		Port:        1001,
		ServiceName: "sname1",
		GroupName:   "gname",
		ClusterName: "a",
	}
	t2 := &RegistryConfig{
		Ip:          "127.0.0.1",
		Port:        1002,
		ServiceName: "sname1",
		GroupName:   "gname2",
		ClusterName: "a",
	}
	r1, err := client.CreateRegister(t1)
	if err != nil {
		return
	}
	r2, err := client.CreateRegister(t2)
	if err != nil {
		return
	}
	err = r1.Reg()
	if err != nil {
		return
	}
	err = r2.Reg()
	if err != nil {
		return
	}
	//defaultClient.Register("name2")
	time.Sleep(time.Hour * 10)
	//r.DeReg()
	//time.Sleep(time.Second * 10)
}

func BenchmarkRegister2(b *testing.B) {
	cfg := Config{
		NamespaceId: "",
		Username:    "nacos",
		Password:    "nacos",
		Addrs:       []string{"localhost:8848"},
	}
	client, err := CreateClient(&cfg)
	if err != nil {
		return
	}
	t1 := &RegistryConfig{
		Ip:          "127.0.0.1",
		Port:        2001,
		ServiceName: "sname2",
		GroupName:   "gname",
		ClusterName: "a",
	}
	r1, err := client.CreateRegister(t1)
	if err != nil {
		return
	}
	err = r1.Reg()
	if err != nil {
		return
	}
	//defaultClient.Register("name2")
	time.Sleep(time.Hour * 10)
	//r.DeReg()
	//time.Sleep(time.Second * 10)
}

func BenchmarkListenService(b *testing.B) {
	cfg := Config{
		NamespaceId: "",
		Username:    "nacos",
		Password:    "nacos",
		Addrs:       []string{"localhost:8848"},
	}
	_, err := InitDefaultClient(&cfg)
	if err != nil {
		return
	}
	//serviceNames, _ := defaultClient.GetAllServicesInfo("gname")
	//log.Info().Strs("serviceNames", serviceNames).Msg("GetAllServicesInfo")
	//regs, _ := defaultClient.SelectInstances("sname1", "gname", []string{"a"})
	//log.Info().Interface("regs", regs).Msg("SelectInstances")
	defaultClient.ListenService("sname1", "gname", []string{"a"}, func(infos []*RegistryInfo) {
		log.Info().Interface("infos", infos).Msg("Regs")
	})
	time.Sleep(time.Hour)
}

func BenchmarkListenService2(b *testing.B) {
	cfg := Config{
		NamespaceId: "",
		Username:    "nacos",
		Password:    "nacos",
		Addrs:       []string{"localhost:8848"},
	}
	_, err := InitDefaultClient(&cfg)
	if err != nil {
		return
	}
	defaultClient.ListenService2("sname1", "gname", []string{"a"}, func(addInfos, delInfos []*RegistryInfo) {
		log.Info().Interface("regs", addInfos).Msg("Add")
		log.Info().Interface("regs", delInfos).Msg("Del")
	})
	time.Sleep(time.Hour)
}

func BenchmarkListenServices(b *testing.B) {
	cfg := Config{
		NamespaceId: "",
		Username:    "nacos",
		Password:    "nacos",
		Addrs:       []string{"localhost:8848"},
	}
	_, err := InitDefaultClient(&cfg)
	if err != nil {
		return
	}
	defaultClient.ListenServices([]string{"sname1", "sname2"}, "gname", []string{"a"}, func(infos []*RegistryInfo) {
		log.Info().Interface("infos", infos).Msg("Regs")
	})
	time.Sleep(time.Hour)
}

func BenchmarkListenServices2(b *testing.B) {
	cfg := Config{
		NamespaceId: "",
		Username:    "nacos",
		Password:    "nacos",
		Addrs:       []string{"localhost:8848"},
	}
	_, err := InitDefaultClient(&cfg)
	if err != nil {
		return
	}
	defaultClient.ListenServices2([]string{"sname1", "sname2"}, "gname", []string{"a"}, func(addInfos, delInfos []*RegistryInfo) {
		log.Info().Interface("regs", addInfos).Msg("Add")
		log.Info().Interface("regs", delInfos).Msg("Del")
	})
	time.Sleep(time.Hour)
}
