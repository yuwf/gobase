package consul

import (
	"fmt"
	"os"
	"testing"
	"time"

	"gobase/loader"

	"github.com/rs/zerolog/log"
)

type ConfTest struct {
	LocalPath string `json:"localPath"`
	LocalIp   string `json:"localIp"`
}

func (c *ConfTest) Normalize() {
	log.Info().Interface("Conf", c).Msg("Normalize")
}

var confMapTest loader.JsonLoaders[ConfTest]
var confTest loader.JsonLoader[ConfTest]
var AppPrivateKey loader.StrLoader

func BenchmarkWatcherKV(b *testing.B) {
	_, err := InitDefaultClient("127.0.0.1:8500", "http")
	if err != nil {
		return
	}
	// 配置修改回调
	confTest.RegHook(func(old, new *ConfTest) {
		log.Info().Interface("Conf", new).Msg("Hook")
	})
	DefaultClient().WatchKV("atlas/nodeIds/node_3", &confTest, true)
	log.Info().Interface("Conf", confTest.Get()).Msg("Get")

	var confTest2 loader.JsonLoader[ConfTest]
	confTest2.LoadBy(confTest.Get())

	DefaultClient().WatchKV("lsmatch/live-douyin/app_private_key.pem", &AppPrivateKey, true)
	log.Info().Interface("Conf", AppPrivateKey.Get()).Msg("Get")

	select {}
}

func BenchmarkWatcherListKV(b *testing.B) {
	_, err := InitDefaultClient("127.0.0.1:8500", "http")
	if err != nil {
		return
	}
	DefaultClient().WatchListKV("atlas/nodeIds", &confMapTest, true)
	log.Info().Interface("Conf", confMapTest.Get()).Msg("Get")
	DefaultClient().WatchListKV("atlas/nodeIds", &confMapTest, true)
	select {}
}

func BenchmarkRegister(b *testing.B) {
	_, err := InitDefaultClient("127.0.0.1:8500", "http")
	if err != nil {
		return
	}
	hostname, _ := os.Hostname()
	conf := &RegistryConfig{
		RegistryName: "live-gate",
		RegistryID:   "serverId-serverName-nodeId",
		RegistryAddr: "localhost",
		RegistryPort: 9500,
		RegistryTag:  []string{"prometheus"},
		RegistryMeta: map[string]string{
			"hostname":    hostname,
			"metricsPath": "/metrics",
			"metricsPort": "9100",
		},
		HealthPort:     9502, //consul内部自己开启监听
		HealthPath:     "/health",
		HealthInterval: 4,
		HealthTimeout:  4,
		DeregisterTime: 4,
	}
	reg, err := DefaultClient().CreateRegister(conf)
	if err == nil {
		err = reg.Reg()
		if err != nil {
			return
		}
		time.Sleep(time.Minute)
	}
	reg.DeReg()
	time.Sleep(time.Minute)
}

func BenchmarkWatcherService(b *testing.B) {
	_, err := InitDefaultClient("127.0.0.1:8500", "http")
	if err != nil {
		return
	}
	hostname, _ := os.Hostname()
	conf := &RegistryConfig{
		RegistryName: "test-serviceName",
		RegistryID:   "test-serviceId-nodeId22",
		RegistryAddr: "localhost",
		RegistryPort: 9500,
		RegistryTag:  []string{"gate"},
		RegistryMeta: map[string]string{
			"hostname":    hostname,
			"metricsPath": "/metrics",
			"metricsPort": "9100",
		},
		HealthPort:     9502, //consul内部自己开启监听
		HealthPath:     "/health",
		HealthInterval: 4,
		HealthTimeout:  4,
		DeregisterTime: 4,
	}
	DefaultClient().WatchServices("gate", func(services []*RegistryInfo) {
		fmt.Println("service change")
		for _, r := range services {
			fmt.Printf("%v\n", *r)
		}
		fmt.Println("")
	})
	reg, err := DefaultClient().CreateRegister(conf)
	reg.Reg()
	time.Sleep(time.Second * 10)
	conf.RegistryID = "test-serviceId-nodeId23"
	conf.HealthPort = 9503
	reg, err = DefaultClient().CreateRegister(conf)
	reg.Reg()
	for {
		time.Sleep(time.Second * 10)
		reg.DeReg()
		time.Sleep(time.Second * 10)
		reg.Reg()
	}
}
