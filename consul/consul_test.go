package consul

import (
	"os"
	"testing"
	"time"

	"gobase/loader"
	_ "gobase/log"

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
		RegistryName: "test-name",
		RegistryID:   "test-id",
		RegistryAddr: "localhost",
		RegistryPort: 9500,
		RegistryTag:  []string{"test-tag"},
		RegistryMeta: map[string]string{
			"hostname":          hostname,
			"metricsPath":       "/metrics",
			"metricsPort":       "9100",
			"healthListenReuse": "yes",
		},
		HealthPort:     9502, //consul内部自己开启监听
		HealthPath:     "/health",
		HealthInterval: 4,
		HealthTimeout:  4,
		DeregisterTime: 4,
	}
	reg, err := DefaultClient().CreateRegister(conf)
	if err == nil {
		for {
			log.Info().Msg("")
			err = reg.Reg()
			if err != nil {
				continue
			}
			time.Sleep(time.Second * 10)
			reg.DeReg()
		}
	}
}

func BenchmarkWatcherServices(b *testing.B) {
	_, err := InitDefaultClient("127.0.0.1:8500", "http")
	if err != nil {
		return
	}
	DefaultClient().WatchServices("test-tag", func(infos []*RegistryInfo) {
		log.Info().Msg("service change")
		for i, r := range infos {
			log.Info().Interface("service", r).Msgf("infos %d", i)
		}
	})
	select {}
}

func BenchmarkWatcherServices2(b *testing.B) {
	_, err := InitDefaultClient("127.0.0.1:8500", "http")
	if err != nil {
		return
	}
	DefaultClient().WatchServices2("test-tag", func(addInfos, delInfos []*RegistryInfo) {
		log.Info().Msg("service change")
		for i, r := range addInfos {
			log.Info().Interface("service", r).Msgf("addInfos %d", i)
		}
		for i, r := range delInfos {
			log.Info().Interface("service", r).Msgf("delInfos %d", i)
		}
	})
	select {}
}

func BenchmarkWatcherServiceServices(b *testing.B) {
	_, err := InitDefaultClient("127.0.0.1:8500", "http")
	if err != nil {
		return
	}
	DefaultClient().WatchServiceServices("test-name", "test-tag", func(infos []*RegistryInfo) {
		log.Info().Msg("service change")
		for i, r := range infos {
			log.Info().Interface("service", r).Msgf("infos %d", i)
		}
	})
	select {}
}

func BenchmarkWatcherServiceServices2(b *testing.B) {
	_, err := InitDefaultClient("127.0.0.1:8500", "http")
	if err != nil {
		return
	}
	DefaultClient().WatchServiceServices2("test-name", "test-tag", func(addInfos, delInfos []*RegistryInfo) {
		log.Info().Msg("service change")
		for i, r := range addInfos {
			log.Info().Interface("service", r).Msgf("addInfos %d", i)
		}
		for i, r := range delInfos {
			log.Info().Interface("service", r).Msgf("delInfos %d", i)
		}
	})
	select {}
}
