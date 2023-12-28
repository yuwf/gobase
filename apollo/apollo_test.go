package apollo

import (
	"testing"

	"github.com/yuwf/gobase/loader"

	_ "github.com/yuwf/gobase/log"

	"github.com/rs/zerolog/log"
)

type ConfTest struct {
	Server_Name string `json:"server_name"`
	Server_Id   int    `json:"server_id"`
}

func (c *ConfTest) Normalize() {
	log.Info().Interface("Conf", c).Msg("Normalize")
}

var confTest loader.JsonLoader[ConfTest]
var strTest loader.StrLoader

func BenchmarkWatcher(b *testing.B) {
	c := &Config{
		Addr:      "127.0.0.1:8080",
		AppID:     "251",
		NameSpace: []string{"application"},
	}
	_, err := InitDefaultClient(c)
	if err != nil {
		return
	}
	defaultClient.Watch("application", "application", &confTest, false)
	log.Info().Interface("Conf", confTest.Get()).Msg("Get")

	defaultClient.Watch("application", "abcd", &strTest, false)

	select {}
}
