package alert

import (
	"testing"
	"time"

	_ "github.com/yuwf/gobase/log"

	"github.com/rs/zerolog/log"

	"github.com/yuwf/gobase/utils"
)

func BenchmarkAlert(b *testing.B) {

	LogAlertCheck = func(prefix string) bool {
		return false
	}
	ParamConf.Load([]byte(`{
		"servername":"Test-ServerName",
		"configs":[
			{
				"addr": "https://open.feishu.cn/open-apis/bot/v2/hook/611f80ae-1cc2-48c1-ba16-6ce105342947",
				"errorprefix":["TestLogAbc"]
			}
		]
	}`), "Path")

	defer utils.HandlePanic()
	InitAlert()
	log.Error().Msg("TestLogAbc Test Alert")
	time.Sleep(time.Second * 5)
	panic("kdjfkd")
}
