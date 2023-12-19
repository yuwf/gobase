package alert

import (
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	_ "github.com/yuwf/gobase/log"

	"github.com/yuwf/gobase/utils"
)

func BenchmarkAlert(b *testing.B) {

	LogAlertCheck = func(prefix string) bool {
		return false
	}

	defer utils.HandlePanic()
	InitAlert()
	SetFeiShuAddr("https://open.feishu.cn/open-apis/bot/v2/hook/7d03d541-05d5-4f79-86d4-3f2708c9f786", "MwOWUyKzlnWfdGrpld1VVg", "Test")
	log.Error().Msg("StrLoader Test Alert")
	time.Sleep(time.Second * 5)
	panic("kdjfkd")
}
