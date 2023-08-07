package alert

import (
	"testing"
	"time"

	"github.com/yuwf/gobase/utils"
)

func BenchmarkAlert(b *testing.B) {
	defer utils.HandlePanic()
	InitAlert()
	SetFeiShuAddr("https://open.feishu.cn/open-apis/bot/v2/hook/3c5db467-64df-4acb-b3f0-97ba98356e55", "yqlg2yZstvM1H9fxGqYYOc", "Test")
	SendFeiShuAlert("Test Alert")
	time.Sleep(time.Second * 5)
	panic("kdjfkd")
}
