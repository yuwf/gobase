package alert

import (
	"testing"
	"time"

	_ "github.com/yuwf/gobase/log"

	"github.com/rs/zerolog/log"

	"github.com/yuwf/gobase/utils"
)

func BenchmarkTrie(b *testing.B) {
	trie := newTrie()
	trie.InsertPrefix("Test")
	trie.InsertSuffix("您好")
	trie.InsertSuffix("hello")

	if !trie.HasPrefixOrSuffix("TestLogAbc") {
		log.Error().Str("s", "TestLogAbc").Msg("Err")
	}
	if trie.HasPrefixOrSuffix("LogAbcTest") {
		log.Error().Str("s", "LogAbcTest").Msg("Err")
	}
	if trie.HasPrefixOrSuffix("LogAbc") {
		log.Error().Str("s", "LogAbc").Msg("Err")
	}
	if !trie.HasPrefixOrSuffix("Hello您好") {
		log.Error().Str("s", "Hello").Msg("Err")
	}
	if trie.HasPrefixOrSuffix("您H好") {
		log.Error().Str("s", "您H好").Msg("Err")
	}
	if trie.HasPrefixOrSuffix("好您") {
		log.Error().Str("s", "好您").Msg("Err")
	}
	if trie.HasPrefixOrSuffix("ello") {
		log.Error().Str("s", "ello").Msg("Err")
	}
	if trie.HasPrefixOrSuffix("he") {
		log.Error().Str("s", "ello").Msg("Err")
	}
}

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
