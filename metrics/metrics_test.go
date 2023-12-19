package metrics

import (
	"testing"

	"github.com/rs/zerolog/log"
	_ "github.com/yuwf/gobase/log"
)

func BenchmarkRegexp(b *testing.B) {
	strs := []string{
		"/game/45a6-5dfkdfj-sdfj45df5df-sldaf/4956886/sakjfajsdfjkasdjfajsfdk/94958585",
	}

	for _, str := range strs {
		log.Info().Msg(str)
		for _, exp := range redisKeyRegexp {
			k, _ := exp.Replace(str, "*", 0, -1)
			log.Info().Msg("\t" + k)
		}
	}

}
