package utils

// https://github.com/yuwf/gobase

import (
	"fmt"
	"runtime"

	"github.com/rs/zerolog/log"
)

// 通用的panic处理函数
func HandlePanic() {
	if r := recover(); r != nil {
		buf := make([]byte, 2048)
		l := runtime.Stack(buf, false)
		err := fmt.Errorf("%v: %s", r, buf[:l])
		log.Error().Err(err).Msg("Panic")
	}
}

func HandlePanic2(paniccall func()) {
	if r := recover(); r != nil {
		buf := make([]byte, 2048)
		l := runtime.Stack(buf, false)
		err := fmt.Errorf("%v: %s", r, buf[:l])
		log.Error().Err(err).Msg("Panic")
		if paniccall != nil {
			paniccall()
		}
	}
}

func HandlePanicWithCaller(caller *CallerDesc) {
	if r := recover(); r != nil {
		buf := make([]byte, 2048)
		l := runtime.Stack(buf, false)
		err := fmt.Errorf("%v: %s", r, buf[:l])
		log.Error().Str("callPos", caller.Pos()).Err(err).Msg("Panic")
	}
}
