package utils

// https://github.com/yuwf/gobase

import (
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog/log"
)

// cron包的一个简单过渡

var defaultCron *cron.Cron

func init() {
	defaultCron = cron.New()
	defaultCron.Start()
}

// 添加一个定时任务
func CronAddFunc(spec string, cmd func()) (int, error) {
	caller := GetCallerDesc(1)
	id, err := defaultCron.AddFunc(spec, func() {
		HandlePanicWithCaller(caller)
		cmd()
	})
	if err != nil {
		log.Error().Err(err).Str("callPos", caller.Pos()).Str("spec", spec).Err(err).Msg("CronAddFunc")
	}
	return int(id), err
}
