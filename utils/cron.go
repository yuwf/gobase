package utils

// https://github.com/yuwf/gobase

import (
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog/log"
)

// cron包的一个简单过渡

var defaultCron *cron.Cron

func init() {
	defaultCron = cron.New(cron.WithSeconds())
	defaultCron.Start()
}

// 添加一个定时任务，秒级任务，和Linux的cron格式一样，但增加了秒的支持
// 返回任务ID，可以用来删除任务
// * * * * * * 表示每秒执行一次
// 0 * * * * * 表示每分钟的第0秒执行一次
func CronAddFunc(spec string, cmd func()) (int, error) {
	caller := GetCallerDesc(1)
	id, err := defaultCron.AddFunc(spec, func() {
		HandlePanicWithCaller(caller)
		cmd()
	})
	if err != nil {
		log.Error().Err(err).Str("callPos", caller.Pos()).Str("spec", spec).Err(err).Msg("CronAddFunc")
		return 0, err
	}
	return int(id), err
}

func CronRemoveFunc(id int) {
	defaultCron.Remove(cron.EntryID(id))
}
