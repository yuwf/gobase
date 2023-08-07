package utils

import (
	"github.com/robfig/cron/v3"
)

// cron包的一个简单过渡

var defaultCron *cron.Cron

func init() {
	defaultCron = cron.New()
}
