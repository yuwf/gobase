package log

import (
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
	"github.com/rs/zerolog/log"
)

var (
	asyncWriteLog diode.Writer
)

func init() {
	zerolog.MessageFieldName = "msg"
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"

	// 文件路径变短
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		slice := strings.Split(file, "/")
		if len(slice) >= 2 {
			return slice[len(slice)-2] + "/" + slice[len(slice)-1] + ":" + strconv.Itoa(line)
		}
		return file + ":" + strconv.Itoa(line)
	}

	// 不初始化默认只有控制台输出
	console := zerolog.ConsoleWriter{Out: os.Stderr, NoColor: false, TimeFormat: zerolog.TimeFieldFormat}
	log.Logger = zerolog.New(console).With().Timestamp().Caller().Logger()
}

func InitLog(prefix string) error {
	// 默认值
	SetLevel(0)
	EnableStdout()

	//默认用同步的，异步需要配置环境变量LOG_ASYNC
	if os.Getenv("LOG_ASYNC") == "1" {
		logwrite.locker = new(nulllock)
		asyncWriteLog = diode.NewWriter(logwrite, 32*1024, 0, func(missed int) {})
		log.Logger = zerolog.New(asyncWriteLog).With().Timestamp().Caller().Logger()
	} else {
		logwrite.locker = new(sync.Mutex)
		log.Logger = zerolog.New(logwrite).With().Timestamp().Caller().Logger()
	}
	logwrite.prefix = prefix
	logwrite.createFile()
	log.Info().Msg("Log init success")
	return nil
}

func Stop() {
	if os.Getenv("LOG_ASYNC") == "1" {
		asyncWriteLog.Close()
	}
	logwrite.close()
}

func SetLevel(l int) {
	log.Logger = log.Level(zerolog.Level(l))
}

// 打开标准控制台输出，可以在日志初始化前调用
func EnableStdout() {
	if logwrite.console == nil {
		logwrite.console = &zerolog.ConsoleWriter{Out: os.Stderr, NoColor: false, TimeFormat: zerolog.TimeFieldFormat}
	}
}

// 关闭标准控制台输出
func DisableStdout() {
	logwrite.console = nil
}

// 落地的日志文件格式 采用标准控制台样式，可以在日志初始化前调用
func EnableStdoutStyle() {
	if logwrite.consoleStyle == nil {
		logwrite.consoleStyle = &zerolog.ConsoleWriter{Out: nil, NoColor: true, TimeFormat: zerolog.TimeFieldFormat}
	}
}

// 关闭
func DisableStdoutStyle() {
	logwrite.consoleStyle = nil
}
