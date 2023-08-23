package log

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type filewrite struct {
	locker       sync.Locker            // 文件锁
	file         *os.File               // 日志文件
	console      *zerolog.ConsoleWriter // 控制台输出，样式 2023-07-01 09:29:55.878 INF log/log.go:52 > Log init success
	consoleStyle *zerolog.ConsoleWriter // 输出到file中日志格式使用console样式
	prefix       string                 // 日志名后缀
	path         string                 // 文件路径
	morningTime  time.Time              // 日志文件创建时间当天的0点
}

var logwrite = &filewrite{}

func init() {
	logwrite.path = os.Getenv("LOG_PATH")
	if logwrite.path == "" {
		logwrite.path = "./"
	}
}

// 空锁
type nulllock struct {
}

func (l *nulllock) Lock() {
}
func (l *nulllock) Unlock() {
}

// 日志写入
func (f *filewrite) Write(p []byte) (n int, err error) {
	f.locker.Lock()
	defer f.locker.Unlock()

	if f.console != nil {
		f.console.Write(p)
	}

	if f.file == nil || time.Since(f.morningTime) >= time.Hour*24 {
		f.createFile()
	}
	if f.file != nil {
		if f.consoleStyle != nil {
			logwrite.consoleStyle.Out = f.file // file会随时修改，每次都赋值下
			f.consoleStyle.Write(p)
		} else {
			f.file.Write(p)
		}
	}
	return len(p), nil
}

//先停止Write 在close
func (f *filewrite) close() {
	f.locker.Lock()
	defer f.locker.Unlock()
	if f.file != nil {
		f.file.Sync()
		f.file.Close()
	}
}

func (f *filewrite) createFile() {
	tmp, err := os.OpenFile(f.fileName(), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err == nil {
		oldfile := f.file
		if oldfile != nil {
			oldfile.Sync()
			oldfile.Close()
		}
		f.file = tmp
		tmp := time.Now()
		f.morningTime = time.Date(tmp.Year(), tmp.Month(), tmp.Day(), 0, 0, 0, 0, tmp.Location())
	}
}

func (f *filewrite) fileName() string {
	return fmt.Sprintf("%s%s_%s.log", f.path, time.Now().Format("2006-01-02"), f.prefix)
}
