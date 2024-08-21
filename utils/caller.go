package utils

// https://github.com/yuwf/gobase

import (
	"runtime"
	"strconv"
	"strings"
)

type CallerDesc struct {
	filename string
	funname  string
	line     int
}

// 返回 文件名:函数名:行号  用作日志
func (c *CallerDesc) Pos() string {
	return c.filename + ":" + c.funname + ":" + strconv.Itoa(c.line)
}

// 返回 文件名:函数名 用作指标统计
func (c *CallerDesc) Name() string {
	return c.filename + ":" + c.funname
}

func GetCallerDesc(skip int) *CallerDesc {
	caller := &CallerDesc{}
	pc, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return caller
	}
	// 文件名只取到go文件的目录层
	slice := strings.Split(file, "/")
	if len(slice) >= 2 {
		caller.filename = slice[len(slice)-2] + "/" + slice[len(slice)-1]
	} else if len(slice) >= 1 {
		caller.filename = slice[len(slice)-1]
	}

	fun := runtime.FuncForPC(pc)
	if fun != nil {
		caller.funname = fun.Name()
		// 只取函数
		slice := strings.Split(caller.funname, ".")
		if len(slice) >= 1 {
			caller.funname = slice[len(slice)-1]
		}
	}

	caller.line = line
	return caller
}
