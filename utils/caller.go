package utils

// https://github.com/yuwf/gobase

import (
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type CallerDesc struct {
	filename string
	funname  string
	line     int

	name string // 文件名:函数名
	pos  string // 文件名:函数名:行号
	loc  string // 函数名:行号
}

// 返回 文件名:函数名:行号
func (c *CallerDesc) Pos() string {
	return c.pos
}

// 返回 文件名:函数名
func (c *CallerDesc) Name() string {
	return c.name
}

// 返回 函数名:行号
func (c *CallerDesc) Loc() string {
	return c.loc
}

var callerCache sync.Map // key: file, value: CallerDesc

func GetCallerDesc(skip int) *CallerDesc {
	pc, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return &CallerDesc{}
	}

	key := file + strconv.Itoa(line)

	if val, ok := callerCache.Load(key); ok {
		desc := val.(*CallerDesc)
		return desc
	}

	// 解析文件路径
	slice := strings.Split(file, "/")
	filename := file
	if len(slice) >= 2 {
		filename = slice[len(slice)-2] + "/" + slice[len(slice)-1]
	}

	// 解析函数名
	fun := runtime.FuncForPC(pc)
	funname := "unknown"
	if fun != nil {
		slice := strings.Split(fun.Name(), ".")
		if len(slice) >= 1 {
			funname = slice[len(slice)-1]
		}
	}

	// 存入缓存
	desc := &CallerDesc{
		filename: filename,
		funname:  funname,
		line:     line,
		name:     filename + ":" + funname,
		pos:      filename + ":" + funname + ":" + strconv.Itoa(line),
		loc:      funname + ":" + strconv.Itoa(line),
	}
	callerCache.Store(key, desc)

	return desc
}
