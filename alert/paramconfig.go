package alert

// https://github.com/yuwf/gobase

import (
	"os"

	"github.com/yuwf/gobase/loader"
	"github.com/yuwf/gobase/utils"
)

type AlertAddr struct {
	Addr   string `json:"addr,omitempty"`   // 报警地址
	Secret string `json:"secret,omitempty"` // 秘钥
}

type AlertConfig struct {
	*AlertAddr
	Only           []string `json:"only,omitempty"`   // log只向指定的ip或者hostname发送报警， 支持?*通配符 不区分大小写
	Ignore         []string `json:"ignore,omitempty"` // log输出忽略的ip或者hostname， 支持?*通配符 不区分大小写
	ErrorPrefix    []string `json:"errorprefix,omitempty"`
	MulErrorPrefix []string `json:"mulerrorprefix,omitempty"` // 单一节点报警，一个错误多个节点可能都会报警 会先调用 LogAlertCheck 判断是否报警节点
	Default        bool     `json:"default,omitempty"`        // 是否为默认的内置的错误报警，不设置就用第1个

	errorTrie    *trie
	mulErrorTrie *trie
	isIgnore     bool // 是否忽略本机的报警
}

type ParamConfig struct {
	ServerName  string         `json:"servername,omitempty"`
	Configs     []*AlertConfig `json:"configs,omitempty"`
	defaultAddr []*AlertAddr   // 默认报警地址
}

// 内置的错误报警
var inneErrorPrefix = []string{
	"Panic",
	"Error",
	"Consul",
	"Nacos",
	"Redis",
	"MySQL",
	"RandParamErr",
	"RecursiveMutex",
	"HttpRequest Err", "HttpRequest TimeOut",
	"GinServer RegHandler", "GinServer TimeOut", "GinServer Hystrix",
	"MsgDispatch Reg", "MsgDispatch TimeOut",
	"RecvMsg",
	"LocalWatch",
}
var inneMulErrorPrefix = []string{
	"JsonLoader", "StrLoader",
}

func (c *ParamConfig) Normalize() {
	hostname, _ := os.Hostname()
	ip := utils.LocalIPString()

	defut := -1
	for i, v := range c.Configs {
		v.errorTrie = newTrie()
		v.mulErrorTrie = newTrie()

		if len(v.Only) > 0 {
			v.isIgnore = true
			for _, o := range v.Only {
				if utils.IsMatch(o, hostname) {
					v.isIgnore = false
					break
				}
				if utils.IsMatch(o, ip) {
					v.isIgnore = false
					break
				}
			}
		} else {
			v.isIgnore = false
			for _, o := range v.Ignore {
				if utils.IsMatch(o, hostname) {
					v.isIgnore = true
					break
				}
				if utils.IsMatch(o, ip) {
					v.isIgnore = true
					break
				}
			}
		}
		if v.isIgnore {
			continue // 忽略了本机的报警，不用构建下面的数据
		}

		// 构建报警数据
		for _, s := range v.ErrorPrefix {
			v.errorTrie.Insert(s)
		}
		for _, s := range v.MulErrorPrefix {
			v.mulErrorTrie.Insert(s)
		}
		if v.Default {
			c.defaultAddr = append(c.defaultAddr, v.AlertAddr)
		}
		if defut == -1 {
			defut = i
		}
	}
	// 如果没设置内置报警，默认给一个
	if len(c.defaultAddr) == 0 && defut != -1 {
		c.defaultAddr = append(c.defaultAddr, c.Configs[defut].AlertAddr)
	}

	// 内置的报警 加入到默认的报警器中
	if defut != -1 && defut < len(c.Configs) {
		for _, s := range inneErrorPrefix {
			c.Configs[defut].errorTrie.Insert(s)
		}
		for _, s := range inneMulErrorPrefix {
			c.Configs[defut].mulErrorTrie.Insert(s)
		}
	}
}

var ParamConf loader.JsonLoader[ParamConfig]
