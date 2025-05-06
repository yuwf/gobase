package alert

// https://github.com/yuwf/gobase

import (
	"github.com/yuwf/gobase/loader"
)

type TrieNode struct {
	children map[rune]*TrieNode
	isEnd    bool
	prefix   string
}

type Trie struct {
	root *TrieNode
}

func NewTrie() *Trie {
	return &Trie{root: &TrieNode{children: make(map[rune]*TrieNode)}}
}

func (t *Trie) Insert(prefix string) {
	node := t.root
	for _, ch := range prefix {
		if node.children[ch] == nil {
			node.children[ch] = &TrieNode{children: make(map[rune]*TrieNode)}
		}
		node = node.children[ch]
	}
	node.prefix = prefix
	node.isEnd = true
}

func (t *Trie) HasPrefix(s string) bool {
	node := t.root
	for _, ch := range s {
		n, ok := node.children[ch]
		if !ok {
			return false
		}
		node = n
		if node.isEnd {
			return true // 当前路径是某个 content 中的前缀
		}
	}
	return false
}

type AlertAddr struct {
	Addr   string `json:"addr,omitempty"`   // 报警地址
	Secret string `json:"secret,omitempty"` // 秘钥
}

type AlertConfig struct {
	*AlertAddr
	ErrorPrefix    []string `json:"errorprefix,omitempty"`
	MulErrorPrefix []string `json:"mulerrorprefix,omitempty"` // 单一节点报警，一个错误多个节点可能都会报警 会先调用 LogAlertCheck 判断是否报警节点
	Default        bool     `json:"default,omitempty"`        // 是否为默认的内置的错误报警，不设置就用第1个

	errorTrie    *Trie
	mulErrorTrie *Trie
}

type ParamConfig struct {
	ServerName  string         `json:"servername,omitempty"`
	Configs     []*AlertConfig `json:"configs,omitempty"`
	defaultAddr *AlertAddr
}

// 内置的错误报警
var inneErrorPrefix = []string{
	"Panic",
	"Consul",
	"Redis",
	"MySQL",
	"RandParamErr",
	"HttpRequest Err", "HttpRequest TimeOut",
	"GinServer RegHandler", "GinServer TimeOut", "GinServer Hystrix",
	"MsgDispatch Reg", "MsgDispatch TimeOut", "MsgDispatch Hystrix",
}
var inneMulErrorPrefix = []string{
	"JsonLoader", "StrLoader",
}

func (c *ParamConfig) Normalize() {
	defut := 0
	for i, v := range c.Configs {
		v.errorTrie = NewTrie()
		v.mulErrorTrie = NewTrie()
		for _, s := range v.ErrorPrefix {
			v.errorTrie.Insert(s)
		}
		for _, s := range v.MulErrorPrefix {
			v.mulErrorTrie.Insert(s)
		}
		if defut != 0 && v.Default {
			defut = i
		}
	}
	if defut < len(c.Configs) {
		c.defaultAddr = c.Configs[defut].AlertAddr
		for _, s := range inneErrorPrefix {
			c.Configs[defut].errorTrie.Insert(s)
		}
		for _, s := range inneMulErrorPrefix {
			c.Configs[defut].mulErrorTrie.Insert(s)
		}
	}
}

var ParamConf loader.JsonLoader[ParamConfig]
