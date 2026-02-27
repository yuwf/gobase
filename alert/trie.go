package alert

// https://github.com/yuwf/gobase

type trieNode struct {
	children map[rune]*trieNode
	isEnd    bool
	prefix   string
}

type trie struct {
	root *trieNode
}

func newTrie() *trie {
	return &trie{root: &trieNode{children: make(map[rune]*trieNode)}}
}

func (t *trie) Insert(prefix string) {
	node := t.root
	for _, ch := range prefix {
		if node.children[ch] == nil {
			node.children[ch] = &trieNode{children: make(map[rune]*trieNode)}
		}
		node = node.children[ch]
	}
	node.prefix = prefix
	node.isEnd = true
}

func (t *trie) HasPrefix(s string) bool {
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
