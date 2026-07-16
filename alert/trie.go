package alert

// https://github.com/yuwf/gobase

type trieNode struct {
	children map[rune]*trieNode
	isEnd    bool
}

type trie struct {
	prefix *trieNode // 前缀树
	suffix *trieNode // 后缀树
}

func newTrie() *trie {
	return &trie{prefix: &trieNode{children: make(map[rune]*trieNode)}, suffix: &trieNode{children: make(map[rune]*trieNode)}}
}

func (t *trie) InsertPrefix(s string) {
	node := t.prefix
	for _, ch := range s {
		if node.children[ch] == nil {
			node.children[ch] = &trieNode{children: make(map[rune]*trieNode)}
		}
		node = node.children[ch]
	}
	node.isEnd = true
}

func (t *trie) InsertSuffix(s string) {
	node := t.suffix
	runes := []rune(s)
	for i := len(runes) - 1; i >= 0; i-- {
		ch := runes[i]
		if node.children[ch] == nil {
			node.children[ch] = &trieNode{children: make(map[rune]*trieNode)}
		}
		node = node.children[ch]
	}
	node.isEnd = true
}

func (t *trie) HasPrefix(s string) bool {
	node := t.prefix
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

func (t *trie) HasSuffix(s string) bool {
	node := t.suffix
	runes := []rune(s)
	for i := len(runes) - 1; i >= 0; i-- {
		ch := runes[i]
		n, ok := node.children[ch]
		if !ok {
			return false
		}
		node = n
		if node.isEnd {
			return true // 当前路径是某个 content 中的后缀
		}
	}
	return false
}

func (t *trie) HasPrefixOrSuffix(s string) bool {
	return t.HasPrefix(s) || t.HasSuffix(s)
}
