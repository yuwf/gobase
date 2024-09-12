package apollo

// https://github.com/yuwf/gobase

import (
	"sync"

	"github.com/apolloconfig/agollo/v4/storage"
)

type watch struct {
	namespace string
	key       string
	f         func(value interface{})
}

type ChangeListener struct {
	sync.Mutex
	watchs []*watch
}

func (l *ChangeListener) addWatch(namespace, key string, f func(value interface{})) {
	l.Lock()
	defer l.Unlock()
	l.watchs = append(l.watchs, &watch{
		namespace: namespace,
		key:       key,
		f:         f,
	})
}

//OnChange 增加变更监控
func (l *ChangeListener) OnChange(event *storage.ChangeEvent) {
	l.Lock()
	watchs := l.watchs
	l.Unlock()

	for key, conf := range event.Changes {
		for _, w := range watchs {
			if event.Namespace == w.namespace && key == w.key {
				w.f(conf.NewValue)
			}
		}
	}
}

//OnNewestChange 监控最新变更
func (l *ChangeListener) OnNewestChange(event *storage.FullChangeEvent) {

}
