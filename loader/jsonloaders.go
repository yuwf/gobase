package loader

// https://github.com/yuwf/gobase

import (
	"encoding/json"
	"reflect"
	"sync"

	"gobase/utils"

	"github.com/rs/zerolog/log"
)

// JsonLoaders json配置加载对象 协程安全
// 如果T有成员函数Normalize， 会加载完配置后调用
type JsonLoaders[T any] struct {
	sync.RWMutex
	confs      map[string]*T                  // 配置对象
	src        map[string][]byte              // 原始值
	updateHook []func(old, new map[string]*T) // 配置更新后的回调
}

// Get 返回的map对象一定不为nil 外层不需要再判断
func (l *JsonLoaders[T]) Get() map[string]*T {
	if l.confs == nil {
		l.Lock()
		defer l.Unlock()
		if l.confs == nil {
			l.confs = map[string]*T{}
		}
		return l.confs
	}
	l.RLock()
	defer l.RUnlock()
	return l.confs
}

// 不会调用Create和Normalize
func (l *JsonLoaders[T]) Set(confs map[string]*T) {
	if confs == nil {
		return
	}
	// 深拷贝一份
	copyConfs := map[string]*T{}
	for k, v := range confs {
		conf := new(T)
		*conf = *v
		copyConfs[k] = conf
	}

	old := l.Get()
	// 替换值
	l.Lock()
	l.confs = copyConfs
	l.src = nil
	hook := l.updateHook // 拷贝出一份来
	l.Unlock()

	// 回调
	for _, f := range hook {
		f(old, confs)
	}
}

// GetItem 获取指定的配置，返回的对象可能会为nil
func (l *JsonLoaders[T]) GetItem(key string) *T {
	l.RLock()
	defer l.RUnlock()
	if l.confs == nil {
		return nil
	}
	conf, ok := l.confs[key]
	if ok {
		return conf
	}
	return nil
}

// RegHook 注册配置修改Hook
func (l *JsonLoaders[T]) RegHook(hook func(old, new map[string]*T)) {
	l.Lock()
	defer l.Unlock()
	l.updateHook = append(l.updateHook, hook)
}

func (l *JsonLoaders[T]) GetSrc() map[string][]byte {
	l.RLock()
	defer l.RUnlock()
	return l.src
}

func (l *JsonLoaders[T]) GetSrcItem(key string) []byte {
	l.RLock()
	defer l.RUnlock()
	src := l.src[key]
	return src
}

func (l *JsonLoaders[T]) Load(src map[string][]byte, path string) error {
	defer utils.HandlePanic()

	// 检查
	if reflect.DeepEqual(l.GetSrc(), src) {
		return nil
	}

	// 构造配置对象
	confs := map[string]*T{}
	for k, v := range src {
		conf := new(T)
		// 调用对象的Create函数
		creater, ok := any(conf).(Creater)
		if ok {
			creater.Create()
		}

		err := json.Unmarshal([]byte(v), conf)
		if err != nil {
			log.Error().Err(err).Str("path", path).Str("key", k).Str("T", reflect.TypeOf(conf).Elem().String()).Msgf("JsonLoaders Unmarshal error")
			return err // 有一条失败 直接返回
		}

		// 调用对象的Normalize函数
		normalizer, ok := any(conf).(Normalizer)
		if ok {
			normalizer.Normalize()
		}

		confs[k] = conf
	}
	log.Info().Str("path", path).Int("count", len(confs)).Str("T", reflect.TypeOf(new(T)).Elem().String()).Msg("JsonLoaders Success")

	old := l.Get()
	// 替换值
	l.Lock()
	l.confs = confs
	l.src = nil
	if src != nil {
		l.src = make(map[string][]byte) // 深拷贝 防止传入的src会修改
		for k, v := range src {
			l.src[k] = make([]byte, len(v))
			copy(l.src[k], v)
		}
	}
	hook := l.updateHook // 拷贝出一份来
	l.Unlock()

	// 回调
	for _, f := range hook {
		f(old, confs)
	}
	return nil
}
