package loader

// https://github.com/yuwf/gobase

import (
	"errors"
	"io/ioutil"
	"os"
	"reflect"
	"sync"

	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

// StrLoaders string配置加载对象 协程安全
type StrLoaders struct {
	sync.RWMutex
	src        map[string][]byte                  // 原始值 每次修改都重新make出一个来
	updateHook []func(old, new map[string]string) // 配置更新后的回调
}

// Get 返回的map对象一定不为nil 外层不需要再判断
func (l *StrLoaders) Get() map[string]string {
	confs := map[string]string{}
	l.RLock()
	defer l.RUnlock()
	if l.src == nil {
		return confs
	}
	for key, src := range l.src {
		confs[key] = utils.BytesToString(src)
	}
	return confs
}

// GetItem 获取指定的配置，返回的对象可能会为nil
func (l *StrLoaders) GetItem(key string) string {
	l.RLock()
	defer l.RUnlock()
	if l.src == nil {
		return ""
	}
	conf, ok := l.src[key]
	if ok {
		return utils.BytesToString(conf)
	}
	return ""
}

// RegHook 注册配置修改Hook
func (l *StrLoaders) RegHook(hook func(old, new map[string]string)) {
	l.Lock()
	defer l.Unlock()
	l.updateHook = append(l.updateHook, hook)
}

// 返回的对象可能会为nil，外部只读不可修改
func (l *StrLoaders) GetSrc() map[string][]byte {
	l.RLock()
	defer l.RUnlock()
	return l.src
}

func (l *StrLoaders) GetSrcItem(key string) []byte {
	l.RLock()
	defer l.RUnlock()
	src := l.src[key]
	return src
}

func (l *StrLoaders) Load(src map[string][]byte, path string) error {
	defer utils.HandlePanic()

	// 检查
	if reflect.DeepEqual(l.GetSrc(), src) {
		return nil
	}
	keys := make([]string, 0, len(src))
	for k := range src {
		keys = append(keys, k)
	}
	log.Info().Str("path", path).Strs("keys", keys).Msg("StrLoaders Success")

	old := l.Get()
	// 替换值
	l.Lock()
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
		f(old, l.Get())
	}
	return nil
}

func (l *StrLoaders) LoadDir(path string) error {
	defer utils.HandlePanic()

	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("StrLoaders LoadDir ReadFile error")
		return err
	}

	// 读取文件
	srcs := map[string][]byte{}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		src, err := os.ReadFile(path + "/" + file.Name())
		if err != nil {
			log.Error().Err(err).Str("path", path).Str("file", file.Name()).Msg("StrLoader LoadDir ReadFile error")
			return err
		}

		// 检查
		if src == nil {
			err := errors.New("src is nil")
			log.Error().Err(err).Str("path", path).Str("file", file.Name()).Msg("StrLoader LoadDir error")
			return err
		}

		srcs[file.Name()] = src
	}

	if reflect.DeepEqual(l.GetSrc(), srcs) {
		return nil
	}

	keys := make([]string, 0, len(srcs))
	for k := range srcs {
		keys = append(keys, k)
	}
	log.Info().Str("path", path).Strs("keys", keys).Msg("StrLoaders LoadFile Success")

	old := l.Get()
	// 替换值
	l.Lock()
	l.src = srcs         // 不需要深拷贝
	hook := l.updateHook // 拷贝出一份来
	l.Unlock()

	// 回调
	for _, f := range hook {
		f(old, l.Get())
	}
	return nil
}

func (l *StrLoaders) SaveDir(path string) error {
	// 检查
	srcs := l.GetSrc()
	if srcs == nil {
		err := errors.New("src is nil")
		log.Error().Err(err).Str("path", path).Msg("StrLoader SaveDir error")
		return err
	}
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("StrLoader SaveDir error")
		return err
	}
	keys := make([]string, 0, len(srcs))
	for key, src := range srcs {
		err := os.WriteFile(path+"/"+key, src, os.FileMode(os.O_WRONLY|os.O_CREATE))
		if err != nil {
			log.Error().Err(err).Str("key", key).Msg("StrLoader SaveDir error")
			return err
		}
		keys = append(keys, key)
	}
	log.Info().Str("path", path).Strs("keys", keys).Msg("StrLoaders SaveDir Success")
	return nil
}
