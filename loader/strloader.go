package loader

// https://github.com/yuwf/gobase

import (
	"errors"
	"os"
	"reflect"
	"sync"

	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

// StrLoader
type StrLoader struct {
	sync.RWMutex
	src        []byte                  // 原始值 每次修改都重新make出一个来
	updateHook []func(old, new string) // 配置更新后的回调
}

// 获取配置
func (l *StrLoader) Get() string {
	l.RLock()
	defer l.RUnlock()
	if l.src == nil {
		return ""
	}
	return utils.BytesToString(l.src)
}

// RegHook 注册配置修改Hook
func (l *StrLoader) RegHook(hook func(old, new string)) {
	l.Lock()
	defer l.Unlock()
	l.updateHook = append(l.updateHook, hook)
}

// 返回的对象可能会为nil，外部只读不可修改
func (l *StrLoader) GetSrc() []byte {
	l.RLock()
	defer l.RUnlock()
	return l.src
}

func (l *StrLoader) Load(src []byte, path string) error {
	defer utils.HandlePanic()

	// 检查
	if reflect.DeepEqual(l.GetSrc(), src) {
		return nil
	}

	log.Info().Str("path", path).Msg("StrLoader Load Success")

	old := l.Get()
	// 替换值
	l.Lock()
	l.src = nil
	if src != nil {
		l.src = make([]byte, len(src)) // 深拷贝 防止传入的src会修改
		copy(l.src, src)
	}
	hook := l.updateHook // 拷贝出一份来
	l.Unlock()

	// 回调
	for _, f := range hook {
		f(old, l.Get())
	}
	return nil
}

func (l *StrLoader) LoadFile(path string) error {
	defer utils.HandlePanic()

	// 读取文件
	src, err := os.ReadFile(path)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("StrLoader LoadFile ReadFile error")
		return err
	}

	// 检查
	if src == nil {
		err := errors.New("src is nil")
		log.Error().Err(err).Str("path", path).Msg("StrLoader LoadFile error")
		return err
	}

	if reflect.DeepEqual(l.GetSrc(), src) {
		return nil
	}

	log.Info().Str("path", path).Msg("StrLoader LoadFile Success")

	old := l.Get()
	// 替换值
	l.Lock()
	l.src = src          // 不需要深拷贝
	hook := l.updateHook // 拷贝出一份来
	l.Unlock()

	// 回调
	for _, f := range hook {
		f(old, l.Get())
	}
	return nil
}

func (l *StrLoader) SaveFile(path string) error {
	// 检查
	src := l.GetSrc()
	if src == nil {
		err := errors.New("src is nil")
		log.Error().Err(err).Str("path", path).Msg("StrLoader SaveFile error")
		return err
	}
	err := os.WriteFile(path, src, os.FileMode(os.O_WRONLY|os.O_CREATE))
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("StrLoader SaveFile error")
	}
	return err
}
