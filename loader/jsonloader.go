package loader

// https://github.com/yuwf/gobase

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sync"

	"github.com/yuwf/gobase/utils"

	"github.com/rs/zerolog/log"
)

// JsonLoader json配置加载对象 协程安全
// 如果T有成员函数Normalize， 会加载完配置后调用
type JsonLoader[T any] struct {
	sync.RWMutex
	conf       *T                  // 配置对象
	src        []byte              // 原始值
	updateHook []func(old, new *T) // 配置更新后的回调
}

// Get 获取配置 返回的指针一定不为nil 外层不需要再判断
func (l *JsonLoader[T]) Get() *T {
	if l.conf == nil {
		l.Lock()
		defer l.Unlock()
		if l.conf == nil {
			conf := new(T)
			// 调用对象的Create函数
			creater, ok := any(conf).(Creater)
			if ok {
				creater.Create()
			}
			// 调用对象的Normalize函数
			normalizer, ok := any(conf).(Normalizer)
			if ok {
				normalizer.Normalize()
			}
			l.conf = conf
		}
		return l.conf
	}
	l.RLock()
	defer l.RUnlock()
	return l.conf
}

// RegHook 注册配置修改Hook
func (l *JsonLoader[T]) RegHook(hook func(old, new *T)) {
	l.Lock()
	defer l.Unlock()
	l.updateHook = append(l.updateHook, hook)
}

func (l *JsonLoader[T]) GetSrc() []byte {
	l.RLock()
	defer l.RUnlock()
	return l.src
}

func (l *JsonLoader[T]) Load(src []byte, path string) error {
	defer utils.HandlePanic()

	// 检查
	if reflect.DeepEqual(l.GetSrc(), src) {
		return nil
	}

	// 构造一个T对象
	conf := new(T)
	// 调用对象的Create函数
	creater, ok := any(conf).(Creater)
	if ok {
		creater.Create()
	}

	if src != nil {
		err := json.Unmarshal(src, conf)
		if err != nil {
			log.Error().Err(err).Str("path", path).Str("T", reflect.TypeOf(conf).Elem().String()).Msg("JsonLoader Load Unmarshal error")
			return err
		}
	}
	log.Info().Str("path", path).Str("T", reflect.TypeOf(conf).Elem().String()).Interface("conf", conf).Msg("JsonLoader Load Success")

	// 调用对象的Normalize函数
	normalizer, ok := any(conf).(Normalizer)
	if ok {
		normalizer.Normalize()
	}

	old := l.Get()
	// 替换值
	l.Lock()
	l.conf = conf
	l.src = nil
	if src != nil {
		l.src = make([]byte, len(src)) // 深拷贝 防止传入的src会修改
		copy(l.src, src)
	}
	hook := l.updateHook // 拷贝出一份来
	l.Unlock()

	// 回调
	for _, f := range hook {
		f(old, conf)
	}
	return nil
}

func (l *JsonLoader[T]) LoadFile(path string) error {
	defer utils.HandlePanic()

	// 读取文件
	src, err := os.ReadFile(path)
	if err != nil {
		log.Error().Err(err).Str("path", path).Str("T", reflect.TypeOf((*T)(nil)).Elem().String()).Msg("JsonLoader LoadFile ReadFile error")
		return err
	}

	// 检查
	if src == nil {
		err := errors.New("src is nil")
		log.Error().Err(err).Str("path", path).Str("T", reflect.TypeOf((*T)(nil)).Elem().String()).Msg("JsonLoader LoadFile error")
		return err
	}
	if reflect.DeepEqual(l.GetSrc(), src) {
		return nil
	}

	// 构造一个T对象
	conf := new(T)
	// 调用对象的Create函数
	creater, ok := any(conf).(Creater)
	if ok {
		creater.Create()
	}

	err = json.Unmarshal(src, conf)
	if err != nil {
		log.Error().Err(err).Str("path", path).Str("T", reflect.TypeOf(conf).Elem().String()).Msg("JsonLoader LoadFile Unmarshal error")
		return err
	}
	log.Info().Str("path", path).Str("T", reflect.TypeOf(conf).Elem().String()).Interface("conf", conf).Msg("JsonLoader LoadFile Success")

	// 调用对象的Normalize函数
	normalizer, ok := any(conf).(Normalizer)
	if ok {
		normalizer.Normalize()
	}

	old := l.Get()
	// 替换值
	l.Lock()
	l.conf = conf
	l.src = src          // 不需要深拷贝
	hook := l.updateHook // 拷贝出一份来
	l.Unlock()

	// 回调
	for _, f := range hook {
		f(old, conf)
	}
	return nil
}

func (l *JsonLoader[T]) SaveFile(path string) error {
	// 检查
	src := l.GetSrc()
	if src == nil {
		err := errors.New("scr is nil")
		log.Error().Err(err).Str("path", path).Str("T", reflect.TypeOf((*T)(nil)).Elem().String()).Msg("JsonLoader SaveFile error")
		return err
	}
	err := os.WriteFile(path, src, os.FileMode(os.O_WRONLY|os.O_CREATE))
	if err != nil {
		log.Error().Err(err).Str("path", path).Str("T", reflect.TypeOf((*T)(nil)).Elem().String()).Msg("JsonLoader SaveFile error")
	}
	return err
}

func (l *JsonLoader[T]) LoadBy(t *T) error {
	if t == nil {
		err := errors.New("t is nil")
		log.Error().Err(err).Str("T", reflect.TypeOf(t).Elem().String()).Msg("JsonLoader LoadBy error")
		return err
	}
	src, err := json.Marshal(t)
	if err != nil {
		log.Error().Err(err).Str("T", reflect.TypeOf(t).Elem().String()).Msg("JsonLoader LoadBy Marshal error")
		return err
	}
	if reflect.DeepEqual(l.GetSrc(), src) {
		return nil
	}

	// 构造一个T对象
	conf := new(T)
	// 调用对象的Create函数
	creater, ok := any(conf).(Creater)
	if ok {
		creater.Create()
	}

	err = json.Unmarshal(src, conf)
	if err != nil {
		log.Error().Err(err).Str("T", reflect.TypeOf(conf).Elem().String()).Msg("JsonLoader LoadBy Unmarshal error")
		return err
	}

	// 调用对象的Normalize函数
	normalizer, ok := any(conf).(Normalizer)
	if ok {
		normalizer.Normalize()
	}

	old := l.Get()
	// 替换值
	l.Lock()
	l.conf = conf
	l.src = src          // 不需要深拷贝
	hook := l.updateHook // 拷贝出一份来
	l.Unlock()

	// 回调
	for _, f := range hook {
		f(old, conf)
	}
	return nil
}
