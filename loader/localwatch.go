package loader

// https://github.com/yuwf/gobase

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuwf/gobase/utils"

	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
)

// LocalWatch 本地文件监控器
type LocalWatch struct {
	sync.RWMutex
	watchers map[string]*fileWatcher // 文件路径 -> 监控器映射
	watcher  *fsnotify.Watcher       // 全局fsnotify监控器

	quit  chan int // 退出检查使用
	state int32    // 运行状态 0:未运行 1：loop中
}

// fileWatcher 单个文件监控器
type fileWatcher struct {
	path          string        // 监控的文件路径
	watchDir      string        // 监控的目录，目前使用fsnotify监控的是文件所在的目录，不是path，在linux会有问题
	loader        Loader        // 配置加载器
	debounceTimer *time.Timer   // 防抖定时器
	debounceDelay time.Duration // 防抖延迟
}

var defaultLocalWatch *LocalWatch

func DefaultLocalWatch() *LocalWatch {
	return defaultLocalWatch
}

func InitDefaultLocalWatch() (*LocalWatch, error) {
	if defaultLocalWatch != nil {
		return defaultLocalWatch, nil
	}
	var err error
	defaultLocalWatch, err = NewLocalWatch()
	return defaultLocalWatch, err
}

// NewLocalWatch 创建本地文件监控器
func NewLocalWatch() (*LocalWatch, error) {
	// 创建fsnotify监控器
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Error().Err(err).Msg("LocalWatch NewWatcher error")
		return nil, err
	}

	w := &LocalWatch{
		watchers: make(map[string]*fileWatcher),
		watcher:  watcher,
		quit:     make(chan int),
		state:    1,
	}
	go w.loop()

	return w, nil
}

// ListenFile 监听本地文件变化，类似nacos的ListenConfig
// path: 要监控的文件路径
// loader: 配置加载器
// immediately: 是否立即加载一次配置，如果失败了就不在监控
func (w *LocalWatch) ListenFile(path string, loader Loader, immediately bool) error {
	// 获取文件的绝对路径
	absPath, err := filepath.Abs(path)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("LocalWatch ListenFile Abs error")
		return err
	}

	// Windows 下还是能收到 WRITE，修改文件能收到通知
	// Linux修改文件 很多工具的原理是 删除原来的，rename新的文件到原来的，收不到Write
	// 所以统一监控目录
	watchDir := filepath.Dir(absPath)

	// 如果要求立即加载，先加载一次
	if immediately {
		err := loader.LoadFile(absPath)
		if err != nil {
			return err
		}
	}

	w.Lock()
	defer w.Unlock()

	// 检查是否已经在监控该文件
	if _, exists := w.watchers[absPath]; exists {
		log.Warn().Str("path", absPath).Msg("LocalWatch ListenFile already watching")
		return nil
	}

	// 判断 watchDir 是否存在了
	watchDirExist := false
	for _, v := range w.watchers {
		if v.watchDir == watchDir {
			watchDirExist = true
		}
	}

	if !watchDirExist {
		// 将文件添加到fsnotify监控器
		err = w.watcher.Add(watchDir)
		if err != nil {
			log.Error().Err(err).Str("path", absPath).Msg("LocalWatch ListenFile Add error")
			return err
		}
	}

	// 添加到监控列表
	fw := &fileWatcher{
		path:          absPath,
		watchDir:      watchDir,
		loader:        loader,
		debounceDelay: 100 * time.Millisecond,
	}
	w.watchers[absPath] = fw

	// 文件存在就延迟读取下
	if !immediately {
		if _, err := os.Stat(absPath); os.IsNotExist(err) {
		} else {
			fw.debounceTimer = time.AfterFunc(fw.debounceDelay, func() {
				defer utils.HandlePanic()
				// 重新加载文件
				fw.loader.LoadFile(fw.path)
			})
		}
	}

	log.Info().Str("path", absPath).Msg("LocalWatch ListenFile")

	return nil
}

// CancelListenFile 取消监听本地文件
func (w *LocalWatch) CancelListenFile(path string) error {
	// 获取文件的绝对路径
	absPath, err := filepath.Abs(path)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("LocalWatch CancelListenFile Abs error")
		return err
	}

	// 查找并停止监控器
	w.Lock()
	fw, exists := w.watchers[absPath]
	if !exists {
		w.Unlock()
		log.Warn().Str("path", absPath).Msg("LocalWatch CancelListenFile not watching")
		return nil
	}

	// 停止防抖定时器
	if fw.debounceTimer != nil {
		fw.debounceTimer.Stop()
	}
	// 删除
	delete(w.watchers, absPath)

	// 判断 watchDir 是否还有在监控的
	watchDirExist := false
	for _, v := range w.watchers {
		if v.watchDir == fw.watchDir {
			watchDirExist = true
		}
	}

	// 从fsnotify中移除文件监控
	if watchDirExist && w.watcher != nil {
		w.watcher.Remove(fw.watchDir)
	}

	w.Unlock()

	log.Info().Str("path", absPath).Msg("LocalWatch CancelListenFile stopped")

	return nil
}

// IsWatching 检查是否正在监控指定文件
func (w *LocalWatch) IsWatching(path string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}

	w.RLock()
	defer w.RUnlock()
	_, exists := w.watchers[absPath]
	return exists
}

// GetWatchingFiles 获取所有正在监控的文件路径
func (w *LocalWatch) GetWatchingFiles() []string {
	w.RLock()
	defer w.RUnlock()

	files := make([]string, 0, len(w.watchers))
	for path := range w.watchers {
		files = append(files, path)
	}
	return files
}

func (w *LocalWatch) Close() {
	if !atomic.CompareAndSwapInt32(&w.state, 1, 0) {
		return
	}

	w.quit <- 1
	<-w.quit

	log.Info().Msg("LocalWatch Quit")
}

// watchLoop 全局文件监控循环
func (w *LocalWatch) loop() {
	defer utils.HandlePanic()

	for {
		select {
		case event, ok := <-w.watcher.Events:
			if !ok {
				log.Error().Msg("LocalWatch events channel closed")
				w.exit()
				return
			}

			// 只处理写操作和重命名操作
			if event.Op&(fsnotify.Write|fsnotify.Rename|fsnotify.Create) == 0 {
				continue
			}

			w.handle(event)

		case err, ok := <-w.watcher.Errors:
			if !ok {
				log.Error().Msg("LocalWatch channel closed")
				w.exit()
				return
			}
			log.Error().Err(err).Msg("LocalWatch error")

		case <-w.quit:
			w.exit()
			w.quit <- 1 // 反写下
			return
		}
	}
}

func (w *LocalWatch) exit() {
	atomic.StoreInt32(&w.state, 0) // 先标记
	defer utils.HandlePanic()

	w.Lock()
	defer w.Unlock()

	// 停止所有防抖定时器
	for _, fw := range w.watchers {
		if fw.debounceTimer != nil {
			fw.debounceTimer.Stop()
		}
	}
	w.watcher.Close()
	w.watchers = make(map[string]*fileWatcher)
}

func (w *LocalWatch) handle(event fsnotify.Event) {
	defer utils.HandlePanic()

	// 查找对应的文件监控器
	w.RLock()
	defer w.RUnlock()

	absPath, err := filepath.Abs(event.Name)
	if err != nil {
		absPath = event.Name
	}
	if fw, ok := w.watchers[absPath]; ok {
		// 取消之前的定时器 设置防抖定时器
		if fw.debounceTimer != nil {
			fw.debounceTimer.Stop()
		}

		fw.debounceTimer = time.AfterFunc(fw.debounceDelay, func() {
			defer utils.HandlePanic()
			// 重新加载文件
			fw.loader.LoadFile(fw.path)
		})
	}
}
