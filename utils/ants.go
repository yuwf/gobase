package utils

// https://github.com/yuwf/gobase

import (
	"container/list"
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/panjf2000/ants"
	"github.com/rs/zerolog/log"
)

// ants包的一个简单过渡

var (
	defaultAntsPool *ants.Pool = NewAntsPool()
	antWG           sync.WaitGroup

	sequenceGroupPool sync.Pool // 分组任务队列使用 *Sequence 列表
)

func init() {
	sequenceGroupPool.New = func() any {
		return &Sequence{}
	}
}

// 暴露出原始对象
func DefaultAntsPool() *ants.Pool {
	return defaultAntsPool
}

func NewAntsPool() *ants.Pool {
	// ExpiryDuration：清理 goroutine 的时间间隔。每隔一段时间，Ants 就会对池中未被使用的 goroutine 进行清理，减少内存占用；
	// PreAlloc：是否在初始化工作池时预分配内存。对于一个超大容量，且任务耗时长的工作池来说，预分配内存可以大幅降低 goroutine 池中的内存重新分配损耗；
	// MaxBlockingTasks：阻塞任务的最大数，0代表无限制；
	// Nonblocking：工作池是否是非阻塞的，这决定了 Pool.Submit 接口在提交任务时是否会被阻塞；
	// PanicHandler：任务崩溃时的处理函数；
	// Logger：日志记录器
	pool, _ := ants.NewPool(ants.DEFAULT_ANTS_POOL_SIZE,
		ants.WithPanicHandler(func(r interface{}) {
			buf := make([]byte, 2048)
			l := runtime.Stack(buf, false)
			err := fmt.Errorf("%v: %s", r, buf[:l])
			log.Error().Err(err).Msg("Panic")
		}))
	return pool
}

// 提交一个任务
func Submit(task func()) {
	SubmitToPool(defaultAntsPool, task)
}

func SubmitToPool(antsPool *ants.Pool, task func()) {
	if task == nil {
		return
	}
	call := func() {
		defer HandlePanic() // 遇到过ants库处理完panic时，无法投递下一个任务，使用Sequence后续任务无法运行的情况
		task()
	}
	if antsPool.Submit(call) != nil {
		// 任务提交失败，直接开启goruntine
		go call()
	}
}

// 提交一个可以等待的任务
func SubmitProcess(task func()) {
	if task == nil {
		return
	}

	antWG.Add(1)

	fun := func() {
		defer antWG.Done() // 防止task崩溃，最好用defer
		task()
	}

	Submit(fun)
}

// 等待提交的任务
func WaitProcess(timeout time.Duration) {
	ch := make(chan int)
	go func() {
		antWG.Wait()
		close(ch)
	}()
	// 超时等待
	timer := time.NewTimer(timeout)
	select {
	case <-ch:
		if !timer.Stop() {
			select {
			case <-timer.C: // try to drain the channel
			default:
			}
		}
	case <-timer.C:
	}
}

type sequenceTask struct {
	ctx     context.Context
	task    func()
	funname string
}

func (t *sequenceTask) funName() string {
	if t.funname != "" {
		return t.funname
	}
	t.funname, _ = GetFuncName(t.task)
	return t.funname
}

// 协成池调用任务队列 保证任务顺序执行
type Sequence struct {
	Pool    *ants.Pool // 为空就使用默认的ants池
	mutex   sync.Mutex
	logName string // 日志名称,默认为空,不打印日志，一般调试用的
	tasks   list.List
	run     bool
	done    chan struct{}
}

func (s *Sequence) OpenLog(name string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.logName = name
}

func (s *Sequence) Submit(ctx context.Context, task func()) {
	if task == nil {
		return
	}
	s.mutex.Lock()         // 加锁
	defer s.mutex.Unlock() // 退出时解锁

	// 添加任务
	st := &sequenceTask{ctx: ctx, task: task}
	if len(s.logName) > 0 {
		LogCtx(log.Debug(), ctx).Int("len", s.tasks.Len()).Str("name", s.logName).Str("fun", st.funName()).Msgf("Sequence Submit")
	}
	s.tasks.PushBack(st)

	// 开启协成池调用handle
	if !s.run {
		s.run = true
		s.done = make(chan struct{})

		SubmitToPool(If(s.Pool != nil, s.Pool, defaultAntsPool), s.handle)
	}
}

// 等待任务执行完成
func (s *Sequence) Wait() {
	if s.done != nil {
		<-s.done
	}
}

// 清空任务，清空的是未执行的任务
func (s *Sequence) Clear() {
	s.mutex.Lock()         // 加锁
	defer s.mutex.Unlock() // 退出时解锁

	if len(s.logName) > 0 {
		log.Debug().Int("len", s.tasks.Len()).Str("name", s.logName).Msgf("Sequence clear")
	}

	// 删除还未执行的任务
	for s.tasks.Len() > 0 {
		s.tasks.Remove(s.tasks.Back())
	}
}

func (s *Sequence) Len() int {
	s.mutex.Lock()         // 加锁
	defer s.mutex.Unlock() // 退出时解锁
	return s.tasks.Len()
}

func (s *Sequence) handle() {
	//取出一个任务
	s.mutex.Lock() // 加锁
	if s.tasks.Len() == 0 {
		// 这里的逻辑理论只有触发Clear函数才会走到
		if len(s.logName) > 0 {
			log.Debug().Int("len", s.tasks.Len()).Str("name", s.logName).Msgf("Sequence is null")
		}
		s.run = false
		close(s.done)
		s.mutex.Unlock() // 解锁
		return           // 退出
	}
	task := s.tasks.Remove(s.tasks.Front()).(*sequenceTask) // 移除当前完成的任务
	if len(s.logName) > 0 {
		LogCtx(log.Debug(), task.ctx).Int("len", s.tasks.Len()+1).Str("name", s.logName).Str("fun", task.funName()).Msgf("Sequence do")
	}
	s.mutex.Unlock() // 解锁

	// 任务执行完之后调用，防止任务有崩溃，放到defer中调用
	defer func() {
		s.mutex.Lock() // 加锁
		if len(s.logName) > 0 {
			LogCtx(log.Debug(), task.ctx).Int("len", s.tasks.Len()).Str("name", s.logName).Str("fun", task.funName()).Msgf("Sequence done")
		}
		// 如果任务列表不为空继续开启下一个handle
		if s.tasks.Len() > 0 {
			SubmitToPool(If(s.Pool != nil, s.Pool, defaultAntsPool), s.handle)
		} else {
			s.run = false
			close(s.done)
		}
		s.mutex.Unlock() // 解锁
	}()

	// 执行task
	task.task()
}

type GroupSequence struct {
	Pool   *ants.Pool // 为空就使用默认的ants池
	mutex  sync.Mutex
	groups map[interface{}]*Sequence
	done   chan struct{}
}

// 提交一个顺序执行的任务，每个组顺序执行
func (g *GroupSequence) Submit(ctx context.Context, groupId interface{}, task func()) {
	if task == nil {
		return
	}

	fun := func() {
		// 任务执行完了，如果任务队列中空了，就删除该组，防止任务有崩溃，放到defer中调用
		defer func() {
			g.mutex.Lock()
			defer g.mutex.Unlock()

			seq := g.groups[groupId]
			if seq.Len() == 0 {
				sequenceGroupPool.Put(seq)
				delete(g.groups, groupId)
				if len(g.groups) == 0 {
					close(g.done)
				}
			}
		}()

		// 执行任务
		task()
	}

	g.mutex.Lock()
	defer g.mutex.Unlock()

	if g.groups == nil {
		g.groups = map[interface{}]*Sequence{}
	}
	if len(g.groups) == 0 {
		g.done = make(chan struct{})
	}

	if seq, ok := g.groups[groupId]; ok {
		seq.Submit(ctx, fun)
	} else {
		seq := sequenceGroupPool.Get().(*Sequence)
		seq.Pool = g.Pool
		g.groups[groupId] = seq
		seq.Submit(ctx, fun)
	}
}

func (g *GroupSequence) Wait() {
	if g.done != nil {
		<-g.done
	}
}

func (g *GroupSequence) Len() int {
	g.mutex.Lock()         // 加锁
	defer g.mutex.Unlock() // 退出时解锁

	if g.groups == nil {
		return 0
	}

	l := 0
	for _, seq := range g.groups {
		l += seq.Len()
	}
	return l
}

func (g *GroupSequence) Clear(groupId interface{}) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if g.groups == nil {
		return
	}

	if seq, ok := g.groups[groupId]; ok {
		seq.Clear()
	}
}

func (g *GroupSequence) ClearAll() {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if g.groups == nil {
		return
	}

	for _, seq := range g.groups {
		seq.Clear()
	}
}
