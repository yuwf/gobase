package utils

// https://github.com/yuwf/gobase

import (
	"container/list"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/panjf2000/ants"
	"github.com/rs/zerolog/log"
)

// ants包的一个简单过渡

var (
	defaultAntsPool *ants.Pool
	antWG           sync.WaitGroup

	sequenceGroupPool sync.Pool // 分组任务队列使用 *Sequence 列表
)

func init() {
	// ExpiryDuration：清理 goroutine 的时间间隔。每隔一段时间，Ants 就会对池中未被使用的 goroutine 进行清理，减少内存占用；
	// PreAlloc：是否在初始化工作池时预分配内存。对于一个超大容量，且任务耗时长的工作池来说，预分配内存可以大幅降低 goroutine 池中的内存重新分配损耗；
	// MaxBlockingTasks：阻塞任务的最大数，0代表无限制；
	// Nonblocking：工作池是否是非阻塞的，这决定了 Pool.Submit 接口在提交任务时是否会被阻塞；
	// PanicHandler：任务崩溃时的处理函数；
	// Logger：日志记录器
	defaultAntsPool, _ = ants.NewPool(ants.DEFAULT_ANTS_POOL_SIZE,
		ants.WithPanicHandler(func(r interface{}) {
			buf := make([]byte, 2048)
			l := runtime.Stack(buf, false)
			err := fmt.Errorf("%v: %s", r, buf[:l])
			log.Error().Err(err).Msg("Panic")
		}))
	sequenceGroupPool.New = func() any {
		return &Sequence{}
	}
}

// 暴露出原始对象
func DefaultAntsPool() *ants.Pool {
	return defaultAntsPool
}

// 提交一个任务
func Submit(task func()) {
	if task == nil {
		return
	}
	defaultAntsPool.Submit(task)
}

// 提交一个可以等待的任务
func SubmitProcess(task func()) error {
	antWG.Add(1)
	return defaultAntsPool.Submit(func() {
		defer antWG.Done()
		task()
	})
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

// 协成池调用任务队列 保证任务顺序执行
type Sequence struct {
	mutex sync.Mutex
	tasks list.List
	run   bool
	// 正在执行的的分组任务
	group   *GroupSequence
	groupId string // 当前执行的groupId
}

func (s *Sequence) Submit(task func()) {
	if task == nil {
		return
	}
	s.mutex.Lock()         // 加锁
	defer s.mutex.Unlock() // 退出时解锁

	// 添加任务
	s.tasks.PushBack(task)

	// 开启协成池调用handle
	if !s.run {
		s.run = true
		defaultAntsPool.Submit(s.handle)
	}
}

func (s *Sequence) submitGroup(group *GroupSequence, task func()) bool {
	s.mutex.Lock()         // 加锁
	defer s.mutex.Unlock() // 退出时解锁

	if s.group != group { // 可能最后一个任务恰好执行完，回收了
		return false
	}

	// 添加任务
	s.tasks.PushBack(task)

	// 开启协成池调用handle
	if !s.run {
		s.run = true
		defaultAntsPool.Submit(s.handle)
	}
	return true
}

func (s *Sequence) Clear() {
	s.mutex.Lock()         // 加锁
	defer s.mutex.Unlock() // 退出时解锁

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
		s.run = false
		if s.group != nil {
			s.group.seqs.Delete(s.groupId) // 先删除
			s.group = nil                  // 置空
			s.groupId = ""
			sequenceGroupPool.Put(s)
		}
		s.mutex.Unlock() // 解锁
		return           // 退出
	}
	task := s.tasks.Remove(s.tasks.Front()).(func()) // 移除当前完成的任务
	s.mutex.Unlock()                                 // 解锁

	// 任务执行完之后调用，防止任务有崩溃，放到defer中调用
	defer func() {
		s.mutex.Lock() // 加锁
		// 如果任务列表不为空继续开启下一个handle
		if s.tasks.Len() > 0 {
			defaultAntsPool.Submit(s.handle)
		} else {
			s.run = false
			if s.group != nil {
				s.group.seqs.Delete(s.groupId) // 先删除
				s.group = nil                  // 置空
				s.groupId = ""
				sequenceGroupPool.Put(s)
			}
		}
		s.mutex.Unlock() // 解锁
	}()

	// 执行task
	if task != nil {
		task()
	}
}

type GroupSequence struct {
	seqs sync.Map // groupId:*Sequence
}

// 提交一个顺序执行的任务，每个组顺序执行
func (g *GroupSequence) Submit(groupId string, task func()) {
	if task == nil {
		return
	}
	gseq, ok := g.seqs.Load(groupId)
	if ok {
		seq := gseq.(*Sequence)
		if seq.submitGroup(g, task) {
			return
		}
	}

	for {
		seq := sequenceGroupPool.Get().(*Sequence)
		seq.group = g
		seq.groupId = groupId
		gseq, ok := g.seqs.LoadOrStore(groupId, seq)
		if ok {
			seq.group = nil
			seq.groupId = ""
			sequenceGroupPool.Put(seq) // 有别的协程设置了，此处回收
			seq = gseq.(*Sequence)
		}
		if seq.submitGroup(g, task) {
			return
		} // 极端情况就是有一个相同的group任务执行完了，立刻结束了， 而gseq还记录是这个已删除的
	}
}
