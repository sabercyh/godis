package ae

import (
	"log"

	"github.com/godis/util"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

type FeType int

// 标识 FileEvent

const (
	AE_READABLE FeType = 1 // 可读事件
	AE_WRITABLE FeType = 2 // 可写事件
)

type TeType int

const (
	AE_NORMAL TeType = 1
	AE_ONCE   TeType = 2
)

type FileProc func(loop *AeLoop, fd int, extra any)

type TimeProc func(loop *AeLoop, id int, extra any)

type AeFileEvent struct {
	fd    int
	mask  FeType
	proc  FileProc
	extra any
}

type AeTimeEvent struct {
	id   int
	mask TeType
	// redis中 返回delta 在时间事件回调函数中返回
	when     int64 // ms 时间点
	interval int64 // ms 间隔
	proc     TimeProc
	extra    any
	next     *AeTimeEvent
}

type AeLoop struct {
	FileEvents      map[int]*AeFileEvent // 使用map进行创建和销毁FileEvent的效率更高
	TimeEvents      *AeTimeEvent
	fileEventFd     int
	timeEventNextId int
	stop            bool
	logger          *logrus.Logger
}

// AE事件到epoll常量的映射关系
var fe2ep = [3]int32{0, unix.EPOLLIN, unix.EPOLLOUT}

// 计算每个fd在map中的唯一标识 fs和mask定义唯一事件
func getFeKey(fd int, mask FeType) int {
	if mask == AE_READABLE {
		return fd
	}
	return fd * -1
}

// getEpollMask 根据给定文件描述符（fd）的当前事件订阅状态计算 epoll 事件掩码。
// 它检查文件描述符是否已注册可读（AE_READABLE）和/或可写（AE_WRITABLE）事件在 AeLoop 的 FileEvents 映射中。
// 对于每种订阅的事件类型，它将相应的 epoll 事件掩码添加到返回值中。
// 此函数用于为 epoll_ctl 操作准备所需的事件掩码，确保 epoll 实例知道我们对 fd 感兴趣的事件类型。
//
// 参数：
// - loop *AeLoop：指向管理文件描述符事件的 AeLoop 实例的指针。
// - fd int：要为其计算 epoll 事件掩码的文件描述符。
//
// 返回值：
// - int32：计算得到的 epoll 事件掩码，指示 fd 订阅的事件类型（可读、可写等）。

func (loop *AeLoop) getEpollMask(fd int) int32 {
	var ev int32
	if loop.FileEvents[getFeKey(fd, AE_READABLE)] != nil {
		ev |= fe2ep[AE_READABLE]
	}
	if loop.FileEvents[getFeKey(fd, AE_WRITABLE)] != nil {
		ev |= fe2ep[AE_WRITABLE]
	}
	return ev
}

func (loop *AeLoop) AddFileEvent(fd int, mask FeType, proc FileProc, extra any) {
	// Calculate the epoll event mask for the current file descriptor
	ev := loop.getEpollMask(fd)

	// Check if the event is already registered, if so, return immediately
	if ev&fe2ep[mask] != 0 {
		// The event is already registered, no further action needed
		return
	}

	// Decide whether to add a new event or enable an existing one based on the current event mask
	op := unix.EPOLL_CTL_ADD // Default operation is to add a new event
	if ev != 0 {
		op = unix.EPOLL_CTL_MOD // If there are existing events, change to enable operation
	}

	// Update the event mask to include the new event
	ev |= fe2ep[mask]

	// Call epoll_ctl to update the epoll instance, to register or enable the event
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: uint32(ev)})
	if err != nil {
		// If the operation fails, log the error and return
		log.Printf("epoll ctl err: %v\n", err)
		return
	}

	// Add the new file event to the AeLoop's FileEvents map
	loop.FileEvents[getFeKey(fd, mask)] = &AeFileEvent{
		fd:    fd,
		mask:  mask,
		proc:  proc,
		extra: extra,
	}

	// Log the operation of adding a file event
	loop.logger.Debugf("ae add file event fd:%v, mask:%v\n", fd, mask)
}

func (loop *AeLoop) RemoveFileEvent(fd int, mask FeType) {
	// epoll ctl
	op := unix.EPOLL_CTL_DEL
	ev := loop.getEpollMask(fd)
	ev &= ^fe2ep[mask]
	if ev != 0 {
		op = unix.EPOLL_CTL_MOD
	}
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: uint32(ev)})
	if err != nil {
		loop.logger.Printf("epoll del err: %v\n", err)
	}
	// ae ctl
	loop.FileEvents[getFeKey(fd, mask)] = nil
	loop.logger.Debugf("ae remove file event fd:%v, mask:%v\n", fd, mask)
}

func (loop *AeLoop) AddTimeEvent(mask TeType, interval int64, proc TimeProc, extra any) int {
	id := loop.timeEventNextId
	loop.timeEventNextId++
	var te AeTimeEvent
	te.id = id
	te.mask = mask
	te.interval = interval
	te.when = util.GetMsTime() + interval
	te.proc = proc
	te.extra = extra
	te.next = loop.TimeEvents
	loop.TimeEvents = &te
	return id
}

func (loop *AeLoop) RemoveTimeEvent(id int) {
	p := loop.TimeEvents
	var pre *AeTimeEvent
	for p != nil {
		if p.id == id {
			if pre == nil {
				loop.TimeEvents = p.next
			} else {
				pre.next = p.next
			}
			p.next = nil
			break
		}
		pre = p
		p = p.next
	}
}

func AeLoopCreate(logger *logrus.Logger) (*AeLoop, error) {
	epollFd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &AeLoop{
		FileEvents:      make(map[int]*AeFileEvent),
		fileEventFd:     epollFd,
		timeEventNextId: 1,
		stop:            false,
		logger:          logger,
	}, nil
}

func (loop *AeLoop) nearestTime() int64 {
	var nearest int64 = util.GetMsTime() + 1000
	p := loop.TimeEvents
	for p != nil {
		if p.when < nearest {
			nearest = p.when
		}
		p = p.next
	}
	return nearest
}

func (loop *AeLoop) AeWait() (tes []*AeTimeEvent, fes []*AeFileEvent) {
	// nearestTime 获取最近到来的timeEvent的触发时间
	// util.GetMsTime 获取当前的系统事件
	// 两者之差为epoll系统调用可以等待的时间，如果timeout时间太短，至少10ms
	timeout := loop.nearestTime() - util.GetMsTime()
	if timeout <= 0 {
		timeout = 10 // at least wait 10ms
	}
	var events [128]unix.EpollEvent
	n, err := unix.EpollWait(loop.fileEventFd, events[:], int(timeout))
	if err != nil {
		loop.logger.Printf("epoll wait warnning: %v\n", err)
	}
	if n > 0 {
		loop.logger.Debugf("ae get %v epoll events\n", n)
	}
	// collect file events register file events
	for i := 0; i < n; i++ {
		if events[i].Events&unix.EPOLLIN != 0 {
			fe := loop.FileEvents[getFeKey(int(events[i].Fd), AE_READABLE)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}
		if events[i].Events&unix.EPOLLOUT != 0 {
			fe := loop.FileEvents[getFeKey(int(events[i].Fd), AE_WRITABLE)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}
	}
	// collect time events
	now := util.GetMsTime() // 获取当前的时间
	p := loop.TimeEvents    // 获取timeEvent事件链表头节点
	// 遍历所有的TimeEvent
	for p != nil {
		// 如果TimeEvent的执行事件小于当前事件，需要执行，放入ReadyTimeEventsSlice中
		if p.when <= now {
			tes = append(tes, p)
		}
		p = p.next
	}
	return
}

func (loop *AeLoop) AeProcess(tes []*AeTimeEvent, fes []*AeFileEvent) {
	// 遍历已经reday的TimeEvents
	for _, te := range tes {
		// 执行TimeEvents的回调函数
		te.proc(loop, te.id, te.extra)
		// 如果TimeEvent是一次性事件，从TimeEventsList中移除
		if te.mask == AE_ONCE {
			loop.RemoveTimeEvent(te.id)
		} else {
			// 计算TimeEvent的下次执行事件
			te.when = util.GetMsTime() + te.interval
		}
	}
	// 遍历FileEvents
	if len(fes) > 0 {
		loop.logger.Debugln("ae is processing file events")
		for _, fe := range fes {
			fe.proc(loop, fe.fd, fe.extra)
		}
	}
}

func (loop *AeLoop) AeMain() {
	// 如果出现了不可恢复的错误 stop = true 比如epoll的fd监听失败
	for !loop.stop {
		// 获取已经ready的事件
		tes, fes := loop.AeWait()
		// 对以上两个事件进行处理
		loop.AeProcess(tes, fes)
	}
}
