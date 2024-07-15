package ae

import (
	"github.com/godis/util"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

type FeType int

const (
	AE_READABLE FeType = 1
	AE_WRITABLE FeType = 2
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
	id       int
	mask     TeType
	when     int64
	interval int64
	proc     TimeProc
	extra    any
	next     *AeTimeEvent
}

type AeLoop struct {
	FileEvents      map[int]*AeFileEvent
	TimeEvents      *AeTimeEvent
	fileEventFd     int
	timeEventNextId int
	stop            bool
	logger          *logrus.Logger
}

var fe2ep = [4]int32{0, unix.EPOLLIN, unix.EPOLLOUT}

func getFeKey(fd int, mask FeType) int {
	if mask == AE_READABLE {
		return fd
	}
	return fd * -1
}

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
	ev := loop.getEpollMask(fd)

	if ev&fe2ep[mask] != 0 {
		return
	}

	op := unix.EPOLL_CTL_ADD
	if ev != 0 {
		op = unix.EPOLL_CTL_MOD
	}

	ev |= fe2ep[mask]

	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: uint32(ev)})
	if err != nil {
		loop.logger.Errorf("epoll ctl err: %v\n", err)
		return
	}

	loop.FileEvents[getFeKey(fd, mask)] = &AeFileEvent{
		fd:    fd,
		mask:  mask,
		proc:  proc,
		extra: extra,
	}

	loop.logger.Debugf("ae add file event fd:%v, mask:%v\n", fd, mask)
}

func (loop *AeLoop) RemoveFileEvent(fd int, mask FeType) {
	op := unix.EPOLL_CTL_DEL
	ev := loop.getEpollMask(fd)
	ev &= 0xff ^ fe2ep[mask]
	if ev != 0 {
		op = unix.EPOLL_CTL_MOD
	}
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: uint32(ev)})
	if err != nil {
		loop.logger.Printf("epoll del err: %v\n", err)
	}
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
	timeout := loop.nearestTime() - util.GetMsTime()
	if timeout <= 0 {
		timeout = 10
	}
	var events [128]unix.EpollEvent
	n, err := unix.EpollWait(loop.fileEventFd, events[:], int(timeout))
	if err != nil {
		loop.logger.Printf("epoll wait warnning: %v\n", err)
	}
	if n > 0 {
		loop.logger.Debugf("ae get %v epoll events\n", n)
	}
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
	now := util.GetMsTime()
	p := loop.TimeEvents
	for p != nil {
		if p.when <= now {
			tes = append(tes, p)
		}
		p = p.next
	}
	return
}

func (loop *AeLoop) AeProcess(tes []*AeTimeEvent, fes []*AeFileEvent) {
	for _, te := range tes {
		te.proc(loop, te.id, te.extra)
		if te.mask == AE_ONCE {
			loop.RemoveTimeEvent(te.id)
		} else {
			te.when = util.GetMsTime() + te.interval
		}
	}
	if len(fes) > 0 {
		loop.logger.Debugln("ae is processing file events")
		for _, fe := range fes {
			fe.proc(loop, fe.fd, fe.extra)
		}
	}
}

func (loop *AeLoop) AeMain() {
	for !loop.stop {
		tes, fes := loop.AeWait()
		loop.AeProcess(tes, fes)
	}
}
