package server

import (
	"sync"

	"github.com/godis/util"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

var wg sync.WaitGroup

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

func (loop *AeLoop) AddReadEvent(fd int, mask FeType, proc FileProc, extra any) {
	ev := unix.EPOLLIN
	op := unix.EPOLL_CTL_ADD

	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: uint32(ev)})
	if err != nil {
		loop.logger.Errorf("epoll ctl err: %v\n", err)
		return
	}

	loop.FileEvents[fd] = &AeFileEvent{
		fd:    fd,
		mask:  mask,
		proc:  proc,
		extra: extra,
	}

	loop.logger.Debugf("ae add file event fd:%v, mask:%v\n", fd, mask)
}

func (loop *AeLoop) ModWriteEvent(fd int, mask FeType, proc FileProc, extra any) {
	ev := unix.EPOLLIN | unix.EPOLLOUT
	op := unix.EPOLL_CTL_MOD

	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: uint32(ev)})
	if err != nil {
		loop.logger.Errorf("epoll mod err: %v\n", err)
		return
	}

	loop.FileEvents[-1*fd] = &AeFileEvent{
		fd:    fd,
		mask:  mask,
		proc:  proc,
		extra: extra,
	}

	loop.logger.Debugf("ae add write event fd:%v, mask:%v\n", fd, mask)
}

func (loop *AeLoop) RemoveWriteEvent(fd int, mask FeType) {
	ev := unix.EPOLLIN
	op := unix.EPOLL_CTL_MOD
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: uint32(ev)})
	if err != nil {
		loop.logger.Printf("epoll mod err: %v\n", err)
	}
	delete(loop.FileEvents, -1*fd)
	loop.logger.Debugf("ae remove write event fd:%v, mask:%v\n", fd, mask)
}

func (loop *AeLoop) RemoveFileEvent(fd int, mask FeType) {
	ev := unix.EPOLLIN
	op := unix.EPOLL_CTL_DEL
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: uint32(ev)})
	if err != nil {
		loop.logger.Printf("epoll del err: %v\n", err)
	}
	delete(loop.FileEvents, fd)
	loop.logger.Debugf("ae remove write event fd:%v, mask:%v\n", fd, mask)
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
retry:
	var events [128]unix.EpollEvent
	n, err := unix.EpollWait(loop.fileEventFd, events[:], int(timeout))

	if err != nil {
		// interrupted system call
		if err == unix.EINTR {
			goto retry
		}
		loop.logger.Errorf("epoll wait warnning: %v\n", err)
		return
	}

	for i := 0; i < n; i++ {
		if events[i].Events&unix.EPOLLIN != 0 {
			fe := loop.FileEvents[int(events[i].Fd)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}
		if events[i].Events&unix.EPOLLOUT != 0 {
			fe := loop.FileEvents[(int(-1 * events[i].Fd))]
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
		for _, fe := range fes {
			if fe.mask == AE_READABLE && fe.fd != server.fd {
				wg.Add(1)
				go func(fd int) {
					ReadBuffer(fd)
					wg.Done()
				}(fe.fd)
			} else if fe.mask == AE_WRITABLE {
				wg.Add(1)
				go func(fd int) {
					SendReplyToClient(fd)
					wg.Done()
				}(fe.fd)
			}
		}
		wg.Wait()

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

	for _, client := range server.clients {
		freeClient(client)
	}

	server.AOF.Buffer.Flush()
	server.AOF.File.Close()

	loop.logger.Infoln("ae loop exit")
}
