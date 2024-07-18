package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/godis/conf"
	"github.com/godis/data"
	"github.com/godis/db"
	"github.com/godis/errs"
	"github.com/godis/net"
	"github.com/godis/util"
	"github.com/sirupsen/logrus"
)

const sub = 'a' - 'A'

type GodisClient struct {
	fd       int
	db       *db.GodisDB
	args     []*data.Gobj
	reply    *data.List
	sentLen  int
	queryBuf []byte
	queryLen int
	cmdTy    conf.CmdType
	bulkNum  int
	bulkLen  int
	logEntry *logrus.Entry
	closed   bool
}

func InitGodisClientInstance(fd int, server *GodisServer) *GodisClient {
	return &GodisClient{
		fd:       fd,
		db:       server.DB,
		queryBuf: make([]byte, conf.GODIS_IO_BUF),
		reply:    data.ListCreate(data.ListType{EqualFunc: data.GStrEqual}),
		logEntry: server.logger.WithFields(logrus.Fields{
			"clientFD": fd,
		}),
		closed: false,
	}
}

func resetClient(client *GodisClient) {
	freeArgs(client)
	client.cmdTy = conf.COMMAND_UNKNOWN
	client.bulkLen = 0
	client.bulkNum = 0
}

func (client *GodisClient) findLineInQuery() (int, error) {
	index := strings.Index(string(client.queryBuf[:client.queryLen]), "\r\n")
	if index < 0 && client.queryLen > conf.GODIS_MAX_INLINE {
		return index, errs.OutOfLimitError
	}
	return index, nil
}

func (client *GodisClient) getNumInQuery(s, e int) (int, error) {
	num, err := strconv.Atoi(string(client.queryBuf[s:e]))
	client.queryBuf = client.queryBuf[e+2:]
	client.queryLen -= e + 2
	return num, err
}

func handleInlineBuf(client *GodisClient) (bool, error) {
	index := strings.Index(string(client.queryBuf[:client.queryLen]), "\r\n")
	if index < 0 {
		if client.queryLen > conf.GODIS_MAX_INLINE {
			return false, errs.OutOfLimitError
		} else {
			return false, errs.CustomError
		}
	}
	subs := strings.Split(string(client.queryBuf[:index]), " ")
	client.queryBuf = client.queryBuf[index+2:]
	client.queryLen -= index + 2
	client.args = make([]*data.Gobj, len(subs))
	for i, v := range subs {
		client.args[i] = data.CreateObject(conf.GSTR, v)
	}
	return true, nil
}

func handleBulkBuf(client *GodisClient) (bool, error) {
	if client.bulkNum == 0 {
		index, err := client.findLineInQuery()
		if index < 0 {
			return false, err
		}
		bnum, err := client.getNumInQuery(1, index)
		if err != nil {
			return false, err
		}
		if bnum == 0 {
			return true, nil
		}
		client.bulkNum = bnum
		client.args = make([]*data.Gobj, bnum)
	}
	for client.bulkNum > 0 {
		if client.bulkLen == 0 {
			index, err := client.findLineInQuery()
			if index < 0 {
				return false, err
			}
			if client.queryBuf[0] != '$' {
				return false, errs.WrongCmdError
			}
			blen, err := client.getNumInQuery(1, index)
			if err != nil || blen == 0 {
				return false, err
			}
			if blen > conf.GODIS_MAX_BULK {
				return false, errs.OutOfLimitError
			}
			client.bulkLen = blen
		}
		if client.queryLen < client.bulkLen+2 {
			return false, nil
		}
		index := client.bulkLen
		if client.queryBuf[index] != '\r' || client.queryBuf[index+1] != '\n' {
			return false, errs.WrongCmdError
		}
		client.args[len(client.args)-client.bulkNum] = data.CreateObject(conf.GSTR, util.BytesToString(client.queryBuf[:index]))
		client.queryBuf = client.queryBuf[index+2:]
		client.queryLen -= index + 2
		client.bulkLen = 0
		client.bulkNum--
	}
	return true, nil
}

func ProcessQueryBuf(client *GodisClient) error {
	for client.queryLen > 0 {
		if client.cmdTy == conf.COMMAND_UNKNOWN {
			if client.queryBuf[0] == '*' {
				client.cmdTy = conf.COMMAND_BULK
			} else {
				client.cmdTy = conf.COMMAND_INLINE
			}
		}
		var ok bool
		var err error
		if client.cmdTy == conf.COMMAND_INLINE {
			ok, err = handleInlineBuf(client)
		} else if client.cmdTy == conf.COMMAND_BULK {
			ok, err = handleBulkBuf(client)
		} else {
			return errs.WrongCmdError
		}
		if err != nil {
			return err
		}
		if ok {
			if len(client.args) == 0 {
				resetClient(client)
			} else {
				ProcessCommand(client)
			}
		} else {
			break
		}
	}
	return nil
}

func lookupCommand(cmdStr string) *GodisCommand {
	for _, c := range cmdTable {
		if len(cmdStr) == len(c.name) {
			for i := range cmdStr {
				if cmdStr[i] != c.name[i] && c.name[i]-cmdStr[i] != sub {
					break
				}
				if i == len(c.name)-1 {
					return c
				}
			}
		}
	}
	return nil
}

func SendReplyToClient(loop *AeLoop, fd int, extra any) {
	client := extra.(*GodisClient)
	// client.logEntry.Debugf("SendReplyToClient, reply len:%v\n", client.reply.Length())
	for client.reply.Length() > 0 {
		rep := client.reply.First()
		buf := rep.Val.Val_.([]byte)
		bufLen := len(buf)
		if client.sentLen < bufLen {
			n, err := net.Write(fd, buf[client.sentLen:])
			if err != nil {
				client.logEntry.Errorf("send reply err: %v\n", err)
				freeClient(client)
				return
			}
			client.sentLen += n
			// client.logEntry.Debugf("send %v bytes to client:%v\n", n, client.fd)
			if client.sentLen == bufLen {
				client.reply.DelNode(rep)
				rep.Val.DecrRefCount()
				client.sentLen = 0
			} else {
				break
			}
		}
	}
	if client.reply.Length() == 0 {
		client.sentLen = 0
		loop.RemoveWriteEvent(fd, AE_WRITABLE)
	}
}

func (client *GodisClient) AddReply(o *data.Gobj) {
	client.reply.Append(o)
	server.AeLoop.ModWriteEvent(client.fd, AE_WRITABLE, SendReplyToClient, client)
}

func (client *GodisClient) AddReplyStr(str string) {
	if client.fd != -1 {
		bytes := util.StringToBytes(str)
		o := data.CreateObject(conf.GBYTES, bytes)
		client.AddReply(o)
	}
}

func (client *GodisClient) AddReplyBytes(bytes []byte) {
	if client.fd != -1 {
		o := data.CreateObject(conf.GBYTES, bytes)
		client.AddReply(o)
	}
}
func ProcessCommand(c *GodisClient) {
	cmdStr := c.args[0].StrVal()
	// c.logEntry.Debugf("process command: %v\n", cmdStr)
	if cmdStr == "quit" {
		freeClient(c)
		return
	}
	cmd := lookupCommand(cmdStr)
	if cmd == nil {
		c.AddReplyStr(fmt.Sprintf("-ERR unknown command '%s'\r\n", cmdStr))
		resetClient(c)
		return
	}
	if cmd.arity != MULTI_ARGS_COMMAND && cmd.arity != len(c.args) {
		c.AddReplyStr(fmt.Sprintf("-ERR wrong number of arguments for '%s' command\r\n", cmdStr))
		resetClient(c)
		return
	}

	start := util.GetUsTime()
	ok, err := cmd.proc(c)
	if err != nil {
		resetClient(c)
		return
	}
	duration := util.GetUsTime() - start
	if duration > server.SlowLogSlowerThan {
		if server.Slowlog.Length() >= server.SlowLogMaxLen {
			server.Slowlog.RPop()
		}
		for _, v := range c.args {
			v.IncrRefCount()
		}
		server.Slowlog.LPush(data.CreateObject(conf.GSLOWLOG, &data.SlowLogEntry{
			ID: func() int64 {
				s, err := util.NewSnowFlake(c.logEntry.Logger, server.workerID)
				if err != nil {
					c.logEntry.Errorf("NewSnowFlake failed: %v\n", err)
					return 0
				}
				id, err := s.NextID()
				if err != nil {
					c.logEntry.Errorf("Generate SnowFlake ID failed: %v\n", err)
					return 0
				}
				return id
			}(),
			Duration: duration,
			Time:     start,
			Robj:     c.args,
			Argc:     len(c.args),
		}))
	}

	if c.fd == -1 || !server.AOF.AppendOnly {
		resetClient(c)
		return
	}

	if cmd.isModify && ok {
		//针对expire命令，需要计算过期的绝对时间
		if cmd.name == "expire" {
			err := server.AOF.PersistExpireCommand(c.args)
			if err != nil {
				c.logEntry.Errorf("AOF persist failed. Command: %v Appendfsync: %d Err: %v\r\n", server.AOF.Command, server.AOF.Appendfsync, err)
			}
		} else {
			err := server.AOF.PersistCommand(c.args)
			if err != nil {
				c.logEntry.Errorf("AOF persist failed. Command: %v Appendfsync: %d Err: %v\r\n", server.AOF.Command, server.AOF.Appendfsync, err)
			}
		}
		resetClient(c)
	}
}

func ReadQueryFromClient(loop *AeLoop, fd int, extra any) {
	client := extra.(*GodisClient)

	if client.closed {
		freeClient(client)
		return
	}
	// client.logEntry.Debugf("ReadQueryFromClient, queryBuf : %v\n", string(client.queryBuf))
	err := ProcessQueryBuf(client)
	if err != nil {
		client.logEntry.Errorf("process query buf err: %v\n", err)
		freeClient(client)
		return
	}
}

func (client *GodisClient) ReadQueryFromAOF() {
	for {
		n, err := server.AOF.Buffer.Read(client.queryBuf[client.queryLen : client.queryLen+4096])
		if err != nil {
			break
		}
		// 更新client参数
		// fmt.Println(n, client.queryLen, len(client.queryBuf), cap(client.queryBuf))
		client.queryLen += n
		client.logEntry.Printf("read %v bytes from client:%v\n", n, client.fd)
		// client.logEntry.Printf("ReadQueryFromAOF, queryBuf : %v\n", client.queryBuf[:client.queryLen])

		err = ProcessQueryBuf(client)
		if err != nil {
			client.logEntry.Printf("process query buf err: %v\n", err)
			continue
		}

		if len(client.queryBuf)-client.queryLen < conf.GODIS_MAX_BULK {
			client.queryBuf = append(client.queryBuf, make([]byte, conf.GODIS_MAX_BULK)...)
		}

	}
}

func freeArgs(client *GodisClient) {
	for i := range client.args {
		client.args[i].DecrRefCount()
	}
	client.args = client.args[:0]
}

func freeReplyList(client *GodisClient) {
	for client.reply.Length() != 0 {
		n := client.reply.Head
		client.reply.DelNode(n)
		n.Val.DecrRefCount()
	}
}
func freeClient(client *GodisClient) {
	freeArgs(client)
	delete(server.clients, client.fd)
	server.AeLoop.RemoveFileEvent(client.fd, AE_READABLE)
	freeReplyList(client)
	net.Close(client.fd)
}

func freeAOFClient(client *GodisClient) {
	freeArgs(client)
	delete(server.clients, client.fd)
	freeReplyList(client)
}

func (client *GodisClient) ReadBuffer() {
	if len(client.queryBuf)-client.queryLen < conf.GODIS_MAX_BULK {
		client.queryBuf = append(client.queryBuf, make([]byte, conf.GODIS_MAX_BULK)...)
	}
	n, err := net.Read(client.fd, client.queryBuf[client.queryLen:])
	if err != nil {
		client.logEntry.Errorf("client %v read err: %v\n", client.fd, err)
		client.closed = true
		return
	}
	if n == 0 {
		client.closed = true
		return
	}
	client.queryLen += n
}
