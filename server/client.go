package server

import (
	"errors"
	"log"
	"strconv"
	"strings"
	"github.com/godis/ae"
	"github.com/godis/conf"
	"github.com/godis/data"
	"github.com/godis/db"
	"github.com/godis/net"
)

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
}

// 建立连接成功创建client实例
func InitGodisClientInstance(fd int, server *GodisServer) *GodisClient {
	return &GodisClient{
		fd: fd,
		db: server.DB,
		queryBuf: make([]byte, conf.GODIS_IO_BUF),
		reply: data.ListCreate(data.ListType{EqualFunc: data.GStrEqual}),
	}
}

func resetClient(client *GodisClient) {
	freeArgs(client)
	client.cmdTy = conf.COMMAND_UNKNOWN
	client.bulkLen = 0
	client.bulkNum = 0
}

func (client *GodisClient) findLineInQuery() (int, error) {
	// "\r\n" 不存在这个字符串时返回 -1
	/*
	已经确定是inline了 为啥还要寻找 \r\n
	*/
	index := strings.Index(string(client.queryBuf[ : client.queryLen]), "\r\n")
	/*
	当index < 0 时表示不存在 \r\n
	*/
	if index < 0 && client.queryLen > conf.GODIS_MAX_INLINE {
		return index, errors.New("too big inline cmd")
	}
	return index, nil
}

func (client *GodisClient) getNumInQuery(s, e int) (int, error) {
	num, err := strconv.Atoi(string(client.queryBuf[s : e])) // 将字符串转数字
	client.queryBuf = client.queryBuf[e+2 : ]
	client.queryLen -= e + 2
	return num, err
}

func handleInlineBuf(client *GodisClient) (bool, error) {
	index := strings.Index(string(client.queryBuf[ : client.queryLen]), "\r\n")
	if index < 0 {
		if client.queryLen > conf.GODIS_MAX_INLINE {
		return false, errors.New("too big inline cmd")
		} else {
			return false, errors.New("unknown error")
		}
	} 
	// 分离命令的名字、参数
	subs := strings.Split(string(client.queryBuf[ : index]), " ")
	client.queryBuf = client.queryBuf[index + 2 : ] // index : \r index + 1 : \n
	client.queryLen -= index + 2 // 清除缓存区
	// 参数也要用object存吗
	client.args = make([]*data.Gobj, len(subs))
	for i, v := range subs {
		client.args[i] = data.CreateObject(conf.GSTR, v)
	}
	return true, nil
}

/*
multibulk 命令的结构体
*命令的字符串个数\r\n
$字符串长度\r\n字符串\r\n
$字符串长度\r\n字符串\r\n ...
*/

func handleBulkBuf(client *GodisClient) (bool, error) {
	/*
	if client.bulkNum = 0 表示对于该命令是第一次处理
	需要知道这次指令的bulk的数目
	*/
	if client.bulkNum == 0 {
		index, err := client.findLineInQuery()
		if index < 0 {
			return false, err
		}
		// 刚开始处理的bulk 需要得知bulk的数量一共有多少 
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
	// read every bulk string
	for client.bulkNum > 0 {
		// read bulk length
		if client.bulkLen == 0 {
			index, err := client.findLineInQuery()
			if index < 0 {
				return false, err
			}

			if client.queryBuf[0] != '$' {
				return false, errors.New("expect $ for bulk length")
			}

			blen, err := client.getNumInQuery(1, index)
			if err != nil || blen == 0 {
				return false, err
			}
			if blen > conf.GODIS_MAX_BULK {
				return false, errors.New("too big bulk")
			}
			client.bulkLen = blen
		}
		// read bulk string
		if client.queryLen < client.bulkLen+2 {
			return false, nil
		}
		index := client.bulkLen
		if client.queryBuf[index] != '\r' || client.queryBuf[index+1] != '\n' {
			return false, errors.New("expect CRLF for bulk end")
		}
		client.args[len(client.args)-client.bulkNum] = data.CreateObject(conf.GSTR, string(client.queryBuf[:index]))
		client.queryBuf = client.queryBuf[index+2:]
		client.queryLen -= index + 2
		client.bulkLen = 0
		client.bulkNum--
	}
	// complete reading every bulk
	return true, nil
}

func ProcessQueryBuf(client *GodisClient) error {
	// multibuk时，
	for client.queryLen > 0 {
		// 如果 client.cmdTy == conf.COMMAND_UNKNOWN 时，表示刚开始处理该指令
		// 需要对该指令的类型进行赋值 INLINE or BULK
		if client.cmdTy == conf.COMMAND_UNKNOWN {
			if client.queryBuf[0] == '*' {
				client.cmdTy = conf.COMMAND_BULK
			} else {
				client.cmdTy = conf.COMMAND_INLINE
			}
		}
		// trans query -> args 命令类型有问题
		var ok bool
		var err error
		if client.cmdTy == conf.COMMAND_INLINE {
			ok, err = handleInlineBuf(client)
		} else if client.cmdTy == conf.COMMAND_BULK {
			ok, err = handleBulkBuf(client)
		} else {
			return errors.New("unknow Godis Command Type")
		}
		// buffer不完整 命令输入参数不够？？
		if err != nil {
			return err
		}
		// after query -> args
		if ok {
			if len(client.args) == 0 {
				// 等待下一次命令
				resetClient(client)
			} else {
				// 得到相应的参数，进行命令的处理
				ProcessCommand(client)
			}
		} else {
			// cmd incomplete
			break
		}
	}
	return nil
}

func lookupCommand(cmdStr string) *GodisCommand {
	for _, c := range cmdTable {
		if c.name == cmdStr {
			return c
		}
	}
	return nil
}

func SendReplyToClient(loop *ae.AeLoop, fd int, extra any) {
	client := extra.(*GodisClient)
	log.Printf("SendReplyToClient, reply len:%v\n", client.reply.Length())
	for client.reply.Length() > 0 {
		rep := client.reply.First()
		buf := []byte(rep.Val.StrVal())
		bufLen := len(buf)
		if client.sentLen < bufLen {
			n, err := net.Write(fd, buf[client.sentLen:])
			if err != nil {
				log.Printf("send reply err: %v\n", err)
				freeClient(client)
				return
			}
			client.sentLen += n
			log.Printf("send %v bytes to client:%v\n", n, client.fd)
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
		loop.RemoveFileEvent(fd, ae.AE_WRITABLE)
	}
}

func (client *GodisClient) AddReply(o *data.Gobj) {
	client.reply.Append(o)
	o.IncrRefCount()
	server.AeLoop.AddFileEvent(client.fd, ae.AE_WRITABLE, SendReplyToClient, client)
}

func (client *GodisClient) AddReplyStr(str string) {
	o := data.CreateObject(conf.GSTR, str)
	client.AddReply(o)
	o.DecrRefCount()
}

func ProcessCommand(c *GodisClient) {
	cmdStr := c.args[0].StrVal()
	log.Printf("process command: %v\n", cmdStr)
	if cmdStr == "quit" {
		freeClient(c)
		return
	}
	cmd := lookupCommand(cmdStr)
	if cmd == nil {
		c.AddReplyStr("-ERR: unknow command")
		resetClient(c)
		return
	} else if cmd.arity != len(c.args) {
		c.AddReplyStr("-ERR: wrong number of args")
		resetClient(c)
		return
	}
	cmd.proc(c)
	resetClient(c)
}

func ReadQueryFromClient(loop *ae.AeLoop, fd int, extra any) {
	// 将interface{}的值断言成GodisClient结构 等价于 client = server.client[fd]
	client := extra.(*GodisClient)
	// 当时用multibulk指令时，当兑取一个新的bulk时，要判断当前缓冲区的剩余空间是否能存下一个bulk
	if len(client.queryBuf)-client.queryLen < conf.GODIS_MAX_BULK {
		client.queryBuf = append(client.queryBuf, make([]byte, conf.GODIS_MAX_BULK)...)
	}
	// 继续读取指令
	n, err := net.Read(fd, client.queryBuf[client.queryLen : ])
	// 读取发生错误，断开与server的连接
	if err != nil {
		log.Printf("client %v read err: %v\n", fd, err)
		freeClient(client)
		return
	}
	// 更新client参数
	client.queryLen += n
	log.Printf("read %v bytes from client:%v\n", n, client.fd)
	log.Printf("ReadQueryFromClient, queryBuf : %v\n", string(client.queryBuf))
	err = ProcessQueryBuf(client)
	if err != nil {
		log.Printf("process query buf err: %v\n", err)
		freeClient(client)
		return
	}
}

func freeArgs(client *GodisClient) {
	for _, v := range client.args {
		v.DecrRefCount()
	}
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
	server.AeLoop.RemoveFileEvent(client.fd, ae.AE_READABLE)
	server.AeLoop.RemoveFileEvent(client.fd, ae.AE_WRITABLE)
	freeReplyList(client)
	net.Close(client.fd)
}
