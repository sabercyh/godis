package persistence

import (
	"bufio"
	"fmt"
	"os"
	"strconv"

	"github.com/godis/conf"
	"github.com/godis/data"
	"github.com/godis/errs"
	"github.com/godis/util"
	"github.com/sirupsen/logrus"
)

type AOF struct {
	Buffer      *bufio.ReadWriter //有缓冲持久化
	AppendOnly  bool              //是否启用AOF
	File        *os.File          //AOF文件句柄
	Appendfsync int               //0:AOF_FSYNC_ALWAYS|1:AOF_FSYNC_EVERYSEC|2:AOF_FSYNC_NO
	Command     string            //待持久化的完整命令
	when        int64             //上次刷盘时间
}

func InitAOF(config *conf.Config, logger *logrus.Logger) *AOF {
	var err error
	aof := &AOF{
		AppendOnly: config.AppendOnly,
		when:       0,
		Command:    "",
	}

	// 若有AOF文件则直接打开，不存在则创建
	aof.File, err = os.OpenFile(config.Dir+config.AppendFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		logger.Info("open aof file\r\n")
	}
	aof.Buffer = bufio.NewReadWriter(bufio.NewReader(aof.File), bufio.NewWriterSize(aof.File, config.AOFBufferSize))

	switch config.Appendfsync {
	case "AOF_FSYNC_ALWAYS":
		aof.Appendfsync = 0
	case "AOF_FSYNC_EVERYSEC":
		aof.Appendfsync = 1
	case "AOF_FSYNC_NO":
		aof.Appendfsync = 2
	}
	return aof
}

func (aof *AOF) FreeCommand() {
	aof.Command = ""
}

/*
以set命令为例，AOF文件中的存储格式为:
*3		命令参数个数
$3		第一个参数的字符数
set		第一个参数
$2
k1
$2
v1
*/
func (aof *AOF) PersistCommand(args []*data.Gobj) error {
	aof.Command += fmt.Sprintf("*%d\r\n", len(args))
	for _, v := range args {
		param := fmt.Sprintf("%v", v.Val_)
		aof.Command += fmt.Sprintf("$%d\r\n%s\r\n", len(param), param)
	}
	err := aof.Persist()
	aof.FreeCommand()

	return err
}

// 额外计算过期绝对时间
func (aof *AOF) PersistExpireCommand(args []*data.Gobj) error {
	aof.Command += fmt.Sprintf("*%d\r\n", len(args))
	cmdStr := args[0].StrVal()
	aof.Command += fmt.Sprintf("$%d\r\n%s\r\n", len(cmdStr), cmdStr)

	expireTime := util.GetTime() + args[2].IntVal()
	// client.logEntry.Printf("now:%d expire:%d\r\n", util.GetTime(), expireTime)
	aof.Command += fmt.Sprintf("$%d\r\n%d\r\n", len(strconv.FormatInt(expireTime, 10)), expireTime)
	err := aof.Persist()
	aof.FreeCommand()

	return err
}

func (aof *AOF) Persist() error {
	var err error
	// 根据刷盘方式写入
	switch aof.Appendfsync {
	case 0:
		aof.Save()
	case 1:
		if util.GetTime()-aof.when > 1 {
			err = aof.Save()
			if err != nil {
				return err
			}
			aof.when = util.GetTime()
		} else {
			aof.Write()
		}
	case 2:
		aof.Write()
	}
	if err != nil {
		return err
	}
	return nil
}

func (aof *AOF) Write() error {
	n, err := aof.Buffer.WriteString(aof.Command)
	if n != len(aof.Command) || err != nil {
		return errs.AOFBufferWriteError
	}
	return nil
}

func (aof *AOF) Save() error {
	n, err := aof.Buffer.WriteString(aof.Command)
	if n != len(aof.Command) || err != nil {
		return errs.AOFBufferWriteError
	}
	err = aof.Buffer.Flush()
	if err != nil {
		return errs.AOFFileSaveError
	}
	return nil
}
