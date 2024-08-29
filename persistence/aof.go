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
	"github.com/rs/zerolog"
)

type AOF struct {
	Buffer      *bufio.ReadWriter //有缓冲持久化
	AppendOnly  bool              //是否启用AOF
	File        *os.File          //AOF文件句柄
	Appendfsync int               //0:always|1:everysec|2:no
	Command     string            //待持久化的完整命令
	when        int64             //上次刷盘时间
	logEntry    zerolog.Logger
}

func InitAOF(config *conf.Config, logger *zerolog.Logger) *AOF {
	var err error
	aof := &AOF{
		AppendOnly: config.AppendOnly,
		when:       0,
		Command:    "",
		logEntry:   logger.With().Logger(),
	}

	// 若有AOF文件则直接打开，不存在则创建
	aof.File, err = os.OpenFile(config.Dir+config.AppendFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0666)
	if err != nil {
		logger.Error().Msg("open aof file failed")
	}
	aof.Buffer = bufio.NewReadWriter(bufio.NewReader(aof.File), bufio.NewWriterSize(aof.File, conf.AOF_BUF_BLOCK_SIZE))

	switch config.Appendfsync {
	case "always":
		aof.Appendfsync = 0
	case "everysec":
		aof.Appendfsync = 1
	case "no":
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
	seconds, err := args[2].Int64Val()
	if err != nil {
		return err
	}
	expireTime := util.GetTime() + seconds
	aof.Command += fmt.Sprintf("$%d\r\n%d\r\n", len(strconv.FormatInt(expireTime, 10)), expireTime)
	err = aof.Persist()
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
		aof.logEntry.Error().Msg("flush aof buffer error")
	}

	return nil
}
