package persistence

import (
	"bufio"
	"io"
	"os"

	"github.com/godis/conf"
	"github.com/sirupsen/logrus"
)

type AOF struct {
	Buffer      *bufio.ReadWriter //有缓冲持久化
	AppendOnly  bool              //是否启用AOF
	File        *os.File          //AOF文件句柄
	Appendfsync bool              //false对应always无缓冲，true对应no有缓冲
	Command     string            //待持久化的完整命令
}

func InitAOF(config *conf.Config, logger *logrus.Logger) *AOF {
	var err error
	aof := &AOF{
		AppendOnly: config.AppendOnly,
		Command:    "",
	}

	// 若有AOF文件则直接打开，不存在则创建
	//Todo，启动时读取是否还需创建？
	aof.File, err = os.OpenFile(config.Dir+config.AppendFilename, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		logger.Info("not found aof file\r\n")
		aof.File, _ = os.Create(config.Dir + config.AppendFilename)
	}

	// 刷盘方式，always表示每条命令都直接写入，no表示每条命令写入缓冲区，由操作系统判断刷盘
	if config.Appendfsync == "always" {
		aof.Appendfsync = false
	} else {
		aof.Appendfsync = true
	}

	if aof.Appendfsync {
		aof.Buffer = bufio.NewReadWriter(bufio.NewReader(aof.File), bufio.NewWriter(aof.File))
	}
	return aof
}

func (aof *AOF) FreeCommand() {
	aof.Command = ""
}

func (aof *AOF) Persistent() {
	// log.Println(aof.Command)
	// 根据刷盘方式写入
	if aof.Appendfsync {
		aof.Buffer.WriteString(aof.Command)
	} else {
		io.WriteString(aof.File, aof.Command)
	}
}
