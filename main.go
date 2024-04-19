package main

import (
	"os"

	"github.com/godis/server"
	"github.com/sirupsen/logrus"
)

func main() {
	var log = logrus.New()
	log.Out = os.Stdout // 设置输出日志位置，可以设置日志到file里
	log.SetFormatter(&logrus.TextFormatter{
		ForceQuote:      true,                  //键值对加引号
		TimestampFormat: "2006-01-02 15:04:05", //时间格式
		FullTimestamp:   true,
	})
	server, err := server.InitGodisServerInstance(6767, log)

	if err != nil {
		log.Printf("init server error: %v\n", err)
	}
	server.AeLoop.AeMain()
}
