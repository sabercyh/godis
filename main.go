package main

import (
	"os"

	"github.com/godis/conf"
	"github.com/godis/server"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func main() {
	var log = logrus.New()
	log.Out = os.Stdout // 设置输出日志位置，可以设置日志到file里
	log.SetFormatter(&logrus.TextFormatter{
		ForceQuote:      true,                  //键值对加引号
		TimestampFormat: "2006-01-02 15:04:05", //时间格式
		FullTimestamp:   true,
	})

	var config conf.Config
	viper.AddConfigPath("./conf/")
	viper.SetConfigName("godis-conf")
	if err := viper.ReadInConfig(); err != nil {
		log.Printf("read godis config error: %v\n", err)
	}

	if err := viper.Unmarshal(&config); err != nil {
		log.Printf("unmarshal config error: %v\n", err)
	}
	log.Printf("init with config %#v \r\n", config)
	server, err := server.InitGodisServerInstance(&config, log)
	if err != nil {
		log.Printf("init server error: %v\n", err)
	}
	server.AeLoop.AeMain()
}
