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
	log.SetReportCaller(true)
	// log.SetLevel(logrus.DebugLevel)

	var config conf.Config
	viper.AddConfigPath("./conf/")
	viper.SetConfigName("godis-conf")
	if err := viper.ReadInConfig(); err != nil {
		log.Errorf("[msg: load godis config failed] [err: %v\r\n]", err)
	}
	if err := viper.Unmarshal(&config); err != nil {
		log.Errorf("[msg: unmarshal godis config failed] [err: %v\r\n]", err)
	}
	log.Debugf("[msg: start godis with config] [%#v \r\n]", config)

	server, err := server.InitGodisServerInstance(&config, log)
	if err != nil {
		log.Errorf("[msg: init server failed] [err: %v\n]", err)
	}

	log.Info("[msg: Godis is running]")
	server.AeLoop.AeMain()
}
