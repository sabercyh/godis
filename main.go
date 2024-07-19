package main

import (
	"os"
	"runtime/pprof"

	"github.com/godis/conf"
	"github.com/godis/server"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func main() {
	f, _ := os.OpenFile("./cpu.pprof", os.O_CREATE|os.O_RDWR, 0644)
	defer f.Close()
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

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
		log.Errorf("[msg: load godis config failed] [err: %v]\r\n", err)
	}
	if err := viper.Unmarshal(&config); err != nil {
		log.Errorf("[msg: unmarshal godis config failed] [err: %v]\r\n", err)
	}
	log.Infof("[msg: start godis with config] [%#v ]\r\n", config)

	server, err := server.InitGodisServerInstance(&config, log)
	if err != nil {
		log.Errorf("[msg: init server failed] [err: %v]\r\n", err)
	}

	log.Info("[msg: Godis is running]\r\n")
	server.AeLoop.AeMain()
}
