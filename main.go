package main

import (
	"flag"
	"os"
	"runtime/pprof"
	"time"

	"github.com/godis/conf"
	"github.com/godis/server"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	_ "go.uber.org/automaxprocs"
)

func main() {
	var pfile string
	var logLevel string
	flag.StringVar(&pfile, "pfile", "./cpu.pprof", "pprof filename")
	flag.StringVar(&logLevel, "loglevel", "info", "log level")
	flag.Parse()

	log := zerolog.
		New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.DateTime}).
		With().Caller().
		Timestamp().
		Logger()

	if logLevel == "trace" {
		log = log.Level(zerolog.TraceLevel)
	} else if logLevel == "debug" {
		log = log.Level(zerolog.DebugLevel)
	} else if logLevel == "info" {
		log = log.Level(zerolog.InfoLevel)
	}

	if log.GetLevel() == zerolog.TraceLevel {
		f, _ := os.OpenFile(pfile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	var config conf.Config
	viper.AddConfigPath("./conf/")
	viper.SetConfigName("godis-conf")
	if err := viper.ReadInConfig(); err != nil {
		log.Error().Err(err).Msg("[msg:load godis config failed]")
	}
	if err := viper.Unmarshal(&config); err != nil {
		log.Error().Err(err).Msg("[msg:unmarshal godis config failed]")
	}
	log.Info().Interface("config", config).Msg("[msg:start godis with config]")

	server, err := server.InitGodisServerInstance(&config, &log)
	if err != nil {
		log.Error().Err(err).Msg("[msg:init server failed]")
	}

	log.Info().Msg("Godis is running...")
	server.AeLoop.AeMain()
}
