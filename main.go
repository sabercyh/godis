package main

import (
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
	log := zerolog.
		New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.DateTime}).
		Level(zerolog.InfoLevel).
		With().Caller().
		Timestamp().
		Logger()

	if log.GetLevel() == zerolog.DebugLevel {
		f, _ := os.OpenFile("./cpu.pprof", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
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
