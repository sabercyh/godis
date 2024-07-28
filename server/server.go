package server

import (
	"os"
	"runtime"
	"time"

	"github.com/godis/conf"
	"github.com/godis/data"
	"github.com/godis/db"
	"github.com/godis/errs"
	"github.com/godis/net"
	"github.com/godis/persistence"
	"github.com/panjf2000/ants/v2"
	"github.com/rs/zerolog"
)

type GodisServer struct {
	fd       int
	port     int
	workerID int64
	DB       *db.GodisDB
	clients  map[int]*GodisClient
	AeLoop   *AeLoop
	logger   *zerolog.Logger
	AOF      *persistence.AOF
	RDB      *persistence.RDB

	Slowlog           *data.List
	SlowLogEntryID    int64
	SlowLogSlowerThan int64
	SlowLogMaxLen     int

	MaxClients int

	ReadPool  *ants.PoolWithFunc
	WritePool *ants.PoolWithFunc
}

var server *GodisServer // 定义server全局变量

func AcceptHandler(loop *AeLoop, fd int, extra any) {
	// 限制最大连接数
	if len(server.clients) >= server.MaxClients {
		server.logger.Info().Msg("exceed max clients len")
		return
	}

	cfd, err := net.Accept(fd)
	if err != nil {
		server.logger.Error().Err(err).Msg("accept err")
		return
	}
	client := InitGodisClientInstance(cfd, server)
	server.clients[cfd] = client
	server.AeLoop.AddReadEvent(cfd, AE_READABLE, ReadQueryFromClient, client)
	server.logger.Debug().Msgf("accept client, fd: %v\n", cfd)
}

func ServerCron(loop *AeLoop, id int, extra any) {
	for i := 0; i < conf.EXPIRE_CHECK_COUNT; i++ {
		entry := server.DB.Expire.RandomGet()
		if entry == nil {
			break
		}
		expireTime, err := entry.Val.Int64Val()
		if err != nil {
			server.logger.Error().Err(err).Msgf("expire time is not int64, key: %v", entry.Key.StrVal())
			continue
		}

		if expireTime < time.Now().Unix() {
			server.DB.Data.Delete(entry.Key)
			server.DB.Expire.Delete(entry.Key)
		}
	}
}

func InitGodisServerInstance(config *conf.Config, logger *zerolog.Logger) (*GodisServer, error) {
	server = &GodisServer{
		port:     config.Port,
		workerID: config.WorkerID,
		clients:  make(map[int]*GodisClient),
		DB: &db.GodisDB{
			Data:   data.DictCreate(),
			Expire: data.DictCreate(),
		},
		logger:            logger,
		AOF:               persistence.InitAOF(config, logger),
		RDB:               persistence.InitRDB(config, logger),
		Slowlog:           data.ListCreate(data.ListType{EqualFunc: data.GStrEqual}),
		SlowLogSlowerThan: config.SlowLogSlowerThan,
		SlowLogMaxLen:     config.SlowLogMaxLen,
		MaxClients:        config.MaxClients,
	}

	if server.AOF.AppendOnly {
		AOFClient := InitGodisClientInstance(-1, server)
		AOFClient.ReadQueryFromAOF()
		freeAOFClient(AOFClient)
	} else {
		err := server.RDB.Load(server.DB)
		if err != nil && err != errs.RDBFileNotExistError {
			server.logger.Error().Err(err).Msg("")
			os.Exit(0)
		}
	}

	var err error
	if server.AeLoop, err = AeLoopCreate(logger); err != nil {
		return nil, err
	}
	if server.fd, err = net.TcpServer(server.port, server.logger); err != nil {
		server.logger.Error().Msg("[msg:server start fail]")
	}

	server.ReadPool, err = ants.NewPoolWithFunc(runtime.GOMAXPROCS(0), func(fd interface{}) {
		ReadBuffer(fd.(int))
		wg.Done()
	})
	if err != nil {
		server.logger.Error().Msg("[msg:create read pool fail]")
	}

	server.WritePool, err = ants.NewPoolWithFunc(runtime.GOMAXPROCS(0), func(fd interface{}) {
		SendReplyToClient(fd.(int))
		wg.Done()
	})
	if err != nil {
		server.logger.Error().Msg("[msg:create write pool fail]")
	}

	server.AeLoop.AddReadEvent(server.fd, AE_READABLE, AcceptHandler, nil)
	server.AeLoop.AddTimeEvent(AE_NORMAL, 100, ServerCron, nil)
	server.logger.Info().Msg("[msg:godis server is up]")
	return server, nil
}
