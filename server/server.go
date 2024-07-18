package server

import (
	"os"
	"time"

	"github.com/godis/conf"
	"github.com/godis/data"
	"github.com/godis/db"
	"github.com/godis/errs"
	"github.com/godis/net"
	"github.com/godis/persistence"
	"github.com/sirupsen/logrus"
)

type GodisServer struct {
	fd       int
	port     int
	workerID int64
	DB       *db.GodisDB
	clients  map[int]*GodisClient
	AeLoop   *AeLoop
	logger   *logrus.Logger
	AOF      *persistence.AOF
	RDB      *persistence.RDB

	Slowlog           *data.List
	SlowLogEntryID    int64
	SlowLogSlowerThan int64
	SlowLogMaxLen     int

	MaxClients int
}

var server *GodisServer // 定义server全局变量

func AcceptHandler(loop *AeLoop, fd int, extra any) {
	// 限制最大连接数
	if len(server.clients) >= server.MaxClients {
		server.logger.Infoln("exceed max clients len")
		return
	}

	cfd, err := net.Accept(fd)
	if err != nil {
		server.logger.Errorf("accept err: %v\n", err)
		return
	}
	client := InitGodisClientInstance(cfd, server)
	server.clients[cfd] = client
	server.AeLoop.AddReadEvent(cfd, AE_READABLE, ReadQueryFromClient, client)
	server.logger.Debugf("accept client, fd: %v\n", cfd)
}

func ServerCron(loop *AeLoop, id int, extra any) {
	for i := 0; i < conf.EXPIRE_CHECK_COUNT; i++ {
		entry := server.DB.Expire.RandomGet()
		if entry == nil {
			break
		}
		expireTime, err := entry.Val.Int64Val()
		if err != nil {
			server.logger.Printf("expire time is not int64, key: %v, err: %v\n", entry.Key.StrVal(), err)
			continue
		}

		if expireTime < time.Now().Unix() {
			server.DB.Data.Delete(entry.Key)
			server.DB.Expire.Delete(entry.Key)
		}
	}
}

func InitGodisServerInstance(config *conf.Config, logger *logrus.Logger) (*GodisServer, error) {
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
			server.logger.Error(err)
			os.Exit(0)
		}
	}

	var err error
	if server.AeLoop, err = AeLoopCreate(logger); err != nil {
		return nil, err
	}
	if server.fd, err = net.TcpServer(server.port, server.logger); err != nil {
		server.logger.Errorln("server start fail")
	}
	server.AeLoop.AddReadEvent(server.fd, AE_READABLE, AcceptHandler, nil)
	server.AeLoop.AddTimeEvent(AE_NORMAL, 100, ServerCron, nil)
	server.logger.Infoln("godis server is up.")
	return server, nil
}
