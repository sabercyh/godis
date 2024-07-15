package server

import (
	"os"
	"time"

	"github.com/godis/ae"
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
	AeLoop   *ae.AeLoop
	logger   *logrus.Logger
	AOF      *persistence.AOF
	RDB      *persistence.RDB

	Slowlog           *data.List
	SlowLogEntryID    int64
	SlowLogSlowerThan int64
	SlowLogMaxLen     int
}

var server *GodisServer // 定义server全局变量

type SlowLogEntry struct {
	robj     []*data.Gobj
	argc     int
	id       int64
	duration int64
	time     int64
}

func AcceptHandler(loop *ae.AeLoop, fd int, extra any) {
	cfd, err := net.Accept(fd)
	if err != nil {
		server.logger.Printf("accept err: %v\n", err)
		return
	}
	client := InitGodisClientInstance(cfd, server)
	// TODO: check max clients limit
	server.clients[cfd] = client
	server.AeLoop.AddFileEvent(cfd, ae.AE_READABLE, ReadQueryFromClient, client)
	server.logger.Debugf("accept client, fd: %v\n", cfd)
}

func ServerCron(loop *ae.AeLoop, id int, extra any) {
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
			Data:   data.DictCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}),
			Expire: data.DictCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}),
		},
		logger:            logger,
		AOF:               persistence.InitAOF(config, logger),
		RDB:               persistence.InitRDB(config, logger),
		Slowlog:           data.ListCreate(data.ListType{EqualFunc: data.GStrEqual}),
		SlowLogSlowerThan: config.SlowLogSlowerThan,
		SlowLogMaxLen:     config.SlowLogMaxLen,
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
	if server.AeLoop, err = ae.AeLoopCreate(logger); err != nil {
		return nil, err
	}
	if server.fd, err = net.TcpServer(server.port, server.logger); err != nil {
		server.logger.Println("server start fail")
	}
	server.AeLoop.AddFileEvent(server.fd, ae.AE_READABLE, AcceptHandler, nil)
	server.AeLoop.AddTimeEvent(ae.AE_NORMAL, 100, ServerCron, nil)
	server.logger.Println("godis server is up.")
	return server, nil
}
