package server

import (
	"time"

	"github.com/godis/ae"
	"github.com/godis/data"
	"github.com/godis/db"
	"github.com/godis/net"
	"github.com/sirupsen/logrus"
)

type GodisServer struct {
	fd      int
	port    int
	DB      *db.GodisDB
	clients map[int]*GodisClient
	AeLoop  *ae.AeLoop
	logger  *logrus.Logger
}

var server *GodisServer // 定义server全局变量

func AcceptHandler(loop *ae.AeLoop, fd int, extra any) {
	// 与监听套接字的fd建立起连接，返回监听这个连接的fd
	cfd, err := net.Accept(fd)
	if err != nil {
		server.logger.Printf("accept err: %v\n", err)
		return
	}
	// 连接fd、服务器实例创建客户端实例
	client := InitGodisClientInstance(cfd, server)
	// TODO: check max clients limit
	// 注册连接
	server.clients[cfd] = client
	// register fileEvent
	server.AeLoop.AddFileEvent(cfd, ae.AE_READABLE, ReadQueryFromClient, client)
	// 接受连接成功
	server.logger.Printf("accept client, fd: %v\n", cfd)
}

const EXPIRE_CHECK_COUNT int = 100

// background job, runs every 100ms 用于后台进行淘汰工作
func ServerCron(loop *ae.AeLoop, id int, extra any) {
	for i := 0; i < EXPIRE_CHECK_COUNT; i++ {
		entry := server.DB.Expire.RandomGet()
		if entry == nil {
			break
		}
		if entry.Val.IntVal() < time.Now().Unix() {
			server.DB.Data.Delete(entry.Key)
			server.DB.Expire.Delete(entry.Key)
		}
	}
}

func InitGodisServerInstance(port int, logger *logrus.Logger) (*GodisServer, error) {
	// 创建redis服务器实例
	server = &GodisServer{
		port:    port,
		clients: make(map[int]*GodisClient),
		DB: &db.GodisDB{
			Data:   data.DictCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}),
			Expire: data.DictCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}),
		},
		logger: logger,
	}
	// 创建AE事件循环 调用epoll_create 监听系统IO
	var err error
	if server.AeLoop, err = ae.AeLoopCreate(logger); err != nil {
		return nil, err
	}
	// 监听端口
	if server.fd, err = net.TcpServer(server.port, server.logger); err != nil {
		server.logger.Println("server start fail")
	}
	// 给服务器端fd添加事件 (fd:监听某个端口事件)
	// 用于监听是否有连接到达，当有连接到达时，调用AcceptHandler对连接请求进行处理
	// 对于服务器端fd事件，只需要读取是够有连接到达，因此设置为ae.AE_READABLE
	// 当和新到的请求建立起连接后，server会创建新的fd来标识这个连接，此时设置为ae.WRITEABLE
	server.AeLoop.AddFileEvent(server.fd, ae.AE_READABLE, AcceptHandler, nil)
	server.AeLoop.AddTimeEvent(ae.AE_NORMAL, 100, ServerCron, nil)
	server.logger.Println("godis server is up.")
	return server, nil
}
