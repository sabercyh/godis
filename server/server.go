package server

import (
	"hash/fnv"
	"log"
	"time"

	"github.com/godis/ae"
	"github.com/godis/conf"
	"github.com/godis/data"
	"github.com/godis/db"
	"github.com/godis/net"
)

type GodisServer struct {
	fd      int
	port    int
	DB      *db.GodisDB
	clients map[int]*GodisClient
	AeLoop  *ae.AeLoop
}

var server *GodisServer

func AcceptHandler(loop *ae.AeLoop, fd int, extra any) {
	cfd, err := net.Accept(fd)
	if err != nil {
		log.Printf("accept err: %v\n", err)
		return
	}
	client := InitGodisClientInstance(cfd, server)
	// TODO: check max clients limit
	server.clients[cfd] = client
	server.AeLoop.AddFileEvent(cfd, ae.AE_READABLE, ReadQueryFromClient, client)
	log.Printf("accept client, fd: %v\n", cfd)
}

const EXPIRE_CHECK_COUNT int = 100

// background job, runs every 100ms
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

func GStrEqual(a, b *data.Gobj) bool {
	if a.Type_ != conf.GSTR || b.Type_ != conf.GSTR {
		return false
	}
	return a.StrVal() == b.StrVal()
}

func GStrHash(key *data.Gobj) int64 {
	if key.Type_ != conf.GSTR {
		return 0
	}
	hash := fnv.New64()
	hash.Write([]byte(key.StrVal()))
	return int64(hash.Sum64())
}

func InitGodisServerInstance(port int) (*GodisServer, error) {
	server = &GodisServer{
		port:    port,
		clients: make(map[int]*GodisClient),
		DB: &db.GodisDB{
			Data:   data.DictCreate(data.DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
			Expire: data.DictCreate(data.DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
		},
	}

	var err error
	if server.AeLoop, err = ae.AeLoopCreate(); err != nil {
		return nil, err
	}
	server.fd, err = net.TcpServer(server.port)

	server.AeLoop.AddFileEvent(server.fd, ae.AE_READABLE, AcceptHandler, nil)
	server.AeLoop.AddTimeEvent(ae.AE_NORMAL, 100, ServerCron, nil)
	log.Println("godis server is up.")
	return server, nil
}
