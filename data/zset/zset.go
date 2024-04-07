package zset

import (
	"github.com/godis/data"
	"github.com/godis/data/zset/skiplist"
	"github.com/godis/server"
)

type SortedSet struct {
	dict *data.Dict
	skiplist *skiplist.SkipList
}




func LoadZsetCommand() {
	// Xadd指令
	server.RegisterCommand(server.NewGodisCommand("xadd", XAddCommand, 3))
}

func XAddCommand(c *server.GodisClient) {
	
}
