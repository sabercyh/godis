package server

import (
	"fmt"
	"time"

	"github.com/godis/conf"
	"github.com/godis/data"
)

type CommandProc func(c *GodisClient)

// do not support bulk command
type GodisCommand struct {
	name  string
	proc  CommandProc
	arity int
}

var cmdTable []GodisCommand = []GodisCommand{
	{"get", getCommand, 2},
	{"set", setCommand, 3},
	{"expire", expireCommand, 3},
	// TODO
}

func expireIfNeeded(key *data.Gobj) {
	entry := server.DB.Expire.Find(key)
	if entry == nil {
		return
	}
	when := entry.Val.IntVal()
	if when > GetMsTime() {
		return
	}
	server.DB.Expire.Delete(key)
	server.DB.Data.Delete(key)
}

func findKeyRead(key *data.Gobj) *data.Gobj {
	expireIfNeeded(key)
	return server.DB.Data.Get(key)
}

func GetMsTime() int64 {
	return time.Now().UnixNano() / 1e6
}

func getCommand(c *GodisClient) {
	key := c.args[1]
	val := findKeyRead(key)
	if val == nil {
		// TODO: extract shared.strings
		c.AddReplyStr("$-1\r\n")
	} else if val.Type_ != conf.GSTR {
		// TODO: extract shared.strings
		c.AddReplyStr("-ERR: wrong type\r\n")
	} else {
		str := val.StrVal()
		c.AddReplyStr(fmt.Sprintf("$%d%v\r\n", len(str), str))
	}
}

func setCommand(c *GodisClient) {
	key := c.args[1]
	val := c.args[2]
	if val.Type_ != conf.GSTR {
		// TODO: extract shared.strings
		c.AddReplyStr("-ERR: wrong type\r\n")
	}
	server.DB.Data.Set(key, val)
	server.DB.Expire.Delete(key)
	c.AddReplyStr("+OK\r\n")
}

func expireCommand(c *GodisClient) {
	key := c.args[1]
	val := c.args[2]
	if val.Type_ != conf.GSTR {
		// TODO: extract shared.strings
		c.AddReplyStr("-ERR: wrong type\r\n")
	}
	expire := GetMsTime() + (val.IntVal() * 1000)
	expObj := data.CreateObjectFromInt(expire)
	server.DB.Expire.Set(key, expObj)
	expObj.DecrRefCount()
	c.AddReplyStr("+OK\r\n")
}
