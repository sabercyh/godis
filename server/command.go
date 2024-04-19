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

func NewGodisCommand(name string, proc CommandProc, arity int) *GodisCommand {
	return &GodisCommand{
		name:  name,
		proc:  proc,
		arity: arity,
	}
}

var cmdTable = map[string]*GodisCommand{
	"set":    NewGodisCommand("set", setCommand, 2),
	"get":    NewGodisCommand("get", getCommand, 3),
	"expire": NewGodisCommand("expire", expireCommand, 3),
	"zadd":   NewGodisCommand("zadd", zaddCommand, 4),
	"zcard":  NewGodisCommand("zcard", zcardCommand, 2),
	"zscore": NewGodisCommand("zscore", zscoreCommand, 3),
	// ZRANGE key start stop
	"zrange": NewGodisCommand("zrange", zrangeCommand, 4),
	// ZRANK key member
	"zrank": NewGodisCommand("zrank", zrankCommand, 3),
	// ZREM key member
	"zrem": NewGodisCommand("zrem", zremCommand, 3),
	// ZCOUNT key min max
	"zcount": NewGodisCommand("zcount", zcountCommand, 4),
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

func zaddCommand(c *GodisClient) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj != nil {
		if zsObj.Type_ != conf.GZSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return
		}
	} else {
		zsObj = data.CreateObject(conf.GZSET, data.NewZset())
		server.DB.Data.Set(key, zsObj)
	}
	zs := zsObj.Val_.(*data.ZSet)
	zaddReply := zs.Zadd(c.args[2:])
	if zaddReply.Err != nil {
		zsObj.DecrRefCount()
		c.AddReplyStr(zaddReply.Err.Error() + "\r\n")
	} else {
		c.AddReplyStr("update (integer)" + fmt.Sprint(zaddReply.UpdateCount) + "\r\n")
		c.AddReplyStr("new (integer)" + fmt.Sprint(zaddReply.NewCount) + "\r\n")
	}
}

func zcardCommand(c *GodisClient) {
	key := c.args[1]
	// 判断key是否存在
	zsObj := server.DB.Data.Get(key)
	if zsObj == nil {
		c.AddReplyStr("(integer) 0\r\n")
	} else {
		zs := zsObj.Val_.(*data.ZSet)
		c.AddReplyStr("(integer) " + fmt.Sprint(zs.Zcard()))
	}
}

func zscoreCommand(c *GodisClient) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj == nil {
		c.AddReplyStr("nil" + "\r\n")
		return
	}
	if err := zsObj.CheckType(conf.GZSET); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
		return
	}
	zs := zsObj.Val_.(*data.ZSet)
	str := zs.Zscore(c.args[2])
	if str == "" {
		c.AddReplyStr("nil" + "\r\n")
		return
	}
	c.AddReplyStr(str + "\r\n")
}

func zrangeCommand(c *GodisClient) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj == nil {
		c.AddReplyStr("(empty list or set)" + "\r\n")
		return
	}
	if err := zsObj.CheckType(conf.GZSET); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
	}
	zs := zsObj.Val_.(*data.ZSet)
	// 从object中提取 start end
	if strSlice, err := zs.Zrange(c.args[2], c.args[3]); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
		return
	} else {
		for i := 0; i < len(strSlice); i++ {
			c.AddReplyStr(strSlice[i] + "\r\n")
		}
	}
}

func zremCommand(c *GodisClient) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj == nil {
		c.AddReplyStr("(integer) 0" + "\r\n")
		return
	}
	if err := zsObj.CheckType(conf.GZSET); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
		return
	}
	zs := zsObj.Val_.(*data.ZSet)
	if n, err := zs.ZREM(c.args[2], c.args[3]); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
	} else {
		c.AddReplyStr("(integer)" + fmt.Sprint(n) + "\r\n")
	}
}

func zrankCommand(c *GodisClient) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj == nil {
		c.AddReplyStr("(integer) 0" + "\r\n")
		return
	}
	if err := zsObj.CheckType(conf.GZSET); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
	}
	zs := zsObj.Val_.(*data.ZSet)
	if rank, err := zs.ZRANK(c.args[2]); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
	} else {
		c.AddReplyStr("(integer)" + fmt.Sprint(rank) + "\r\n")
	}
}

func zcountCommand(c *GodisClient) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj == nil {
		c.AddReplyStr("(integer) 0" + "\r\n")
		return
	}
	if err := zsObj.CheckType(conf.GZSET); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
		return
	}
	if number, err := zsObj.Val_.(*data.ZSet).Zcount(c.args[2], c.args[3]); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
	} else {
		c.AddReplyStr("(integer) " + fmt.Sprint(number) + "\r\n")
	}
}
