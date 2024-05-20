package server

import (
	"fmt"

	"github.com/godis/conf"
	"github.com/godis/data"
	"github.com/godis/errs"
	"github.com/godis/util"
)

type CommandProc func(c *GodisClient) (bool, error)

// do not support bulk command
type GodisCommand struct {
	name     string
	proc     CommandProc
	arity    int
	isModify bool //是否为修改命令，若是查询命令则不需要持久化
}

func NewGodisCommand(name string, proc CommandProc, arity int, isModify bool) *GodisCommand {
	return &GodisCommand{
		name:     name,
		proc:     proc,
		arity:    arity,
		isModify: isModify,
	}
}

var cmdTable = map[string]*GodisCommand{
	"set":    NewGodisCommand("set", setCommand, 3, true),
	"setnx":  NewGodisCommand("setnx", setnxCommand, 3, true),
	"get":    NewGodisCommand("get", getCommand, 2, false),
	"del":    NewGodisCommand("del", delCommand, 2, true),
	"exists": NewGodisCommand("exists", existsCommand, 2, false),
	"expire": NewGodisCommand("expire", expireCommand, 3, true),
	"zadd":   NewGodisCommand("zadd", zaddCommand, 4, true),
	"zcard":  NewGodisCommand("zcard", zcardCommand, 2, false),
	"zscore": NewGodisCommand("zscore", zscoreCommand, 3, false),
	// ZRANGE key start stop
	"zrange": NewGodisCommand("zrange", zrangeCommand, 4, false),
	// ZRANK key member
	"zrank": NewGodisCommand("zrank", zrankCommand, 3, false),
	// ZREM key member
	"zrem": NewGodisCommand("zrem", zremCommand, 3, true),
	// ZCOUNT key min max
	"zcount": NewGodisCommand("zcount", zcountCommand, 4, false),
}

func expireIfNeeded(key *data.Gobj) {
	entry := server.DB.Expire.Find(key)
	if entry == nil {
		return
	}
	when := entry.Val.IntVal()
	if when > util.GetMsTime() {
		return
	}
	server.DB.Expire.Delete(key)
	server.DB.Data.Delete(key)
}

func findKeyRead(key *data.Gobj) *data.Gobj {
	expireIfNeeded(key)
	return server.DB.Data.Get(key)
}

func getCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := findKeyRead(key)
	if val == nil {
		// TODO: extract shared.strings
		c.AddReplyStr("$-1\r\n")
		return false, errs.KeyNotExistError
	} else if val.Type_ != conf.GSTR {
		// TODO: extract shared.strings
		c.AddReplyStr("-ERR: wrong type\r\n")
		return false, errs.TypeCheckError
	} else {
		str := val.StrVal()
		c.AddReplyStr(fmt.Sprintf("$%d%v\r\n", len(str), str))
	}
	return true, nil

}

func delCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	err := server.DB.Data.Delete(key)
	if err != nil {
		c.AddReplyStr("$-1\r\n")
		return false, errs.DelKeyError
	}
	c.AddReplyStr("$1\r\n")
	return true, nil

}
func setCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := c.args[2]
	if val.Type_ != conf.GSTR {
		// TODO: extract shared.strings
		c.AddReplyStr("-ERR: wrong type\r\n")
		return false, errs.TypeCheckError
	}
	server.DB.Data.Set(key, val)
	server.DB.Expire.Delete(key)
	c.AddReplyStr("+OK\r\n")
	return true, nil
}

func setnxCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := c.args[2]
	if val.Type_ != conf.GSTR {
		// TODO: extract shared.strings
		c.AddReplyStr("-ERR: wrong type\r\n")
		return false, errs.TypeCheckError
	}
	err := server.DB.Data.SetNx(key, val)
	if err != nil {
		c.AddReplyStr("$-1\r\n")
		return false, errs.KeyExistsError
	}
	server.DB.Expire.Delete(key)
	c.AddReplyStr("+OK\r\n")
	return true, nil
}
func existsCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := findKeyRead(key)
	if val == nil {
		c.AddReplyStr("$0\r\n")
		return false, errs.KeyNotExistError
	}
	c.AddReplyStr("$1\r\n")
	return true, nil

}
func expireCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := c.args[2]
	if val.Type_ != conf.GSTR {
		// TODO: extract shared.strings
		c.AddReplyStr("-ERR: wrong type\r\n")
		return false, errs.TypeCheckError
	}
	expire := util.GetMsTime() + (val.IntVal() * 1000)
	expObj := data.CreateObjectFromInt(expire)
	server.DB.Expire.Set(key, expObj)
	expObj.DecrRefCount()
	c.AddReplyStr("+OK\r\n")
	return true, nil
}

func zaddCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj != nil {
		if zsObj.Type_ != conf.GZSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
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
		return false, zaddReply.Err
	} else {
		c.AddReplyStr("update (integer)" + fmt.Sprint(zaddReply.UpdateCount) + "\r\n")
		c.AddReplyStr("new (integer)" + fmt.Sprint(zaddReply.NewCount) + "\r\n")
	}
	return true, nil
}

func zcardCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	// 判断key是否存在
	zsObj := server.DB.Data.Get(key)
	if zsObj == nil {
		c.AddReplyStr("(integer) 0\r\n")
		return false, errs.KeyNotExistError
	} else {
		zs := zsObj.Val_.(*data.ZSet)
		c.AddReplyStr("(integer) " + fmt.Sprint(zs.Zcard()))
	}
	return true, nil
}

func zscoreCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj == nil {
		c.AddReplyStr("nil" + "\r\n")
		return false, errs.KeyNotExistError
	}
	if err := zsObj.CheckType(conf.GZSET); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
		return false, errs.TypeCheckError
	}
	zs := zsObj.Val_.(*data.ZSet)
	str := zs.Zscore(c.args[2])
	if str == "" {
		c.AddReplyStr("nil" + "\r\n")
		return false, errs.KeyNotExistError
	}
	c.AddReplyStr(str + "\r\n")
	return true, nil
}

func zrangeCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj == nil {
		c.AddReplyStr("(empty list or set)" + "\r\n")
		return false, errs.KeyNotExistError
	}
	if err := zsObj.CheckType(conf.GZSET); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
		return false, errs.TypeCheckError
	}
	zs := zsObj.Val_.(*data.ZSet)
	// 从object中提取 start end
	if strSlice, err := zs.Zrange(c.args[2], c.args[3]); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
		return false, err
	} else {
		for i := 0; i < len(strSlice); i++ {
			c.AddReplyStr(strSlice[i] + "\r\n")
		}
	}
	return true, nil
}

func zremCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj == nil {
		c.AddReplyStr("(integer) 0" + "\r\n")
		return false, errs.KeyNotExistError
	}
	if err := zsObj.CheckType(conf.GZSET); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
		return false, errs.TypeCheckError
	}
	zs := zsObj.Val_.(*data.ZSet)
	if n, err := zs.ZREM(c.args[2], c.args[3]); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
		return false, err
	} else {
		c.AddReplyStr("(integer)" + fmt.Sprint(n) + "\r\n")
	}
	return true, nil
}

func zrankCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj == nil {
		c.AddReplyStr("(integer) 0" + "\r\n")
		return false, errs.KeyNotExistError
	}
	if err := zsObj.CheckType(conf.GZSET); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
		return false, errs.TypeCheckError
	}
	zs := zsObj.Val_.(*data.ZSet)
	if rank, err := zs.ZRANK(c.args[2]); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
		return false, err
	} else {
		c.AddReplyStr("(integer)" + fmt.Sprint(rank) + "\r\n")
	}
	return true, nil
}

func zcountCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj == nil {
		c.AddReplyStr("(integer) 0" + "\r\n")
		return false, errs.KeyNotExistError
	}
	if err := zsObj.CheckType(conf.GZSET); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
		return false, errs.TypeCheckError
	}
	if number, err := zsObj.Val_.(*data.ZSet).Zcount(c.args[2], c.args[3]); err != nil {
		c.AddReplyStr(err.Error() + "\r\n")
		return false, err
	} else {
		c.AddReplyStr("(integer) " + fmt.Sprint(number) + "\r\n")
	}
	return true, nil
}
