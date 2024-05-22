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
	// string
	"set":    NewGodisCommand("set", setCommand, 3, true),
	"setnx":  NewGodisCommand("setnx", setnxCommand, 3, true),
	"get":    NewGodisCommand("get", getCommand, 2, false),
	"del":    NewGodisCommand("del", delCommand, 2, true),
	"exists": NewGodisCommand("exists", existsCommand, 2, false),
	"expire": NewGodisCommand("expire", expireCommand, 3, true),
	//hash
	"hset":    NewGodisCommand("hset", hsetCommand, 4, true),
	"hget":    NewGodisCommand("hget", hgetCommand, 3, false),
	"hdel":    NewGodisCommand("hdel", hdelCommand, 3, true),
	"hexists": NewGodisCommand("hexists", hexistsCommand, 3, false),
	"hgetall": NewGodisCommand("hgetall", hgetallCommand, 2, false),
	//set
	"sadd":        NewGodisCommand("sadd", saddCommand, 3, true),
	"scard":       NewGodisCommand("scard", scardCommand, 2, false),
	"sismember":   NewGodisCommand("sismember", sismemberCommand, 3, false),
	"smembers":    NewGodisCommand("smembers", smembersCommand, 2, false),
	"srandmember": NewGodisCommand("srandmember", srandmemberCommand, 2, false),
	"srem":        NewGodisCommand("srem", sremCommand, 3, true),
	"sinter":      NewGodisCommand("sinter", sinterCommand, 3, false),
	"sdiff":       NewGodisCommand("sdiff", sdiffCommand, 3, false),
	"sunion":      NewGodisCommand("sunion", sunionCommand, 3, false),
	//zset
	"zadd":   NewGodisCommand("zadd", zaddCommand, 4, true),
	"zcard":  NewGodisCommand("zcard", zcardCommand, 2, false),
	"zscore": NewGodisCommand("zscore", zscoreCommand, 3, false),
	"zrange": NewGodisCommand("zrange", zrangeCommand, 4, false), // ZRANGE key start stop
	"zrank":  NewGodisCommand("zrank", zrankCommand, 3, false),   // ZRANK key member
	"zrem":   NewGodisCommand("zrem", zremCommand, 3, true),      // ZREM key member
	"zcount": NewGodisCommand("zcount", zcountCommand, 4, false), // ZCOUNT key min max
	//bitmap
	"setbit":   NewGodisCommand("setbit", setbitCommand, 4, true),
	"getbit":   NewGodisCommand("getbit", getbitCommand, 3, false),
	"bitcount": NewGodisCommand("bitcount", bitcountCommand, 2, false),
	"bitop":    NewGodisCommand("bitop", bitopCommand, 4, false),
	"bitpos":   NewGodisCommand("bitpos", bitposCommand, 3, false),
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
		c.AddReplyStr("(nil)\r\n")
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
		c.AddReplyStr("(integer) 0\r\n")
		return false, errs.DelKeyError
	}
	c.AddReplyStr("(integer) 1\r\n")
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
		c.AddReplyStr("(integer) 0\r\n")
		return false, errs.KeyExistsError
	}
	server.DB.Expire.Delete(key)
	c.AddReplyStr("(integer) 1\r\n")
	return true, nil
}
func existsCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := findKeyRead(key)
	if val == nil {
		c.AddReplyStr("(integer) 0\r\n")
		return false, errs.KeyNotExistError
	}
	c.AddReplyStr("(integer) 1\r\n")
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

func hsetCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	htObj := server.DB.Data.Get(key)
	if htObj != nil {
		if htObj.Type_ != conf.GDICT {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		htObj = data.CreateObject(conf.GDICT, data.DictCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key, htObj)
	}
	ht := htObj.Val_.(*data.Dict)
	ht.Set(c.args[2], c.args[3])
	server.DB.Data.Set(key, htObj)
	server.DB.Expire.Delete(key)
	c.AddReplyStr("(integer) 1\r\n")
	return true, nil
}

func hgetCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	htObj := findKeyRead(key)
	if htObj != nil {
		if htObj.Type_ != conf.GDICT {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("(nil)\r\n")
		return false, errs.KeyNotExistError
	}
	ht := htObj.Val_.(*data.Dict)
	val := ht.Get(c.args[2])
	if val != nil {
		if val.Type_ != conf.GSTR {
			c.AddReplyStr("-ERR: wrong type\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("(nil)\r\n")
		return false, errs.FieldNotExistError
	}
	str := val.StrVal()
	c.AddReplyStr(fmt.Sprintf("$%d%v\r\n", len(str), str))
	return true, nil
}

func hgetallCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	htObj := findKeyRead(key)
	if htObj != nil {
		if htObj.Type_ != conf.GDICT {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("(nil)\r\n")
		return false, errs.KeyNotExistError
	}
	ht := htObj.Val_.(*data.Dict)
	objs := ht.IterateDict()
	reply := ""
	for i := range objs {
		reply += fmt.Sprintf("%d) $%d%v\r\n%d) $%d%v\r\n", 2*i+1, len(objs[i][0].StrVal()), objs[i][0].StrVal(), 2*i+2, len(objs[i][1].StrVal()), objs[i][1].StrVal())
	}
	c.AddReplyStr(reply)
	return true, nil
}

func hexistsCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	htObj := findKeyRead(key)
	if htObj != nil {
		if htObj.Type_ != conf.GDICT {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("(integer) 0\r\n")
		return false, errs.KeyNotExistError
	}
	ht := htObj.Val_.(*data.Dict)
	val := ht.Get(c.args[2])
	if val != nil {
		if val.Type_ != conf.GSTR {
			c.AddReplyStr("-ERR: wrong type\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("(integer) 0\r\n")
		return false, errs.FieldNotExistError
	}
	c.AddReplyStr("(integer) 1\r\n")
	return true, nil
}
func hdelCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	htObj := server.DB.Data.Get(key)
	if htObj != nil {
		if htObj.Type_ != conf.GDICT {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("(integer) 0\r\n")
		return false, errs.KeyNotExistError
	}
	ht := htObj.Val_.(*data.Dict)
	err := ht.Delete(c.args[2])
	if err != nil {
		c.AddReplyStr("(integer) 0\r\n")
		return false, errs.DelFieldError
	}
	c.AddReplyStr("(integer) 1\r\n")
	return true, nil
}

func saddCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	setObj := server.DB.Data.Get(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		setObj = data.CreateObject(conf.GSET, data.SetCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key, setObj)
	}
	set := setObj.Val_.(*data.Set)
	err := set.SAdd(c.args[2])
	server.DB.Data.Set(key, setObj)
	server.DB.Expire.Delete(key)
	if err != nil {
		c.AddReplyStr("(integer) 0\r\n")
		return false, err
	}
	c.AddReplyStr("(integer) 1\r\n")
	return true, nil
}

func smembersCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	setObj := findKeyRead(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("(integer) 0\r\n")
		return false, errs.KeyNotExistError
	}
	set := setObj.Val_.(*data.Set)
	members := set.Dict.IterateDict()
	reply := ""
	for i := range members {
		reply += fmt.Sprintf("%d) $%d%v\r\n", i+1, len(members[i][0].StrVal()), members[i][0].StrVal())
	}
	c.AddReplyStr(reply)
	return true, nil
}

func scardCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	setObj := findKeyRead(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("(integer) 0\r\n")
		return false, errs.KeyNotExistError
	}
	set := setObj.Val_.(*data.Set)

	c.AddReplyStr(fmt.Sprintf("(integer) %d\r\n", set.Len))
	return true, nil
}
func sismemberCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	setObj := findKeyRead(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("(integer) 0\r\n")
		return false, errs.KeyNotExistError
	}
	set := setObj.Val_.(*data.Set)
	target := c.args[2]
	member := set.Dict.Find(target)
	if member == nil {
		c.AddReplyStr("(integer) 0\r\n")
		return true, nil
	}
	c.AddReplyStr("(integer) 1\r\n")
	return true, nil
}
func srandmemberCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	setObj := server.DB.Data.Get(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		setObj = data.CreateObject(conf.GSET, data.SetCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key, setObj)
	}
	set := setObj.Val_.(*data.Set)
	member := set.Dict.RandomGet()
	if member == nil {
		c.AddReplyStr("(nil)\r\n")
		return false, nil
	}
	c.AddReplyStr(fmt.Sprintf("$%d%v\r\n", len(member.Key.StrVal()), member.Key.StrVal()))
	return true, nil
}

func sremCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	setObj := server.DB.Data.Get(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		setObj = data.CreateObject(conf.GSET, data.SetCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key, setObj)
	}
	set := setObj.Val_.(*data.Set)
	member := c.args[2]
	err := set.Dict.Delete(member)
	if err != nil {
		c.AddReplyStr("(integer) 0\r\n")
		return false, nil
	}
	c.AddReplyStr("(integer) 1\r\n")
	return true, nil
}

func sinterCommand(c *GodisClient) (bool, error) {
	key1, key2 := c.args[1], c.args[2]
	setObj1 := findKeyRead(key1)
	if setObj1 != nil {
		if setObj1.Type_ != conf.GSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		setObj1 = data.CreateObject(conf.GSET, data.SetCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key1, setObj1)
	}
	set1 := setObj1.Val_.(*data.Set)

	setObj2 := findKeyRead(key2)
	if setObj2 != nil {
		if setObj2.Type_ != conf.GSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		setObj2 = data.CreateObject(conf.GSET, data.SetCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key2, setObj2)
	}
	set2 := setObj2.Val_.(*data.Set)

	inter := set1.SInter(set2)
	if len(inter) == 0 {
		c.AddReplyStr("(empty array)\r\n")
		return true, nil
	}
	reply := ""
	for i := range inter {
		reply += fmt.Sprintf("%d) $%d%v\r\n", i+1, len(inter[i]), inter[i])
	}
	c.AddReplyStr(reply)
	return true, nil
}

func sdiffCommand(c *GodisClient) (bool, error) {
	key1, key2 := c.args[1], c.args[2]
	setObj1 := findKeyRead(key1)
	if setObj1 != nil {
		if setObj1.Type_ != conf.GSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		setObj1 = data.CreateObject(conf.GSET, data.SetCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key1, setObj1)
	}
	set1 := setObj1.Val_.(*data.Set)

	setObj2 := findKeyRead(key2)
	if setObj2 != nil {
		if setObj2.Type_ != conf.GSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		setObj2 = data.CreateObject(conf.GSET, data.SetCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key2, setObj2)
	}
	set2 := setObj2.Val_.(*data.Set)

	diff := set1.SDiff(set2)
	if len(diff) == 0 {
		c.AddReplyStr("(empty array)\r\n")
		return true, nil
	}
	reply := ""
	for i := range diff {
		reply += fmt.Sprintf("%d) $%d%v\r\n", i+1, len(diff[i]), diff[i])
	}
	c.AddReplyStr(reply)
	return true, nil
}

func sunionCommand(c *GodisClient) (bool, error) {
	key1, key2 := c.args[1], c.args[2]
	setObj1 := findKeyRead(key1)
	if setObj1 != nil {
		if setObj1.Type_ != conf.GSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		setObj1 = data.CreateObject(conf.GSET, data.SetCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key1, setObj1)
	}
	set1 := setObj1.Val_.(*data.Set)

	setObj2 := findKeyRead(key2)
	if setObj2 != nil {
		if setObj2.Type_ != conf.GSET {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		setObj2 = data.CreateObject(conf.GSET, data.SetCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key2, setObj2)
	}
	set2 := setObj2.Val_.(*data.Set)

	union := set1.SUnion(set2)
	if len(union) == 0 {
		c.AddReplyStr("(empty array)\r\n")
		return true, nil
	}
	reply := ""
	for i := range union {
		reply += fmt.Sprintf("%d) $%d%v\r\n", i+1, len(union[i]), union[i])
	}
	c.AddReplyStr(reply)
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

func setbitCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	bitObj := server.DB.Data.Get(key)
	if bitObj != nil {
		if bitObj.Type_ != conf.GBIT {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		bitObj = data.CreateObject(conf.GBIT, data.BitmapCreate())
		server.DB.Data.Set(key, bitObj)
	}
	bit := bitObj.Val_.(*data.Bitmap)
	offset, value := c.args[2].StrVal(), c.args[3].StrVal()
	err := bit.SetBit(offset, value)
	if err != nil {
		c.AddReplyStr(fmt.Sprintf("-ERR:%v\r\n", err))
		return false, err
	}
	server.DB.Data.Set(key, bitObj)
	server.DB.Expire.Delete(key)
	c.AddReplyStr(fmt.Sprintf("(integer) %s\r\n", value))
	return true, nil
}

func getbitCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	bitObj := findKeyRead(key)
	if bitObj != nil {
		if bitObj.Type_ != conf.GBIT {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		bitObj = data.CreateObject(conf.GBIT, data.BitmapCreate())
		server.DB.Data.Set(key, bitObj)
	}
	bit := bitObj.Val_.(*data.Bitmap)
	offset := c.args[2].StrVal()
	b, err := bit.GetBit(offset)
	if err != nil {
		c.AddReplyStr(fmt.Sprintf("-ERR:%v\r\n", err))
		return false, err
	}
	c.AddReplyStr(fmt.Sprintf("(integer) %s\r\n", string(b)))
	return true, nil
}

func bitcountCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	bitObj := findKeyRead(key)
	if bitObj != nil {
		if bitObj.Type_ != conf.GBIT {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		bitObj = data.CreateObject(conf.GBIT, data.BitmapCreate())
		server.DB.Data.Set(key, bitObj)
	}
	bit := bitObj.Val_.(*data.Bitmap)
	count := bit.BitCount()
	c.AddReplyStr(fmt.Sprintf("(integer) %d\r\n", count))
	return true, nil
}

func bitopCommand(c *GodisClient) (bool, error) {
	op := c.args[1].StrVal()
	key1, key2 := c.args[2], c.args[3]
	bitObj1 := findKeyRead(key1)
	if bitObj1 != nil {
		if bitObj1.Type_ != conf.GBIT {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		bitObj1 = data.CreateObject(conf.GBIT, data.BitmapCreate())
		server.DB.Data.Set(key1, bitObj1)
	}
	bitObj2 := findKeyRead(key2)
	if bitObj2 != nil {
		if bitObj2.Type_ != conf.GBIT {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		bitObj2 = data.CreateObject(conf.GBIT, data.BitmapCreate())
		server.DB.Data.Set(key2, bitObj2)
	}

	bit1, bit2 := bitObj1.Val_.(*data.Bitmap), bitObj2.Val_.(*data.Bitmap)

	res, err := bit1.BitOp(bit2, op)
	if err != nil {
		c.AddReplyStr(fmt.Sprintf("-ERR:%v\r\n", err))
		return false, err
	}
	c.AddReplyStr(fmt.Sprintf("%v\r\n", res))
	return true, nil
}
func bitposCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	bitObj := findKeyRead(key)
	if bitObj != nil {
		if bitObj.Type_ != conf.GBIT {
			c.AddReplyStr("-ERR:WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		bitObj = data.CreateObject(conf.GBIT, data.BitmapCreate())
		server.DB.Data.Set(key, bitObj)
	}
	bit := bitObj.Val_.(*data.Bitmap)
	offset, err := bit.BitPos(c.args[2].StrVal())
	if err != nil {
		if err == errs.BitNotFoundError {
			c.AddReplyStr("(integer) -1\r\n")
			return false, nil
		} else {
			c.AddReplyStr(fmt.Sprintf("-ERR:%v\r\n", err))
			return false, err
		}
	}
	c.AddReplyStr(fmt.Sprintf("(integer) %d\r\n", offset))
	return true, nil
}
