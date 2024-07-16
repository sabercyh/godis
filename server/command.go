package server

import (
	"fmt"
	"strconv"
	"strings"

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
	// system
	"ping": NewGodisCommand("ping", pingCommand, 1, false),
	// string
	"set":    NewGodisCommand("set", setCommand, 3, true),
	"setnx":  NewGodisCommand("setnx", setnxCommand, 3, true),
	"get":    NewGodisCommand("get", getCommand, 2, false),
	"del":    NewGodisCommand("del", delCommand, 2, true),
	"exists": NewGodisCommand("exists", existsCommand, 2, false),
	"incr":   NewGodisCommand("incr", incrCommand, 2, false),
	"expire": NewGodisCommand("expire", expireCommand, 3, true),
	// list
	"lpush":  NewGodisCommand("lpush", lpushCommand, 3, true),
	"lpop":   NewGodisCommand("lpop", lpopCommand, 2, true),
	"rpush":  NewGodisCommand("rpush", rpushCommand, 3, true),
	"rpop":   NewGodisCommand("rpop", rpopCommand, 2, true),
	"lset":   NewGodisCommand("lset", lsetCommand, 4, true),
	"lrem":   NewGodisCommand("lrem", lremCommand, 3, true),
	"llen":   NewGodisCommand("llen", llenCommand, 2, false),
	"lindex": NewGodisCommand("lindex", lindexCommand, 3, false),
	"lrange": NewGodisCommand("lrange", lrangeCommand, 4, false),
	// hash
	"hset":    NewGodisCommand("hset", hsetCommand, 4, true),
	"hget":    NewGodisCommand("hget", hgetCommand, 3, false),
	"hdel":    NewGodisCommand("hdel", hdelCommand, 3, true),
	"hexists": NewGodisCommand("hexists", hexistsCommand, 3, false),
	"hgetall": NewGodisCommand("hgetall", hgetallCommand, 2, false),
	// set
	"sadd":        NewGodisCommand("sadd", saddCommand, 3, true),
	"scard":       NewGodisCommand("scard", scardCommand, 2, false),
	"sismember":   NewGodisCommand("sismember", sismemberCommand, 3, false),
	"smembers":    NewGodisCommand("smembers", smembersCommand, 2, false),
	"srandmember": NewGodisCommand("srandmember", srandmemberCommand, 2, false),
	"srem":        NewGodisCommand("srem", sremCommand, 3, true),
	"spop":        NewGodisCommand("spop", spopCommand, 2, true),
	"sinter":      NewGodisCommand("sinter", sinterCommand, 3, false),
	"sdiff":       NewGodisCommand("sdiff", sdiffCommand, 3, false),
	"sunion":      NewGodisCommand("sunion", sunionCommand, 3, false),
	// zset
	"zadd":   NewGodisCommand("zadd", zaddCommand, 4, true),
	"zcard":  NewGodisCommand("zcard", zcardCommand, 2, false),
	"zscore": NewGodisCommand("zscore", zscoreCommand, 3, false),
	"zrange": NewGodisCommand("zrange", zrangeCommand, 4, false), // ZRANGE key start stop
	"zrank":  NewGodisCommand("zrank", zrankCommand, 3, false),   // ZRANK key member
	"zrem":   NewGodisCommand("zrem", zremCommand, 3, true),      // ZREM key member
	"zcount": NewGodisCommand("zcount", zcountCommand, 4, false), // ZCOUNT key min max
	// bitmap
	"setbit":   NewGodisCommand("setbit", setbitCommand, 4, true),
	"getbit":   NewGodisCommand("getbit", getbitCommand, 3, false),
	"bitcount": NewGodisCommand("bitcount", bitcountCommand, 2, false),
	"bitop":    NewGodisCommand("bitop", bitopCommand, 4, false),
	"bitpos":   NewGodisCommand("bitpos", bitposCommand, 3, false),

	"slowlog": NewGodisCommand("slowlog", slowlogCommand, 2, false),
	"save":    NewGodisCommand("save", saveCommand, 1, false),
	"bgsave":  NewGodisCommand("bgsave", bgsaveCommand, 1, false),
}

func expireIfNeeded(key *data.Gobj) {
	entry := server.DB.Expire.Find(key)
	if entry == nil {
		return
	}
	when, err := entry.Val.Int64Val()
	if err != nil {
		return
	}
	if when > util.GetTime() {
		return
	}
	server.DB.Expire.Delete(key)
	server.DB.Data.Delete(key)
}

func findKeyRead(key *data.Gobj) *data.Gobj {
	expireIfNeeded(key)
	return server.DB.Data.Get(key)
}

func pingCommand(c *GodisClient) (bool, error) {
	c.AddReplyStr("+PONG\r\n")
	return true, nil
}
func setCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := c.args[2]
	if val.Type_ != conf.GSTR {
		c.AddReplyStr("-ERR wrong type\r\n")
		return false, errs.TypeCheckError
	}
	server.DB.Data.Set(key, val)
	server.DB.Expire.Delete(key)
	c.AddReplyStr("+OK\r\n")
	return true, nil
}

func getCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := findKeyRead(key)
	if val == nil {
		c.AddReplyStr("$-1\r\n")
		return false, errs.KeyNotExistError
	} else if val.Type_ != conf.GSTR {
		c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
		return false, errs.TypeCheckError
	} else {
		str := val.StrVal()
		c.AddReplyStr(fmt.Sprintf("$%d\r\n%v\r\n", len(str), str))
	}
	return true, nil

}

func delCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	err := server.DB.Data.Delete(key)
	if err != nil {
		c.AddReplyStr(":0\r\n")
		return false, errs.DelKeyError
	}
	c.AddReplyStr(":1\r\n")
	return true, nil

}

func incrCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	rawVal := server.DB.Data.Get(key)
	if rawVal != nil {
		if rawVal.Type_ != conf.GSTR {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
		num, err := rawVal.IntVal()
		num++
		if err != nil {
			c.AddReplyStr("-ERR value is not an integer or out of range\r\n")
			return false, errs.TypeCheckError
		}
		rawVal.Val_ = strconv.Itoa(num)
		c.AddReplyStr(fmt.Sprintf(":%d\r\n", num))

	} else {
		num := "1"
		server.DB.Data.Set(key, data.CreateObject(conf.GSTR, num))
		c.AddReplyStr(":1\r\n")
	}
	return true, nil

}

func setnxCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := c.args[2]
	if val.Type_ != conf.GSTR {
		c.AddReplyStr("-ERR wrong type\r\n")
		return false, errs.TypeCheckError
	}
	err := server.DB.Data.SetNx(key, val)
	if err != nil {
		c.AddReplyStr(":0\r\n")
		return false, errs.KeyExistsError
	}
	c.AddReplyStr(":1\r\n")
	return true, nil
}
func existsCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := findKeyRead(key)
	if val == nil {
		c.AddReplyStr(":0\r\n")
		return false, errs.KeyNotExistError
	}
	c.AddReplyStr(":1\r\n")
	return true, nil
}
func expireCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := c.args[2]
	if val.Type_ != conf.GSTR {
		c.AddReplyStr("-ERR value is not an integer or out of range\r\n")
		return false, errs.TypeCheckError
	}
	seconds, err := val.Int64Val()
	if err != nil {
		return false, err
	}
	expire := util.GetTime() + seconds
	expObj := data.CreateObjectFromInt(expire)
	server.DB.Expire.Set(key, expObj)
	c.AddReplyStr(":1\r\n")
	return true, nil
}

func lpushCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	listObj := server.DB.Data.Get(key)
	if listObj != nil {
		if listObj.Type_ != conf.GLIST {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		listObj = data.CreateObject(conf.GLIST, data.ListCreate(data.ListType{EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key, listObj)

	}
	list := listObj.Val_.(*data.List)
	list.LPush(c.args[2])
	c.AddReplyStr(fmt.Sprintf(":%d\r\n", list.Length()))
	return true, nil
}
func lpopCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	listObj := server.DB.Data.Get(key)
	if listObj != nil {
		if listObj.Type_ != conf.GLIST {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("$-1\r\n")
		return false, nil
	}
	list := listObj.Val_.(*data.List)
	nodeVal := list.LPop()
	if nodeVal == nil {
		c.AddReplyStr("$-1\r\n")
		return false, nil
	}
	c.AddReplyStr(fmt.Sprintf("$%d\r\n%s\r\n", len(nodeVal.StrVal()), nodeVal.StrVal()))
	return true, nil
}
func rpushCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	listObj := server.DB.Data.Get(key)
	if listObj != nil {
		if listObj.Type_ != conf.GLIST {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		listObj = data.CreateObject(conf.GLIST, data.ListCreate(data.ListType{EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key, listObj)

	}
	list := listObj.Val_.(*data.List)
	list.RPush(c.args[2])
	c.AddReplyStr(fmt.Sprintf(":%d\r\n", list.Length()))
	return true, nil
}

func rpopCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	listObj := server.DB.Data.Get(key)
	if listObj != nil {
		if listObj.Type_ != conf.GLIST {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("$-1\r\n")
		return false, nil
	}
	list := listObj.Val_.(*data.List)
	nodeVal := list.RPop()
	if nodeVal == nil {
		c.AddReplyStr("$-1\r\n")
		return false, nil
	}
	c.AddReplyStr(fmt.Sprintf("$%d\r\n%s\r\n", len(nodeVal.StrVal()), nodeVal.StrVal()))
	return true, nil
}

func llenCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	listObj := server.DB.Data.Get(key)
	if listObj != nil {
		if listObj.Type_ != conf.GLIST {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr(":0\r\n")
		return true, nil
	}
	list := listObj.Val_.(*data.List)
	c.AddReplyStr(fmt.Sprintf(":%d\r\n", list.Length()))
	return true, nil
}

func lindexCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	listObj := server.DB.Data.Get(key)
	if listObj != nil {
		if listObj.Type_ != conf.GLIST {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("$-1\r\n")
		return true, nil
	}
	list := listObj.Val_.(*data.List)
	index, err := c.args[2].IntVal()
	if err != nil {
		c.AddReplyStr("-ERR value is not an integer or out of range\r\n")
		return false, errs.TypeCheckError
	}
	node := list.Index(int(index))
	if node == nil {
		c.AddReplyStr("$-1\r\n")
		return true, nil
	}
	c.AddReplyStr(fmt.Sprintf("$%d\r\n%s\r\n", len(node.Val.StrVal()), node.Val.StrVal()))
	return true, nil
}

func lsetCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	listObj := server.DB.Data.Get(key)
	if listObj != nil {
		if listObj.Type_ != conf.GLIST {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		listObj = data.CreateObject(conf.GLIST, data.ListCreate(data.ListType{EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key, listObj)
	}
	list := listObj.Val_.(*data.List)

	index, err := c.args[2].IntVal()
	if err != nil {
		c.AddReplyStr("-ERR value is not an integer or out of range\r\n")
		return false, errs.TypeCheckError
	}

	err = list.Set(int(index), c.args[3])
	if err != nil {
		c.AddReplyStr("-ERR index out of range\r\n")
		return false, nil
	}
	c.AddReplyStr("+OK\r\n")
	return true, nil
}

func lremCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	listObj := server.DB.Data.Get(key)
	if listObj != nil {
		if listObj.Type_ != conf.GLIST {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr(":0\r\n")
		return false, nil
	}
	list := listObj.Val_.(*data.List)
	err := list.Rem(c.args[2])
	if err != nil {
		c.AddReplyStr(":0\r\n")
		return false, nil
	}
	c.AddReplyStr(":1\r\n")
	return true, nil
}
func lrangeCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	listObj := server.DB.Data.Get(key)
	if listObj != nil {
		if listObj.Type_ != conf.GLIST {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("*0\r\n")
		return true, nil
	}
	list := listObj.Val_.(*data.List)
	left, err := c.args[2].IntVal()
	if err != nil {
		c.AddReplyStr("-ERR value is not an integer or out of range\r\n")
		return false, errs.TypeCheckError
	}
	right, err := c.args[3].IntVal()
	if err != nil {
		c.AddReplyStr("-ERR value is not an integer or out of range\r\n")
		return false, errs.TypeCheckError
	}
	Gobjs := list.Range(left, right)
	if len(Gobjs) == 0 {
		c.AddReplyStr("*0\r\n")
		return true, nil
	}
	reply := "*" + strconv.Itoa(len(Gobjs)) + "\r\n"
	for i := range Gobjs {
		reply += fmt.Sprintf("$%d\r\n%s\r\n", len(Gobjs[i].StrVal()), Gobjs[i].StrVal())
	}
	c.AddReplyStr(reply)
	return true, nil
}
func hsetCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	htObj := server.DB.Data.Get(key)
	if htObj != nil {
		if htObj.Type_ != conf.GDICT {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		htObj = data.CreateObject(conf.GDICT, data.DictCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key, htObj)

	}
	ht := htObj.Val_.(*data.Dict)
	ht.Set(c.args[2], c.args[3])
	c.AddReplyStr(":1\r\n")
	return true, nil
}

func hgetCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	htObj := findKeyRead(key)
	if htObj != nil {
		if htObj.Type_ != conf.GDICT {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("$-1\r\n")
		return false, errs.KeyNotExistError
	}
	ht := htObj.Val_.(*data.Dict)
	val := ht.Get(c.args[2])
	if val != nil {
		if val.Type_ != conf.GSTR {
			c.AddReplyStr("-ERR wrong type\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("$-1\r\n")
		return false, errs.FieldNotExistError
	}
	str := val.StrVal()
	c.AddReplyStr(fmt.Sprintf("$%d\r\n%v\r\n", len(str), str))
	return true, nil
}

func hgetallCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	htObj := findKeyRead(key)
	if htObj != nil {
		if htObj.Type_ != conf.GDICT {
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("*0\r\n")
		return false, errs.KeyNotExistError
	}
	ht := htObj.Val_.(*data.Dict)
	objs := ht.IterateDict()
	reply := fmt.Sprintf("*%d\r\n", len(objs))
	for i := range objs {
		reply += fmt.Sprintf("$%d\r\n%v\r\n$%d\r\n%v\r\n", len(objs[i][0].StrVal()), objs[i][0].StrVal(), len(objs[i][1].StrVal()), objs[i][1].StrVal())
	}
	c.AddReplyStr(reply)
	return true, nil
}

func hexistsCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	htObj := findKeyRead(key)
	if htObj != nil {
		if htObj.Type_ != conf.GDICT {
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr(":0\r\n")
		return false, errs.KeyNotExistError
	}
	ht := htObj.Val_.(*data.Dict)
	val := ht.Get(c.args[2])
	if val != nil {
		if val.Type_ != conf.GSTR {
			c.AddReplyStr("-ERR wrong type\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr(":0\r\n")
		return false, errs.FieldNotExistError
	}
	c.AddReplyStr(":1\r\n")
	return true, nil
}
func hdelCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	htObj := server.DB.Data.Get(key)
	if htObj != nil {
		if htObj.Type_ != conf.GDICT {
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr(":0\r\n")
		return false, errs.KeyNotExistError
	}
	ht := htObj.Val_.(*data.Dict)
	err := ht.Delete(c.args[2])
	if err != nil {
		c.AddReplyStr(":0\r\n")
		return false, errs.DelFieldError
	}
	c.AddReplyStr(":1\r\n")
	return true, nil
}

func saddCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	setObj := server.DB.Data.Get(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		setObj = data.CreateObject(conf.GSET, data.SetCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key, setObj)
	}
	set := setObj.Val_.(*data.Set)
	err := set.SAdd(c.args[2])
	if err != nil {
		c.AddReplyStr(":0\r\n")
		return false, err
	}
	c.AddReplyStr(":1\r\n")
	return true, nil
}

func smembersCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	setObj := findKeyRead(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr(":0\r\n")
		return false, errs.KeyNotExistError
	}
	set := setObj.Val_.(*data.Set)
	members := set.Dict.IterateDict()
	reply := fmt.Sprintf("*%d\r\n", len(members))
	for i := range members {
		reply += fmt.Sprintf("$%d\r\n%v\r\n", len(members[i][0].StrVal()), members[i][0].StrVal())
	}
	c.AddReplyStr(reply)
	return true, nil
}

func scardCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	setObj := findKeyRead(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr(":0\r\n")
		return false, errs.KeyNotExistError
	}
	set := setObj.Val_.(*data.Set)

	c.AddReplyStr(fmt.Sprintf(":%d\r\n", set.Length()))
	return true, nil
}
func sismemberCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	setObj := findKeyRead(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr(":0\r\n")
		return false, errs.KeyNotExistError
	}
	set := setObj.Val_.(*data.Set)
	target := c.args[2]
	member := set.Dict.Find(target)
	if member == nil {
		c.AddReplyStr(":0\r\n")
		return true, nil
	}
	c.AddReplyStr(":1\r\n")
	return true, nil
}
func srandmemberCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	setObj := server.DB.Data.Get(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("$-1\r\n")
		return false, nil
	}
	set := setObj.Val_.(*data.Set)
	member := set.Dict.RandomGet()
	if member == nil {
		c.AddReplyStr("$-1\r\n")
		return false, nil
	}
	c.AddReplyStr(fmt.Sprintf("$%d\r\n%v\r\n", len(member.Key.StrVal()), member.Key.StrVal()))
	return true, nil
}

func sremCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	setObj := server.DB.Data.Get(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("$:0\r\n")
		return false, nil
	}
	set := setObj.Val_.(*data.Set)
	member := c.args[2]
	err := set.SDel(member)
	if err != nil {
		c.AddReplyStr(":0\r\n")
		return false, nil
	}
	c.AddReplyStr(":1\r\n")
	return true, nil
}

func spopCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	setObj := server.DB.Data.Get(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("$-1\r\n")
		return false, nil
	}
	set := setObj.Val_.(*data.Set)
	setVal := set.Pop()
	if setVal == "" {
		c.AddReplyStr("$-1\r\n")
		return false, nil
	}
	c.AddReplyStr(fmt.Sprintf("$%d\r\n%s\r\n", len(setVal), setVal))
	return true, nil
}

func sinterCommand(c *GodisClient) (bool, error) {
	key1, key2 := c.args[1], c.args[2]
	setObj1 := findKeyRead(key1)
	if setObj1 != nil {
		if setObj1.Type_ != conf.GSET {
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
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
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		setObj2 = data.CreateObject(conf.GSET, data.SetCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual}))
		server.DB.Data.Set(key2, setObj2)
	}
	set2 := setObj2.Val_.(*data.Set)

	inter := set1.SInter(set2)
	if len(inter) == 0 {
		c.AddReplyStr("*0\r\n")
		return true, nil
	}

	reply := fmt.Sprintf("*%d\r\n", len(inter))
	for i := range inter {
		reply += fmt.Sprintf("$%d\r\n%v\r\n", len(inter[i]), inter[i])
	}
	c.AddReplyStr(reply)
	return true, nil
}

func sdiffCommand(c *GodisClient) (bool, error) {
	key1, key2 := c.args[1], c.args[2]
	setObj1 := findKeyRead(key1)
	if setObj1 != nil {
		if setObj1.Type_ != conf.GSET {
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
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
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
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

	reply := fmt.Sprintf("*%d\r\n", len(diff))
	for i := range diff {
		reply += fmt.Sprintf("$%d\r\n%v\r\n", len(diff[i]), diff[i])
	}
	c.AddReplyStr(reply)
	return true, nil
}

func sunionCommand(c *GodisClient) (bool, error) {
	key1, key2 := c.args[1], c.args[2]
	setObj1 := findKeyRead(key1)
	if setObj1 != nil {
		if setObj1.Type_ != conf.GSET {
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
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
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
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
	reply := fmt.Sprintf("*%d\r\n", len(union))
	for i := range union {
		reply += fmt.Sprintf("$%d\r\n%v\r\n", len(union[i]), union[i])
	}
	c.AddReplyStr(reply)
	return true, nil
}
func zaddCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj != nil {
		if zsObj.Type_ != conf.GZSET {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
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
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
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
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
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
	c.AddReplyStr(fmt.Sprintf(":%s\r\n", string(b)))
	return true, nil
}

func bitcountCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	bitObj := findKeyRead(key)
	if bitObj != nil {
		if bitObj.Type_ != conf.GBIT {
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		bitObj = data.CreateObject(conf.GBIT, data.BitmapCreate())
		server.DB.Data.Set(key, bitObj)
	}
	bit := bitObj.Val_.(*data.Bitmap)
	count := bit.BitCount()
	c.AddReplyStr(fmt.Sprintf(":%d\r\n", count))
	return true, nil
}

func bitopCommand(c *GodisClient) (bool, error) {
	op := c.args[1].StrVal()
	key1, key2 := c.args[2], c.args[3]
	bitObj1 := findKeyRead(key1)
	if bitObj1 != nil {
		if bitObj1.Type_ != conf.GBIT {
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		bitObj1 = data.CreateObject(conf.GBIT, data.BitmapCreate())
		server.DB.Data.Set(key1, bitObj1)
	}
	bitObj2 := findKeyRead(key2)
	if bitObj2 != nil {
		if bitObj2.Type_ != conf.GBIT {
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
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
			c.AddReplyStr("-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
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
			c.AddReplyStr("$-1\r\n")
			return false, nil
		} else {
			c.AddReplyStr(fmt.Sprintf("-ERR:%v\r\n", err))
			return false, err
		}
	}
	c.AddReplyStr(fmt.Sprintf(":%d\r\n", offset))
	return true, nil
}

func slowlogCommand(c *GodisClient) (bool, error) {
	op := c.args[1]
	subcommand := strings.ToLower(op.StrVal())
	switch subcommand {
	case "get":
		if server.Slowlog.Length() == 0 {
			c.AddReplyStr("*0\r\n")
			return false, nil
		}
		slowLogEntrys := server.Slowlog.Range(0, server.Slowlog.Length())
		reply := ""
		for i := 0; i < len(slowLogEntrys); i++ {
			entry := slowLogEntrys[i]
			if entry.Type_ != conf.GSLOWLOG {
				c.AddReplyStr("-ERR wrong type\r\n")
				return false, errs.TypeCheckError
			}
			slowLogEntry := entry.Val_.(*SlowLogEntry)
			reply += fmt.Sprintf("%d) 1) (integer) %d\r\n   2) (integer) %d\r\n   3) (integer) %d\r\n   4) 1)%s\r\n", i+1, slowLogEntry.id, slowLogEntry.time, slowLogEntry.duration, slowLogEntry.robj[0].StrVal())
			for j := 1; j < slowLogEntry.argc; j++ {
				reply += fmt.Sprintf("      %d) %s\r\n", j+1, slowLogEntry.robj[j].StrVal())
			}
		}
		c.AddReplyStr(reply)
	case "len":
		c.AddReplyStr(fmt.Sprintf(":%d\r\n", server.Slowlog.Length()))
	case "reset":
		server.Slowlog.Clear()
		c.AddReplyStr("+OK\r\n")
	default:
		c.AddReplyStr(fmt.Sprintf("-ERR unknown subcommand '%s'.\r\n", subcommand))
		return false, errs.WrongCmdError
	}

	return true, nil
}

func saveCommand(c *GodisClient) (bool, error) {
	if server.RDB.IsRDBSave() {
		c.AddReplyStr("-ERR Background save already in progress\r\n")
		return false, errs.RDBIsSavingError
	}
	if err := server.RDB.Save(server.DB); err != nil {
		c.AddReplyStr("-ERR Failed to save rdb file\r\n")
		return false, err
	} else {
		c.AddReplyStr("+OK\r\n")
	}
	return true, nil
}

func bgsaveCommand(c *GodisClient) (bool, error) {
	if server.RDB.IsRDBSave() {
		c.AddReplyStr("-ERR Background save already in progress\r\n")
		return false, errs.RDBIsSavingError
	}
	if err := server.RDB.BgSave(server.DB); err != nil {
		c.AddReplyStr("-ERR Failed to save rdb file\r\n")
		return false, err
	} else {
		c.AddReplyStr("+Background saving started\r\n")
	}
	return true, nil
}
