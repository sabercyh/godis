package server

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/godis/conf"
	"github.com/godis/data"
	"github.com/godis/errs"
	"github.com/godis/util"
)

const MULTI_ARGS_COMMAND int = -1

type CommandProc func(c *GodisClient) (bool, error)

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
	"ping":     NewGodisCommand("ping", pingCommand, 1, false),
	"shutdown": NewGodisCommand("shutdown", shutdownCommand, 1, false),
	// string
	"set":    NewGodisCommand("set", setCommand, 3, true),
	"mset":   NewGodisCommand("mset", msetCommand, MULTI_ARGS_COMMAND, true),
	"setnx":  NewGodisCommand("setnx", setnxCommand, 3, true),
	"get":    NewGodisCommand("get", getCommand, 2, false),
	"del":    NewGodisCommand("del", delCommand, MULTI_ARGS_COMMAND, true),
	"exists": NewGodisCommand("exists", existsCommand, MULTI_ARGS_COMMAND, false),
	"incr":   NewGodisCommand("incr", incrCommand, 2, true),
	"expire": NewGodisCommand("expire", expireCommand, 3, true),
	// list
	"lpush":  NewGodisCommand("lpush", lpushCommand, MULTI_ARGS_COMMAND, true),
	"lpop":   NewGodisCommand("lpop", lpopCommand, 2, true),
	"rpush":  NewGodisCommand("rpush", rpushCommand, MULTI_ARGS_COMMAND, true),
	"rpop":   NewGodisCommand("rpop", rpopCommand, 2, true),
	"lset":   NewGodisCommand("lset", lsetCommand, 4, true),
	"lrem":   NewGodisCommand("lrem", lremCommand, 3, true),
	"llen":   NewGodisCommand("llen", llenCommand, 2, false),
	"lindex": NewGodisCommand("lindex", lindexCommand, 3, false),
	"lrange": NewGodisCommand("lrange", lrangeCommand, 4, false),
	// hash
	"hset":    NewGodisCommand("hset", hsetCommand, MULTI_ARGS_COMMAND, true),
	"hget":    NewGodisCommand("hget", hgetCommand, 3, false),
	"hdel":    NewGodisCommand("hdel", hdelCommand, MULTI_ARGS_COMMAND, true),
	"hexists": NewGodisCommand("hexists", hexistsCommand, 3, false),
	"hgetall": NewGodisCommand("hgetall", hgetallCommand, 2, false),
	// set
	"sadd":        NewGodisCommand("sadd", saddCommand, MULTI_ARGS_COMMAND, true),
	"scard":       NewGodisCommand("scard", scardCommand, 2, false),
	"sismember":   NewGodisCommand("sismember", sismemberCommand, 3, false),
	"smembers":    NewGodisCommand("smembers", smembersCommand, 2, false),
	"srandmember": NewGodisCommand("srandmember", srandmemberCommand, 2, false),
	"srem":        NewGodisCommand("srem", sremCommand, MULTI_ARGS_COMMAND, true),
	"spop":        NewGodisCommand("spop", spopCommand, 2, true),
	"sinter":      NewGodisCommand("sinter", sinterCommand, 3, false),
	"sdiff":       NewGodisCommand("sdiff", sdiffCommand, 3, false),
	"sunion":      NewGodisCommand("sunion", sunionCommand, 3, false),
	// zset
	"zadd":    NewGodisCommand("zadd", zaddCommand, MULTI_ARGS_COMMAND, true),
	"zcard":   NewGodisCommand("zcard", zcardCommand, 2, false),
	"zscore":  NewGodisCommand("zscore", zscoreCommand, 3, false),
	"zrange":  NewGodisCommand("zrange", zrangeCommand, 4, false),
	"zrank":   NewGodisCommand("zrank", zrankCommand, 3, false),
	"zrem":    NewGodisCommand("zrem", zremCommand, MULTI_ARGS_COMMAND, true),
	"zcount":  NewGodisCommand("zcount", zcountCommand, 4, false),
	"zpopmin": NewGodisCommand("zpopmin", zpopminCommand, 2, true),
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

func shutdownCommand(c *GodisClient) (bool, error) {
	server.AeLoop.stop = true
	return true, nil
}
func setCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := c.args[2]
	server.DB.Data.Set(key, val)
	val.IncrRefCount()
	server.DB.Expire.Delete(key)
	c.AddReplyStr("+OK\r\n")
	return true, nil
}

func msetCommand(c *GodisClient) (bool, error) {
	if len(c.args) < 3 || len(c.args[1:])%2 != 0 {
		c.AddReplyStr("-ERR wrong number of arguments for 'mset' command\r\n")
		return false, errs.ParamsCheckError
	}
	var key, val *data.Gobj

	for i := 1; i < len(c.args); i += 2 {
		key = c.args[i]
		val = c.args[i+1]
		server.DB.Data.Set(key, val)
		val.IncrRefCount()
		server.DB.Expire.Delete(key)
	}

	c.AddReplyStr("+OK\r\n")
	return true, nil
}

func getCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := findKeyRead(key)
	if val == nil {
		c.AddReplyStr("$-1\r\n")
		return false, errs.KeyNotExistError
	}

	str := val.StrVal()
	if str == "" {
		c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
		return false, errs.TypeCheckError
	}

	c.AddReplyStrVal(str)
	return true, nil

}

func delCommand(c *GodisClient) (bool, error) {
	if len(c.args) < 2 {
		c.AddReplyStr("-ERR wrong number of arguments for 'del' command\r\n")
		return false, errs.ParamsCheckError
	}
	var key *data.Gobj
	count := 0
	for i := range c.args {
		key = c.args[i]
		err := server.DB.Data.Delete(key)
		if err != nil {
			continue
		}
		count++
	}
	c.AddReplyStr(fmt.Sprintf(":%d\r\n", count))
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
		numVal := strconv.Itoa(num)
		rawVal.Val_ = numVal
		c.AddReplyIntVal(numVal)

	} else {
		numVal := "1"
		server.DB.Data.Set(key, data.CreateObject(conf.GSTR, numVal))
		c.AddReplyStr(":1\r\n")
	}
	return true, nil

}

func setnxCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := c.args[2]
	err := server.DB.Data.SetNx(key, val)
	if err != nil {
		c.AddReplyStr(":0\r\n")
		return false, errs.KeyExistsError
	}
	c.AddReplyStr(":1\r\n")
	return true, nil
}
func existsCommand(c *GodisClient) (bool, error) {
	if len(c.args) < 2 {
		c.AddReplyStr("-ERR wrong number of arguments for 'exists' command\r\n")
		return false, errs.ParamsCheckError
	}
	var key *data.Gobj
	count := 0
	for i := range c.args {
		key = c.args[i]
		val := findKeyRead(key)
		if val != nil {
			count++
		}
	}
	c.AddReplyStr(fmt.Sprintf(":%d\r\n", count))
	return true, nil
}
func expireCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	val := c.args[2]
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
	if len(c.args) < 3 {
		c.AddReplyStr("-ERR wrong number of arguments for 'lpush' command\r\n")
		return false, errs.ParamsCheckError
	}

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

	for i := 2; i < len(c.args); i++ {
		list.LPush(c.args[i])
	}
	c.AddReplyIntVal(strconv.Itoa(list.Length()))
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
	c.AddReplyStrVal(nodeVal.StrVal())
	return true, nil
}
func rpushCommand(c *GodisClient) (bool, error) {
	if len(c.args) < 3 {
		c.AddReplyStr("-ERR wrong number of arguments for 'rpush' command\r\n")
		return false, errs.ParamsCheckError
	}

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
	for i := 2; i < len(c.args); i++ {
		list.RPush(c.args[i])
	}
	c.AddReplyIntVal(strconv.Itoa(list.Length()))
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
	c.AddReplyStrVal(nodeVal.StrVal())
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
	reply := list.RangeVal(left, right)
	if len(reply) == 0 {
		c.AddReplyStr("*0\r\n")
		return true, nil
	}

	c.AddReplyBytes(reply)
	return true, nil
}
func hsetCommand(c *GodisClient) (bool, error) {
	if len(c.args[2:])%2 != 0 {
		c.AddReplyStr("-ERR wrong number of arguments for 'hset' command\r\n")
		return false, errs.ParamsCheckError
	}

	var count int
	key := c.args[1]
	htObj := server.DB.Data.Get(key)
	if htObj != nil {
		if htObj.Type_ != conf.GDICT {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		htObj = data.CreateObject(conf.GDICT, data.DictCreate())
		server.DB.Data.Set(key, htObj)

	}
	ht := htObj.Val_.(*data.Dict)

	for i := 2; i < len(c.args); i += 2 {
		count += ht.Set(c.args[i], c.args[i+1])
		c.args[i+1].IncrRefCount()
	}

	c.AddReplyIntVal(strconv.Itoa(count))
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
	reply := fmt.Sprintf("*%d\r\n", len(objs)*2)
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
	var count int

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

	for i := 2; i < len(c.args); i++ {
		err := ht.Delete(c.args[i])
		if err != nil {
			continue
		}
		count++
	}

	c.AddReplyStr(fmt.Sprintf(":%d\r\n", count))
	return true, nil
}

func saddCommand(c *GodisClient) (bool, error) {
	if len(c.args) < 3 {
		c.AddReplyStr("-ERR wrong number of arguments for 'sadd' command\r\n")
		return false, errs.ParamsCheckError
	}

	key := c.args[1]
	setObj := server.DB.Data.Get(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		setObj = data.CreateObject(conf.GSET, data.SetCreate())
		server.DB.Data.Set(key, setObj)
	}
	set := setObj.Val_.(*data.Set)

	for i := 2; i < len(c.args); i++ {
		set.SAdd(c.args[i])
	}
	c.AddReplyIntVal(strconv.Itoa(set.Length()))

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
	if len(c.args) < 3 {
		c.AddReplyStr("-ERR wrong number of arguments for 'srem' command\r\n")
		return false, errs.ParamsCheckError
	}

	var count int

	key := c.args[1]
	setObj := server.DB.Data.Get(key)
	if setObj != nil {
		if setObj.Type_ != conf.GSET {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr(":0\r\n")
		return false, nil
	}
	set := setObj.Val_.(*data.Set)

	for i := 2; i < len(c.args); i++ {
		member := c.args[i]
		err := set.SDel(member)
		if err != nil {
			continue
		}
		count++
	}

	c.AddReplyStr(fmt.Sprintf(":%d\r\n", count))

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
	c.AddReplyStrVal(setVal)
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
		setObj1 = data.CreateObject(conf.GSET, data.SetCreate())
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
		setObj2 = data.CreateObject(conf.GSET, data.SetCreate())
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
		setObj1 = data.CreateObject(conf.GSET, data.SetCreate())
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
		setObj2 = data.CreateObject(conf.GSET, data.SetCreate())
		server.DB.Data.Set(key2, setObj2)
	}
	set2 := setObj2.Val_.(*data.Set)

	diff := set1.SDiff(set2)
	if len(diff) == 0 {
		c.AddReplyStr("*0\r\n")
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
		setObj1 = data.CreateObject(conf.GSET, data.SetCreate())
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
		setObj2 = data.CreateObject(conf.GSET, data.SetCreate())
		server.DB.Data.Set(key2, setObj2)
	}
	set2 := setObj2.Val_.(*data.Set)

	union := set1.SUnion(set2)
	if len(union) == 0 {
		c.AddReplyStr("*0\r\n")
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
	if len(c.args) < 4 || len(c.args[2:])%2 != 0 {
		c.AddReplyStr("-ERR wrong number of arguments for 'zadd' command\r\n")
		return false, errs.ParamsCheckError
	}
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
	newCount, err := zs.Zadd(c.args[2:])
	if err != nil {
		c.AddReplyStr("-ERR value is not a valid float\r\n")
		return false, err
	}

	c.AddReplyIntVal(strconv.Itoa(newCount))

	return true, nil
}

func zcardCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj != nil {
		if zsObj.Type_ != conf.GZSET {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr(":0\r\n")
		return false, errs.KeyNotExistError
	}

	zs := zsObj.Val_.(*data.ZSet)
	c.AddReplyStr(fmt.Sprintf(":%d\r\n", zs.Zcard()))
	return true, nil
}

func zscoreCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj != nil {
		if zsObj.Type_ != conf.GZSET {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("$-1\r\n")
		return false, errs.KeyNotExistError
	}
	zs := zsObj.Val_.(*data.ZSet)
	str := zs.Zscore(c.args[2])
	if str == "" {
		c.AddReplyStr("$-1\r\n")
		return false, errs.KeyNotExistError
	}
	c.AddReplyStr(fmt.Sprintf("$%d\r\n%s\r\n", len(str), str))
	return true, nil
}

// 返回指定范围内的元素
func zrangeCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj != nil {
		if zsObj.Type_ != conf.GZSET {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("*0\r\n")
		return false, errs.KeyNotExistError
	}
	zs := zsObj.Val_.(*data.ZSet)
	if strSlice, err := zs.Zrange(c.args[2], c.args[3]); err != nil {
		if err == errs.OutOfRangeError {
			c.AddReplyStr("*0\r\n")
		} else {
			c.AddReplyStr("-ERR value is not an integer or out of range\r\n")
		}
		return false, err
	} else {
		bytes := bytes.Buffer{}
		bytes.WriteString(fmt.Sprintf("*%d\r\n", len(strSlice)))
		for i := 0; i < len(strSlice); i++ {
			bytes.WriteString(fmt.Sprintf("$%d", len(strSlice[i])))
			bytes.WriteString(fmt.Sprintf("\r\n%s\r\n", strSlice[i]))
		}
		c.AddReplyBytes(bytes.Bytes())
	}
	return true, nil
}

func zremCommand(c *GodisClient) (bool, error) {
	if len(c.args) < 3 {
		c.AddReplyStr("-ERR wrong number of arguments for 'zrem' command\r\n")
		return false, errs.ParamsCheckError
	}
	var count int
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj != nil {
		if zsObj.Type_ != conf.GZSET {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr(":0\r\n")
		return false, errs.KeyNotExistError
	}
	zs := zsObj.Val_.(*data.ZSet)

	for i := 2; i < len(c.args); i++ {
		if n, err := zs.ZREM(c.args[i]); err != nil {
			continue
		} else {
			count += n
		}
	}

	c.AddReplyStr(fmt.Sprintf(":%d\r\n", count))

	return true, nil
}

func zrankCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj != nil {
		if zsObj.Type_ != conf.GZSET {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("$-1\r\n")
		return false, errs.KeyNotExistError
	}
	zs := zsObj.Val_.(*data.ZSet)
	if rank, err := zs.ZRANK(c.args[2]); err != nil {
		c.AddReplyStr("$-1\r\n")
		return false, err
	} else {
		c.AddReplyStr(fmt.Sprintf(":%d\r\n", rank))
	}
	return true, nil
}

func zcountCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj != nil {
		if zsObj.Type_ != conf.GZSET {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr(":0\r\n")
		return false, errs.KeyNotExistError
	}

	number, err := zsObj.Val_.(*data.ZSet).Zcount(c.args[2], c.args[3])
	if err != nil {
		c.AddReplyStr("-ERR value is not an integer or out of range\r\n")
		return false, err
	}

	c.AddReplyStr(fmt.Sprintf(":%d\r\n", number))
	return true, nil
}

func zpopminCommand(c *GodisClient) (bool, error) {
	key := c.args[1]
	zsObj := server.DB.Data.Get(key)
	if zsObj != nil {
		if zsObj.Type_ != conf.GZSET {
			c.AddReplyStr("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
			return false, errs.TypeCheckError
		}
	} else {
		c.AddReplyStr("*0\r\n")
		return false, errs.KeyNotExistError
	}

	zs := zsObj.Val_.(*data.ZSet)
	if zs.Zlen() == 0 {
		c.AddReplyStr("*0\r\n")
		return false, errs.KeyNotExistError
	}

	member, score, err := zs.ZPOPMIN()
	if err != nil {
		c.AddReplyStr("*0\r\n")
		return false, err
	}
	s := strconv.FormatFloat(score, 'f', -1, 64)

	c.AddReplyStrVal(member)
	c.AddReplyStrVal(s)
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
	rawByte, err := bit.SetBit(offset, value)
	if err != nil {
		c.AddReplyStr(fmt.Sprintf("-ERR %v\r\n", err))
		return false, err
	}
	c.AddReplyStr(fmt.Sprintf(":%d\r\n", rawByte))
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
		c.AddReplyStr(":0\r\n")
		return false, nil
	}
	bit := bitObj.Val_.(*data.Bitmap)
	offset := c.args[2].StrVal()
	b, err := bit.GetBit(offset)
	if err != nil {
		c.AddReplyStr(fmt.Sprintf("-ERR %v\r\n", err))
		return false, err
	}
	c.AddReplyStr(fmt.Sprintf(":%d\r\n", b))
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
		c.AddReplyStr(":0\r\n")
		return false, nil
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
	c.AddReplyStr(fmt.Sprintf(":%b\r\n", res))
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
		if c.args[2].StrVal() == "0" {
			c.AddReplyStr(":0\r\n")
		} else {
			c.AddReplyStr(":-1\r\n")
		}
		return false, nil
	}
	bit := bitObj.Val_.(*data.Bitmap)
	offset, err := bit.BitPos(c.args[2].StrVal())
	if err != nil {
		if err == errs.BitNotFoundError {
			c.AddReplyStr(":-1\r\n")
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
		reply := server.Slowlog.RangeSlowlog()

		c.AddReplyBytes(reply)
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
