package persistence

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"syscall"

	"github.com/godis/conf"
	"github.com/godis/data"
	"github.com/godis/db"
	"github.com/godis/errs"
	"github.com/godis/util"
	"github.com/sirupsen/logrus"
)

type RDB struct {
	Buffer         []byte //RDB文件缓冲区
	Filename       string //RDB文件名
	RDBCheckSum    bool
	RDBCompression bool
	log            *logrus.Logger
	isRDBSave      bool
}

func InitRDB(config *conf.Config, logger *logrus.Logger) *RDB {
	return &RDB{
		Buffer:         make([]byte, conf.RDB_BUF_BLOCK_SIZE),
		Filename:       config.DBFilename,
		RDBCheckSum:    config.RDBCheckSum,
		RDBCompression: config.RDBCompression,
		log:            logger,
	}
}

func (rdb *RDB) IsRDBSave() bool {
	return rdb.isRDBSave
}
func (rdb *RDB) Save(db *db.GodisDB) error {
	if rdb.isRDBSave {
		return errs.RDBIsSavingError
	}
	rdb.isRDBSave = true
	defer func() {
		rdb.isRDBSave = false
	}()
	err := rdb.save(db)
	if err != nil {
		rdb.log.Errorf("rdb save failed, err:%v", err)
		return err
	}
	return nil
}

func (rdb *RDB) BgSave(db *db.GodisDB) error {
	if rdb.isRDBSave {
		return errs.RDBIsSavingError
	}
	rdb.isRDBSave = true
	defer func() {
		rdb.isRDBSave = false
	}()

	id, _, _ := syscall.Syscall(syscall.SYS_CLONE, 0, 0, 0)
	if id == 0 {
		err := rdb.save(db)
		if err != nil {
			rdb.log.Errorf("rdb bgsave failed, err:%v", err)
			return err
		}
		os.Exit(0)
	} else if id > 0 {
		rdb.log.Infof("start rdb bgsave")
	} else {
		rdb.log.Errorf("fork failed")
		return errs.ForkError
	}
	return nil
}

func (rdb *RDB) save(db *db.GodisDB) error {
	tempFilename := fmt.Sprintf("temp-%d.rdb", util.GetMsTime())
	tempFile, err := os.Create(tempFilename)
	if err != nil {
		rdb.log.Errorf("create tempfile %s failed, err:%v", tempFilename, err)
		return err
	}
	defer func() {
		tempFile.Close()
		os.Rename(tempFilename, rdb.Filename)
	}()

	buffer := bytes.NewBuffer(make([]byte, conf.RDB_BUF_BLOCK_SIZE))
	buffer.Write([]byte(conf.RDB_APPNAME))
	buffer.Write([]byte(conf.RDB_GODIS_VERSION))

	Gobjs := db.Data.IterateDict()

	for _, obj := range Gobjs {
		key, val := obj[0], obj[1]
		if err := rdb.Persist(db, buffer, key, val); err != nil {
			rdb.log.Errorf("persist key:%s failed, err:%v", key.StrVal(), err)
		}
	}
	buffer.WriteByte(byte(conf.RDB_EOF))
	return nil
}

func (rdb *RDB) Persist(db *db.GodisDB, buffer *bytes.Buffer, key, val *data.Gobj) error {
	switch key.Type_ {
	case conf.GSTR:
		return rdb.PersistString(db, buffer, key, val)
	case conf.GLIST:
		return rdb.PersistList(db, buffer, key, val)
	case conf.GDICT:
		return rdb.PersistDict(db, buffer, key, val)
	case conf.GSET:
		return rdb.PersistSet(db, buffer, key, val)
	case conf.GZSET:
		return rdb.PersistZSet(db, buffer, key, val)
	case conf.GBIT:
		return rdb.PersistBit(db, buffer, key, val)
	default:
		return errs.TypeCheckError
	}
}

func (rdb *RDB) PersistString(db *db.GodisDB, buffer *bytes.Buffer, key, val *data.Gobj) error {
	rdb.checkExpire(db, buffer, key)
	buffer.WriteByte(byte(conf.GSTR))
	rdb.WriteString(buffer, key)
	rdb.WriteString(buffer, val)
	return nil
}

func (rdb *RDB) PersistList(db *db.GodisDB, buffer *bytes.Buffer, key, val *data.Gobj) error {
	rdb.checkExpire(db, buffer, key)
	buffer.WriteByte(byte(conf.GLIST))
	rdb.WriteString(buffer, key)
	list := val.Val_.(*data.List)
	rdb.WriteLen(buffer, list.Length())
	var node *data.Node
	for i := 1; i <= list.Length(); i++ {
		node = list.ForwardIndex(i)
		rdb.WriteString(buffer, node.Val)
	}
	return nil
}

func (rdb *RDB) PersistDict(db *db.GodisDB, buffer *bytes.Buffer, key, val *data.Gobj) error {
	rdb.checkExpire(db, buffer, key)
	buffer.WriteByte(byte(conf.GDICT))
	rdb.WriteString(buffer, key)
	dict := val.Val_.(*data.Dict)
	objs := dict.IterateDict()
	rdb.WriteLen(buffer, len(objs))
	for _, obj := range objs {
		rdb.WriteString(buffer, obj[0])
		rdb.WriteString(buffer, obj[1])
	}

	return nil
}

func (rdb *RDB) PersistSet(db *db.GodisDB, buffer *bytes.Buffer, key, val *data.Gobj) error {
	rdb.checkExpire(db, buffer, key)
	buffer.WriteByte(byte(conf.GSET))
	rdb.WriteString(buffer, key)
	set := val.Val_.(*data.Set)
	rdb.WriteLen(buffer, set.Length())
	for _, member := range set.Dict.IterateDict() {
		rdb.WriteString(buffer, member[1])
	}
	return nil
}

func (rdb *RDB) PersistZSet(db *db.GodisDB, buffer *bytes.Buffer, key, val *data.Gobj) error {
	rdb.checkExpire(db, buffer, key)
	buffer.WriteByte(byte(conf.GZSET))
	rdb.WriteString(buffer, key)
	zset := val.Val_.(*data.ZSet)
	rdb.WriteLen(buffer, int(zset.Zcard()))
	for _, obj := range zset.Dict.IterateDict() {
		member, score := obj[0], obj[1]
		rdb.WriteString(buffer, member)
		rdb.WriteFloat64(buffer, score)
	}
	return nil
}

func (rdb *RDB) PersistBit(db *db.GodisDB, buffer *bytes.Buffer, key, val *data.Gobj) error {
	rdb.checkExpire(db, buffer, key)
	return nil
}
func (rdb *RDB) checkExpire(db *db.GodisDB, buffer *bytes.Buffer, key *data.Gobj) {
	if expireKey := db.Expire.Get(key); expireKey != nil {
		buffer.WriteByte(byte(conf.RDB_Expire))

		expireTime := make([]byte, 8)
		binary.BigEndian.PutUint64(expireTime, uint64(expireKey.IntVal()))
		buffer.Write(expireTime)
	}
}
func (rdb *RDB) WriteString(buffer *bytes.Buffer, obj *data.Gobj) (bool, error) {
	if obj.Type_ == conf.GSTR {
		str := obj.StrVal()
		rdb.WriteLen(buffer, len(str))
		buffer.WriteString(str)
		return true, nil
	}
	return false, nil
}

func (rdb *RDB) WriteFloat64(buffer *bytes.Buffer, obj *data.Gobj) (bool, error) {
	score, ok := obj.Val_.(float64)
	if ok {
		str := strconv.FormatFloat(score, 'f', -1, 64)
		buffer.WriteString(str)
		return true, nil
	}
	return false, nil
}
func (rdb *RDB) WriteLen(buffer *bytes.Buffer, length int) (bool, error) {
	switch {
	case length <= 2<<6-1:
		buffer.WriteByte(byte(length))
	case length <= 2<<14-1:
		buffer.WriteByte(byte(length>>8) | 0x40)
		buffer.WriteByte(byte(length))
	case length <= 2<<32-1:
		buffer.WriteByte(0x80)
		l := make([]byte, 4)
		binary.BigEndian.PutUint32(l, uint32(length))
		buffer.Write(l)
	default:
		return false, errs.OutOfRangeError
	}
	return true, nil
}
