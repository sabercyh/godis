package persistence

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
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
	CheckSum       uint64
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

	buffer := bytes.NewBuffer(make([]byte, 0, conf.RDB_BUF_BLOCK_SIZE))
	buffer.Write([]byte(conf.RDB_APPNAME))
	buffer.Write([]byte(conf.RDB_VERSION))

	Gobjs := db.Data.IterateDict()

	for _, obj := range Gobjs {
		key, val := obj[0], obj[1]
		if err := rdb.Persist(db, buffer, key, val); err != nil {
			rdb.log.Errorf("persist key:%s failed, err:%v", key.StrVal(), err)
		}
	}

	buffer.WriteByte(byte(conf.RDB_OPCODE_EOF))
	if rdb.RDBCheckSum {
		checksumNum := util.CheckSumCreate(buffer.Bytes())
		rdb.log.Infof("rdb checksum:%d\r\n", checksumNum)
		checksum := make([]byte, 8)
		binary.BigEndian.PutUint64(checksum, checksumNum)
		buffer.Write(checksum)
	}
	tempFile.Write(buffer.Bytes()[:buffer.Len()])
	return nil
}

func (rdb *RDB) Persist(db *db.GodisDB, buffer *bytes.Buffer, key, val *data.Gobj) error {
	switch val.Type_ {
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
	buffer.WriteByte(conf.RDB_TYPE_STRING)
	rdb.WriteString(buffer, key)
	rdb.WriteString(buffer, val)
	return nil
}

func (rdb *RDB) PersistList(db *db.GodisDB, buffer *bytes.Buffer, key, val *data.Gobj) error {
	rdb.checkExpire(db, buffer, key)
	buffer.WriteByte(conf.RDB_TYPE_LIST)
	rdb.WriteString(buffer, key)
	list := val.Val_.(*data.List)
	rdb.WriteLen(buffer, list.Length())

	// var node *data.Node
	// for i := 0; i < list.Length(); i++ {
	// 	node = list.ForwardIndex(i)
	// 	rdb.log.Debugln(node.Val.StrVal(), list.Length())
	// 	rdb.WriteString(buffer, node.Val)
	// }

	nodes := list.Range(0, list.Length())
	for _, node := range nodes {
		rdb.WriteString(buffer, node)
	}
	return nil
}

func (rdb *RDB) PersistDict(db *db.GodisDB, buffer *bytes.Buffer, key, val *data.Gobj) error {
	rdb.checkExpire(db, buffer, key)
	buffer.WriteByte(conf.RDB_TYPE_HASH)
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
	buffer.WriteByte(conf.RDB_TYPE_SET)
	rdb.WriteString(buffer, key)
	set := val.Val_.(*data.Set)
	rdb.WriteLen(buffer, set.Length())
	for _, member := range set.Dict.IterateDict() {
		rdb.WriteString(buffer, member[0])
	}
	return nil
}

func (rdb *RDB) PersistZSet(db *db.GodisDB, buffer *bytes.Buffer, key, val *data.Gobj) error {
	rdb.checkExpire(db, buffer, key)
	buffer.WriteByte(conf.RDB_TYPE_ZSET)
	rdb.WriteString(buffer, key)
	zset := val.Val_.(*data.ZSet)
	rdb.WriteLen(buffer, int(zset.Zcard()))
	for _, obj := range zset.Dict.IterateDict() {
		member, score := obj[0], obj[1]
		rdb.WriteString(buffer, member)
		rdb.WriteString(buffer, score)
	}
	return nil
}

func (rdb *RDB) PersistBit(db *db.GodisDB, buffer *bytes.Buffer, key, val *data.Gobj) error {
	rdb.checkExpire(db, buffer, key)

	buffer.WriteByte(conf.RDB_TYPE_BITMAP)
	rdb.WriteString(buffer, key)
	bitmap := val.Val_.(*data.Bitmap)
	rdb.WriteLen(buffer, bitmap.Len)
	for _, b := range bitmap.Bytes {
		buffer.WriteByte(b)
	}
	return nil
}
func (rdb *RDB) checkExpire(db *db.GodisDB, buffer *bytes.Buffer, key *data.Gobj) {
	if expireKey := db.Expire.Get(key); expireKey != nil {
		buffer.WriteByte(byte(conf.RDB_OPCODE_EXPIRETIME))

		expireTime := make([]byte, 8)
		binary.BigEndian.PutUint64(expireTime, uint64(expireKey.IntVal()))
		buffer.Write(expireTime)
	}
}
func (rdb *RDB) WriteString(buffer *bytes.Buffer, obj *data.Gobj) (bool, error) {
	str := obj.StrVal()
	rdb.WriteLen(buffer, len(str))
	buffer.WriteString(str)
	return true, nil
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

func (rdb *RDB) Load(db *db.GodisDB) error {
	_, err := os.Stat(rdb.Filename)
	if os.IsNotExist(err) {
		rdb.log.Errorf("rdb file %s not exist", rdb.Filename)
		return errs.RDBFileNotExistError
	}
	if err != nil {
		rdb.log.Errorf("stat rdb file %s failed, err:%v", rdb.Filename, err)
		return err
	}
	return rdb.load(db)
}

func (rdb *RDB) load(db *db.GodisDB) error {
	file, err := os.Open(rdb.Filename)
	if err != nil {
		rdb.log.Errorf("open rdb file %s failed, err:%v", rdb.Filename, err)
		return err
	}
	defer file.Close()

	buffer := make([]byte, conf.RDB_BUF_BLOCK_SIZE)
	n, err := file.Read(buffer)
	if err != nil {
		rdb.log.Errorf("read rdb file %s failed, err:%v", rdb.Filename, err)
		return err
	}
	// rdb.log.Debugln(buffer[9:n])
	if rdb.RDBCheckSum {
		getChecksum := binary.BigEndian.Uint64(buffer[n-8 : n])
		expectChecksum := util.CheckSumCreate(buffer[:n-8])
		if expectChecksum != getChecksum {
			rdb.log.Errorf("rdb file checksum not match,expect:%d,get:%d", expectChecksum, getChecksum)
			return errs.RDBFileDamagedError
		}
	}

	buffer, err = rdb.checkAppName(buffer)
	if err != nil {
		rdb.log.Errorf("check rdb file %s appname failed, err:%v", rdb.Filename, err)
		return err
	}
	buffer, err = rdb.checkVersion(buffer)
	if err != nil {
		rdb.log.Errorf("check rdb file %s version failed, err:%v", rdb.Filename, err)
		return err
	}

	var expireTime int64 = -1

	for {
		switch buffer[0] {
		case conf.RDB_OPCODE_EXPIRETIME:
			var ex int
			buffer, ex, err = rdb.LoadNumber(buffer[1:])
			expireTime = int64(ex)
			if err != nil {
				rdb.log.Errorf("load rdb file %s expiretime failed, err:%v", rdb.Filename, err)
				return err
			}
		case conf.RDB_OPCODE_EOF:
			return nil
		default:
			var key *data.Gobj
			buffer, key, err = rdb.LoadCommand(buffer, db)
			if err != nil {
				rdb.log.Errorf("load rdb file %s command failed, err:%v", rdb.Filename, err)
				return err
			}
			if expireTime != -1 {
				expObj := data.CreateObjectFromInt(expireTime)
				db.Expire.Set(key, expObj)
				expireTime = -1
			}
		}
	}
}

func (rdb *RDB) checkAppName(buffer []byte) ([]byte, error) {
	if string(buffer[:conf.RDB_APPNAME_LEN]) != conf.RDB_APPNAME {
		rdb.log.Errorf("rdb file %s is not godis rdb file", rdb.Filename)
		return nil, errs.RDBAppNameError
	}
	buffer = buffer[conf.RDB_APPNAME_LEN:]
	return buffer, nil
}

func (rdb *RDB) checkVersion(buffer []byte) ([]byte, error) {
	if string(buffer[:conf.RDB_VERSION_LEN]) != conf.RDB_VERSION {
		rdb.log.Errorf("rdb file %s version err", rdb.Filename)
		return nil, errs.RDBVersionError
	}
	buffer = buffer[conf.RDB_VERSION_LEN:]
	return buffer, nil
}

func (rdb *RDB) LoadNumber(buffer []byte) ([]byte, int, error) {
	switch {
	case buffer[0]>>6 == 0x00:
		return buffer[1:], int(buffer[0]), nil
	case buffer[0]>>6 == 0x01:
		return buffer[2:], int(uint16(buffer[0]&0x3f)<<8 | uint16(buffer[1])), nil
	case buffer[0]>>6 == 0x10:
		return buffer[5:], int(uint32(buffer[0]&0x3f)<<24 | uint32(buffer[1])<<16 | uint32(buffer[2])<<8 | uint32(buffer[3])), nil
	}
	return nil, 0, errs.RDBLoadFailedError
}

func (rdb *RDB) LoadCommand(buffer []byte, db *db.GodisDB) ([]byte, *data.Gobj, error) {
	var key *data.Gobj
	var err error
	switch buffer[0] {
	case conf.RDB_TYPE_STRING:
		buffer, key, err = rdb.LoadString(buffer[1:], db)
		if err != nil {
			return nil, nil, errs.RDBLoadFailedError
		}
	case conf.RDB_TYPE_LIST:
		buffer, key, err = rdb.LoadList(buffer[1:], db)
		if err != nil {
			return nil, nil, errs.RDBLoadFailedError
		}
	case conf.RDB_TYPE_HASH:
		buffer, key, err = rdb.LoadDict(buffer[1:], db)
		if err != nil {
			return nil, nil, errs.RDBLoadFailedError
		}
	case conf.RDB_TYPE_SET:
		buffer, key, err = rdb.LoadSet(buffer[1:], db)
		if err != nil {
			return nil, nil, errs.RDBLoadFailedError
		}
	case conf.RDB_TYPE_ZSET:
		buffer, key, err = rdb.LoadZset(buffer[1:], db)
		if err != nil {
			return nil, nil, errs.RDBLoadFailedError
		}
	case conf.RDB_TYPE_BITMAP:
		buffer, key, err = rdb.LoadBitmap(buffer[1:], db)
		if err != nil {
			return nil, nil, errs.RDBLoadFailedError
		}
	default:
		return nil, nil, errs.RDBLoadFailedError
	}

	return buffer, key, nil
}

func (rdb *RDB) LoadSDS(buffer []byte) ([]byte, *data.Gobj, error) {
	buffer, length, err := rdb.LoadNumber(buffer)
	if err != nil {
		return nil, nil, err
	}
	if len(buffer) < length {
		return nil, nil, errs.RDBLoadFailedError
	}
	val := string(buffer[:length])
	sds := &data.Gobj{
		Type_: conf.GSTR,
		Val_:  val,
	}

	buffer = buffer[length:]
	return buffer, sds, nil
}

func (rdb *RDB) LoadString(buffer []byte, db *db.GodisDB) ([]byte, *data.Gobj, error) {
	buffer, key, err := rdb.LoadSDS(buffer)
	if err != nil {
		return nil, nil, errs.RDBLoadFailedError
	}
	buffer, val, err := rdb.LoadSDS(buffer)
	if err != nil {
		return nil, nil, errs.RDBLoadFailedError
	}
	db.Data.Set(key, val)
	return buffer, key, nil
}

func (rdb *RDB) LoadList(buffer []byte, db *db.GodisDB) ([]byte, *data.Gobj, error) {
	buffer, key, err := rdb.LoadSDS(buffer)
	if err != nil {
		return nil, nil, errs.RDBLoadFailedError
	}
	buffer, length, err := rdb.LoadNumber(buffer)
	if err != nil {
		return nil, nil, errs.RDBLoadFailedError
	}

	list := data.ListCreate(data.ListType{EqualFunc: data.GStrEqual})
	var val *data.Gobj
	for i := 0; i < length; i++ {
		buffer, val, err = rdb.LoadSDS(buffer)
		if err != nil {
			return nil, nil, errs.RDBLoadFailedError
		}
		list.Append(val)
	}
	listObj := data.CreateObject(conf.GLIST, list)

	db.Data.Set(key, listObj)
	return buffer, key, nil
}

func (rdb *RDB) LoadDict(buffer []byte, db *db.GodisDB) ([]byte, *data.Gobj, error) {
	buffer, key, err := rdb.LoadSDS(buffer)
	if err != nil {
		return nil, nil, errs.RDBLoadFailedError
	}

	buffer, length, err := rdb.LoadNumber(buffer)
	if err != nil {
		return nil, nil, errs.RDBLoadFailedError
	}

	dict := data.DictCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual})
	var k, v *data.Gobj
	for i := 0; i < length; i++ {
		buffer, k, err = rdb.LoadSDS(buffer)
		if err != nil {
			return nil, nil, errs.RDBLoadFailedError
		}
		buffer, v, err = rdb.LoadSDS(buffer)
		if err != nil {
			return nil, nil, errs.RDBLoadFailedError
		}
		dict.Set(k, v)
	}
	dictObj := data.CreateObject(conf.GDICT, dict)

	db.Data.Set(key, dictObj)
	return buffer, key, nil
}

func (rdb *RDB) LoadSet(buffer []byte, db *db.GodisDB) ([]byte, *data.Gobj, error) {
	buffer, key, err := rdb.LoadSDS(buffer)
	if err != nil {
		return nil, nil, errs.RDBLoadFailedError
	}

	buffer, length, err := rdb.LoadNumber(buffer)
	if err != nil {
		return nil, nil, errs.RDBLoadFailedError
	}
	set := data.SetCreate(data.DictType{HashFunc: data.GStrHash, EqualFunc: data.GStrEqual})
	var k *data.Gobj
	for i := 0; i < length; i++ {
		buffer, k, err = rdb.LoadSDS(buffer)
		if err != nil {
			return nil, nil, errs.RDBLoadFailedError
		}
		set.SAdd(k)
	}
	setObj := data.CreateObject(conf.GSET, set)

	db.Data.Set(key, setObj)
	return buffer, key, nil
}

func (rdb *RDB) LoadZset(buffer []byte, db *db.GodisDB) ([]byte, *data.Gobj, error) {
	buffer, key, err := rdb.LoadSDS(buffer)
	if err != nil {
		return nil, nil, errs.RDBLoadFailedError
	}

	buffer, length, err := rdb.LoadNumber(buffer)
	if err != nil {
		return nil, nil, errs.RDBLoadFailedError
	}

	zset := data.NewZset()

	var k, score *data.Gobj
	for i := 0; i < length; i++ {
		buffer, k, err = rdb.LoadSDS(buffer)
		if err != nil {
			return nil, nil, errs.RDBLoadFailedError
		}
		buffer, score, err = rdb.LoadSDS(buffer)
		if err != nil {
			return nil, nil, errs.RDBLoadFailedError
		}
		zset.Zadd([]*data.Gobj{k, score})
	}
	zsetObj := data.CreateObject(conf.GZSET, zset)

	db.Data.Set(key, zsetObj)
	return buffer, key, nil
}

func (rdb *RDB) LoadBitmap(buffer []byte, db *db.GodisDB) ([]byte, *data.Gobj, error) {
	buffer, key, err := rdb.LoadSDS(buffer)
	if err != nil {
		return nil, nil, errs.RDBLoadFailedError
	}

	buffer, length, err := rdb.LoadNumber(buffer)
	if err != nil {
		return nil, nil, errs.RDBLoadFailedError
	}

	bitmap := data.BitmapCreate()
	bitmap.Bytes = buffer[:length]
	bitmap.Len = length
	buffer = buffer[length:]

	bitmapObj := data.CreateObject(conf.GBIT, bitmap)

	db.Data.Set(key, bitmapObj)
	return buffer, key, nil
}
