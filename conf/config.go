package conf

type RDBLenType byte

const (
	RDB_APPNAME     string = "GODIS"
	RDB_VERSION     string = "0001"
	RDB_APPNAME_LEN        = 5
	RDB_VERSION_LEN        = 4

	RDB_OPCODE_EXPIRETIME = 0xfd
	RDB_OPCODE_EOF        = 0xff
)

const (
	RDB_BUF_BLOCK_SIZE    = 20 * 1024 * 1024
	AOF_RW_BUF_BLOCK_SIZE = 10 * 1024 * 1024
	AOF_BUF_BLOCK_SIZE    = 10 * 1024 * 1024

	EXPIRE_CHECK_COUNT int = 100
)

type CmdType = byte

const (
	COMMAND_UNKNOWN CmdType = 0x00
	COMMAND_INLINE  CmdType = 0x01
	COMMAND_BULK    CmdType = 0x02
)

const (
	GODIS_IO_BUF     int = 1024 * 16
	GODIS_MAX_BULK   int = 1024 * 4
	GODIS_MAX_INLINE int = 1024 * 4
	GODIS_REPLY_BUF  int = 128
)

type Gtype uint8

const (
	GSTR     Gtype = 0x00
	GLIST    Gtype = 0x01
	GSET     Gtype = 0x02
	GZSET    Gtype = 0x03
	GDICT    Gtype = 0x04
	GBIT     Gtype = 0x05
	GSLOWLOG Gtype = 0x06
	GBYTES   Gtype = 0x06
)
const (
	RDB_TYPE_STRING = 0x00
	RDB_TYPE_LIST   = 0x01
	RDB_TYPE_SET    = 0x02
	RDB_TYPE_ZSET   = 0x03
	RDB_TYPE_HASH   = 0x04
	RDB_TYPE_BIT    = 0x05
)

type Gval any

type Config struct {
	Port     int   `json:"port"`
	WorkerID int64 `json:"workerid"`

	RDBCompression bool   `json:"rdbcompression"`
	RDBCheckSum    bool   `json:"rdbchecksum"`
	DBFilename     string `json:"dbfilename"`

	AppendOnly     bool   `json:"appendonly"`     //是否启用AOF
	Dir            string `json:"dir"`            //AOF文件保存路径
	AppendFilename string `json:"appendfilename"` //AOF文件名
	Appendfsync    string `json:"appendfsync"`    //AOF持久化策略，AOF_FSYNC_EVERYSEC|AOF_FSYNC_ALWAYS|AOF_FSYNC_NO

	SlowLogSlowerThan int64 `json:"slowlogslowerthan"` //慢查询阈值
	SlowLogMaxLen     int   `json:"slowlogmaxlen"`     //慢查询日志最大长度

	MaxClients int `json:"maxclients"`
}
