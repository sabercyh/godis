package conf

type RDBOp byte
type RDBLenType byte

const (
	RDB_APPNAME       string = "GODIS"
	RDB_GODIS_VERSION string = "0001"

	RDB_Expire RDBOp = 0xfd
	RDB_EOF    RDBOp = 0xff
)

const (
	RDB_BUF_BLOCK_SIZE    = 10 * 1024 * 1024
	AOF_RW_BUF_BLOCK_SIZE = 10 * 1024 * 1024
	AOF_BUF_BLOCK_SIZE    = 10 * 1024 * 1024
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
}
