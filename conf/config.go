package conf

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
	GSTR  Gtype = 0x00
	GLIST Gtype = 0x01
	GSET  Gtype = 0x02
	GZSET Gtype = 0x03
	GDICT Gtype = 0x04
	GBIT  Gtype = 0x05
)

type Gval any

type Config struct {
	Port           int    `json:"port"`
	AppendOnly     bool   `json:"appendonly"`     //是否启用AOF
	Dir            string `json:"dir"`            //AOF文件保存路径
	AppendFilename string `json:"appendfilename"` //AOF文件名
	Appendfsync    string `json:"appendfsync"`    //AOF持久化策略，AOF_FSYNC_EVERYSEC|AOF_FSYNC_ALWAYS|AOF_FSYNC_NO
	AOFBufferSize  int    `json:"aofbuffersize"`  //持久化策略为AOF_FSYNC_NO时，缓冲区大小
}
