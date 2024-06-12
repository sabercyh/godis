package errs

type GodisError struct {
	Errno  int    `json:"err_no"`
	ErrMsg string `json:"err_msg"`
}

func (e *GodisError) Error() string {
	return e.ErrMsg
}

var CustomError = &GodisError{1, "custom error"}

// 基础errors
var (
	TypeConvertError    = &GodisError{100, "type convert error"}
	TypeCheckError      = &GodisError{101, "type check error"}
	ParamsCheckError    = &GodisError{102, "params check error"}
	OutOfRangeError     = &GodisError{103, "out of range error"}
	AOFBufferWriteError = &GodisError{104, "aof buffer write error"}
	AOFFileSaveError    = &GodisError{105, "aof file save error"}
	UnknownError        = &GodisError{106, "unknown error"}
	RDBIsSavingError    = &GodisError{107, "rdb is saving error"}
	ForkError           = &GodisError{108, "fork error"}
)

// 数据类型errors
var (
	SkipListDeleteNodeError = &GodisError{1000, "skip list delete node error"}
	FieldNotExistError      = &GodisError{1001, "field not exists error"}
	FieldExistError         = &GodisError{1002, "field exists error"}
	DelFieldError           = &GodisError{1003, "del field error"}
	BitNotFoundError        = &GodisError{1004, "bit not found error"}
	BitOffsetError          = &GodisError{1005, "bit offset is not an integer or out of range"}
	BitValueError           = &GodisError{1006, "bit is not an integer or out of range"}
	BitOpError              = &GodisError{1007, "bitop error"}
	NodeNotFoundError       = &GodisError{1008, "node not found error"}
)

// 通用errors
var (
	ExpandError      = &GodisError{10000, "expand error"}
	KeyExistsError   = &GodisError{10001, "key exists error"}
	KeyNotExistError = &GodisError{10002, "key not exists error"}
	OutOfLimitError  = &GodisError{10003, "cmd length out of limit error"}
	WrongCmdError    = &GodisError{10004, "wrong cmd error"}
	DelKeyError      = &GodisError{10005, "del key error"}
)
