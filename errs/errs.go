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
	TypeConvertError = &GodisError{100, "type convert error"}
	TypeCheckError   = &GodisError{101, "type check error"}
	ParamsCheckError = &GodisError{102, "params check error"}
	OutOfRangeError  = &GodisError{103, "out of range error"}
)

// 数据类型errors
var (
	SkipListDeleteNodeError = &GodisError{1000, "skip list delete node error"}
)

// 通用errors
var (
	ExpandError      = &GodisError{10000, "expand error"}
	KeyExistsError   = &GodisError{10001, "key exists error"}
	KeyNotExistError = &GodisError{10002, "key not exists error"}
	OutOfLimitError  = &GodisError{10003, "cmd length out of limit error"}
	WrongCmdError    = &GodisError{10004, "wrong cmd error"}
)
