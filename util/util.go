package util

import "time"

func GetMsTime() int64 {
	return time.Now().UnixNano() / 1e6
}

func GetTime() int64 {
	return time.Now().Unix()
}
