package util

import (
	"hash/crc64"
	"time"
)

func GetMsTime() int64 {
	return time.Now().UnixNano() / 1e6
}

func GetUsTime() int64 {
	return time.Now().UnixNano() / 1e3
}

func GetTime() int64 {
	return time.Now().Unix()
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func Abs(x int) int {
	if x > 0 {
		return x
	}
	return -1 * x
}
func CheckSumCreate(bytes []byte) uint64 {
	return crc64.Checksum(bytes, crc64.MakeTable(crc64.ECMA))
}
