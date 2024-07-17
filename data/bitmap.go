package data

import (
	"encoding/binary"
	"strconv"

	"github.com/godis/errs"
	"github.com/godis/util"
)

const (
	MaxOffset = 7
)

type Bitmap struct {
	Bytes []byte
	Len   int
}

func BitmapCreate() *Bitmap {
	return &Bitmap{
		Bytes: make([]byte, 0),
		Len:   0,
	}
}

func (bit *Bitmap) SetBit(offsetStr string, val string) (byte, error) {
	b, err := bit.getByteValue(val)
	if err != nil {
		return 0, err
	}
	offsetInt, err := strconv.Atoi(offsetStr)
	if err != nil {
		return 0, errs.BitOffsetError
	}

	index := offsetInt / 8
	offset := MaxOffset - offsetInt%8
	if index < 0 {
		return 0, errs.BitOffsetError
	} else if index >= bit.Len {
		newBytes := make([]byte, util.Max(2*index-bit.Len+1, 8))
		bit.Bytes = append(bit.Bytes, newBytes...)
		bit.Len += len(newBytes)
	}
	rawByte := bit.Bytes[index] >> offset & 1

	if b == 1 {
		bit.Bytes[index] |= 1 << offset
	} else {
		bit.Bytes[index] &= 0xff ^ (1 << offset)
	}
	return rawByte, nil
}

func (bit *Bitmap) getByteValue(val string) (byte, error) {
	switch val {
	case "0":
		return 0, nil
	case "1":
		return 1, nil
	default:
		return 0, errs.BitValueError
	}
}

func (bit *Bitmap) GetBit(offsetStr string) (byte, error) {
	offsetInt, err := strconv.Atoi(offsetStr)
	if err != nil {
		return 0, errs.BitOffsetError
	}

	index := offsetInt / 8
	offset := MaxOffset - offsetInt%8
	if index < 0 {
		return 0, errs.BitOffsetError
	}
	if index >= bit.Len {
		return '0', nil
	}
	b := bit.Bytes[index] >> offset & 1

	switch b {
	case 0:
		return '0', nil
	case 1:
		return '1', nil
	default:
		return 0, errs.BitValueError
	}
}

func (bit *Bitmap) BitCount() int {
	count := 0
	for i := range bit.Bytes {
		for j := MaxOffset; j >= 0; j-- {
			if (bit.Bytes[i] >> j & 1) == 1 {
				count++
			}
		}
	}
	return count
}

func (bit *Bitmap) BitOp(bit2 *Bitmap, op string) (uint64, error) {
	switch op {
	case "and":
		return bit.BitOpAND(bit2), nil
	case "or":
		return bit.BitOpOR(bit2), nil
	case "xor":
		return bit.BitOpXOR(bit2), nil
	default:
		return 0, errs.BitOpError
	}
}

func (bit *Bitmap) BitOpAND(bit2 *Bitmap) uint64 {
	bytes := []byte{}
	l, r := 0, 0
	for ; l < bit.Len && r < bit2.Len; l, r = l+1, r+1 {
		bytes = append(bytes, (bit.Bytes[l])&(bit2.Bytes[r]))
	}
	for ; l < bit.Len; l++ {
		bytes = append(bytes, 0)
	}
	for ; r < bit2.Len; r++ {
		bytes = append(bytes, 0)
	}

	return binary.BigEndian.Uint64(bytes)
}

func (bit *Bitmap) BitOpOR(bit2 *Bitmap) uint64 {
	bytes := []byte{}
	l, r := 0, 0
	for ; l < bit.Len && r < bit2.Len; l, r = l+1, r+1 {
		bytes = append(bytes, (bit.Bytes[l])|(bit2.Bytes[r]))
	}
	for ; l < bit.Len; l++ {
		bytes = append(bytes, (bit.Bytes[l]))
	}
	for ; r < bit2.Len; r++ {
		bytes = append(bytes, (bit2.Bytes[r]))
	}
	return binary.BigEndian.Uint64(bytes)
}
func (bit *Bitmap) BitOpXOR(bit2 *Bitmap) uint64 {
	bytes := []byte{}
	l, r := 0, 0
	for ; l < bit.Len && r < bit2.Len; l, r = l+1, r+1 {
		bytes = append(bytes, (bit.Bytes[l])^(bit2.Bytes[r]))
	}
	for ; l < bit.Len; l++ {
		bytes = append(bytes, 0xff)
	}
	for ; r < bit2.Len; r++ {
		bytes = append(bytes, 0xff)
	}
	return binary.BigEndian.Uint64(bytes)
}

func (bit *Bitmap) BitPos(target string) (int, error) {
	var index int
	val, err := bit.getByteValue(target)
	if err != nil {
		return 0, errs.BitValueError
	}
	for i := range bit.Bytes {
		for j := MaxOffset; j >= 0; j-- {
			if (bit.Bytes[i] >> j & 1) != val {
				index++
			} else {
				return index, nil
			}
		}
	}
	return 0, errs.BitNotFoundError
}
