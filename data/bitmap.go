package data

import (
	"strconv"

	"github.com/godis/errs"
	"github.com/godis/util"
)

type Bitmap struct {
	Bytes []byte
	Top   int
	Len   int
}

func BitmapCreate() *Bitmap {
	return &Bitmap{
		Bytes: make([]byte, 0),
		Top:   -1,
		Len:   0,
	}
}

func (bit *Bitmap) SetBit(offset string, val string) error {
	b, err := bit.getByteValue(val)
	if err != nil {
		return err
	}
	offsetInt, err := strconv.Atoi(offset)
	if err != nil {
		return errs.BitOffsetError
	}
	if offsetInt < 0 {
		return errs.BitOffsetError
	} else if offsetInt >= bit.Len {
		newBytes := make([]byte, util.Max(2*offsetInt-bit.Len+1, 10))
		for i := 0; i < len(newBytes); i++ {
			newBytes[i] = '0'
		}
		bit.Bytes = append(bit.Bytes, newBytes...)
		bit.Len = 2*offsetInt + 1
	}
	bit.Bytes[offsetInt] = b

	if b == '1' {
		bit.Top = offsetInt
	}
	return nil
}

func (bit *Bitmap) getByteValue(val string) (byte, error) {
	switch val {
	case "0":
		return '0', nil
	case "1":
		return '1', nil
	default:
		return '0', errs.BitValueError
	}
}

func (bit *Bitmap) GetBit(offset string) (byte, error) {
	offsetInt, err := strconv.Atoi(offset)
	if err != nil {
		return 0, errs.BitOffsetError
	}
	if offsetInt < 0 {
		return 0, errs.BitOffsetError
	}
	if offsetInt >= bit.Len {
		return '0', nil
	}
	b := bit.Bytes[offsetInt]
	return b, nil
}

func (bit *Bitmap) BitCount() int {
	count := 0
	for i := range bit.Bytes {
		if bit.Bytes[i] == '1' {
			count++
		}
	}
	return count
}

func (bit *Bitmap) BitOp(bit2 *Bitmap, op string) (string, error) {
	switch op {
	case "and":
		return bit.BitOpAND(bit2), nil
	case "or":
		return bit.BitOpOR(bit2), nil
	case "xor":
		return bit.BitOpXOR(bit2), nil
	default:
		return "", errs.BitOpError
	}
}

func (bit *Bitmap) BitOpAND(bit2 *Bitmap) string {
	bytes := []byte{}
	l, r := 0, 0
	for ; l <= bit.Top && r <= bit2.Top; l, r = l+1, r+1 {
		bytes = append(bytes, (bit.Bytes[l]&1)&(bit2.Bytes[r]&1)+'0')
	}
	for ; l <= bit.Top; l++ {
		bytes = append(bytes, '0')
	}
	for ; r <= bit2.Top; r++ {
		bytes = append(bytes, '0')
	}
	return string(bytes)
}

func (bit *Bitmap) BitOpOR(bit2 *Bitmap) string {
	bytes := []byte{}
	l, r := 0, 0
	for ; l <= bit.Top && r <= bit2.Top; l, r = l+1, r+1 {
		bytes = append(bytes, (bit.Bytes[l]&1)|(bit2.Bytes[r]&1)+'0')
	}
	for ; l <= bit.Top; l++ {
		bytes = append(bytes, bit.Bytes[l])
	}
	for ; r <= bit2.Top; r++ {
		bytes = append(bytes, bit2.Bytes[r])
	}
	return string(bytes)
}

func (bit *Bitmap) BitOpXOR(bit2 *Bitmap) string {
	bytes := []byte{}
	l, r := 0, 0
	for ; l <= bit.Top && r <= bit2.Top; l, r = l+1, r+1 {
		bytes = append(bytes, (bit.Bytes[l]&1)^(bit2.Bytes[r]&1)+'0')
	}
	for ; l <= bit.Top; l++ {

		bytes = append(bytes, bit.Bytes[l])
	}
	for ; r <= bit2.Top; r++ {
		bytes = append(bytes, bit2.Bytes[r])
	}
	return string(bytes)
}

func (bit *Bitmap) BitPos(target string) (int, error) {
	val, err := bit.getByteValue(target)
	if err != nil {
		return 0, errs.BitValueError
	}
	for i := range bit.Bytes {
		if bit.Bytes[i] == val {
			return i, nil
		}
	}
	return 0, errs.BitNotFoundError
}
