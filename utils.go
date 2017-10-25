package main

import (
	"bytes"
	"encoding/binary"

	"math"
	"strconv"

	"github.com/vmihailenco/msgpack"
)

func reply(messge []byte) []byte {
	return bytes.Join([][]byte{messge, crlf}, blank)
}

func mergeTag(tip1 string, c []byte) []byte {
	b := []byte(tip1)
	return bytes.Join([][]byte{b, c}, blank)
}

func mergeTag2(key []byte, tip2 string) []byte {
	c := []byte(tip2)
	return bytes.Join([][]byte{key, delimiter, c}, blank)
}

func mergeTag3(tip1 []byte, tip3 []byte) []byte {
	return bytes.Join([][]byte{tip1, delimiter, tip3}, blank)
}

func mergeTag4(tip1 []byte, tip3 []byte) []byte {
	return bytes.Join([][]byte{tip1, delimiter, tip3}, blank)
}

func mergeTag5(tip1 []byte, tip2 []byte) []byte {
	return bytes.Join([][]byte{tip1, delimiter, tip2}, blank)
}

func mergeTag6(first byte, last string) []byte {
	tmp := []byte("")
	tmp = append(tmp, first)
	tmp = append(tmp, []byte(last)...)
	return tmp
}

func mergeTag7(first []byte, last []byte) []byte {
	return bytes.Join([][]byte{first, last}, blank)
}

func LastIndex(key []byte) string {
	indexs := bytes.Split(key, delimiter)
	if len(indexs) > 0 {
		return string(indexs[len(indexs)-1])
	}
	return ""
}

func S2B(s string) []byte {
	return []byte(s)
}

func B2S(b []byte) string {
	return string(b)
}

func F32B(float float32) []byte {
	bits := math.Float32bits(float)
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, bits)
	return bytes
}

func B2F32(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	return math.Float32frombits(bits)
}

func F642B(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}

func B2F64(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	return math.Float64frombits(bits)
}

func BtI(buf []byte) int {
	if i, err := strconv.Atoi(string(buf)); err == nil {
		return i
	}
	return 0
}

func ItS(value int) string {
	return strconv.Itoa(value)
}

func ItB(value int) []byte {
	return []byte(ItS(value))
}

func compare(value []byte, tip []byte, tag []byte) bool {
	if bytes.Equal(equal, tip) && bytes.Compare(value, tag) == 0 {
		return true
	} else if bytes.Equal(greater, tip) && bytes.Compare(value, tag) == 1 {
		return true
	} else if bytes.Equal(greaterEqual, tip) && bytes.Compare(value, tag) >= 0 {
		return true
	} else if bytes.Equal(less, tip) && bytes.Compare(value, tag) == -1 {
		return true
	} else if bytes.Equal(lessEqual, tip) && bytes.Compare(value, tag) <= 0 {
		return true
	}
	return false
}

func marshaler(value interface{}) ([]byte, bool) {
	if b, err := msgpack.Marshal(value); err == nil {
		return b, true
	}
	return nil, false
}

func unmarshaler(value []byte, model Modelx) (Modelx, bool) {
	if err := msgpack.Unmarshal(value, &model); err == nil {
		//开始校验数据
		return model, true
	}
	return nil, false
}
