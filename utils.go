package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"strconv"
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
	return bytes.Join([][]byte{tip1, delimiter, tip2, delimiter}, blank)
}

func LastIndex(key []byte) string {
	indexs := bytes.Split(key, delimiter)
	if len(indexs) > 0 {
		return string(indexs[len(indexs)-1])
	}
	return ""
}

func BtI(buf []byte) int64 {
	if s, err := strconv.ParseInt(string(buf), 10, 32); err == nil {
		return s
	}
	return 0
}

func BtF(bytes []byte) float64 {
	float, err := strconv.ParseFloat(string(bytes), 64)
	if err != nil {
		fmt.Println(err)
	}
	return float
}

func FtB(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}

func compare(value []byte, tip []byte, tag []byte) bool {
	if bytes.Compare(equal, tip) == 0 && bytes.Compare(value, tag) == 0 {
		return true
	} else if bytes.Compare(greater, tip) == 0 && bytes.Compare(value, tag) > 0 {
		return true
	} else if bytes.Compare(greaterEqual, tip) == 0 && bytes.Compare(value, tag) >= 0 {
		return true
	} else if bytes.Compare(less, tip) == 0 && bytes.Compare(value, tag) < 0 {
		return true
	} else if bytes.Compare(lessEqual, tip) == 0 && bytes.Compare(value, tag) <= 0 {
		return true
	}
	return false
}

func crunchSplitFunc(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if i := bytes.Index(data, delimit); i >= 0 {
		return i + 2, data[0:i], nil
	}

	if atEOF {
		return len(data), data, nil
	}

	return
}
