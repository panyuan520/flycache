package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
)

var delimit []byte = []byte("\r\n")
var delimit2 []byte = []byte("\t\n")
var blank []byte = []byte("")
var comma []byte = []byte(",")
var questionMark []byte = []byte("?")
var partitionMark []byte = []byte("|")
var delimiter []byte = []byte(":")
var equal []byte = []byte("=")
var greater []byte = []byte(">")
var greaterEqual []byte = []byte(">=")
var less []byte = []byte("<")
var lessEqual []byte = []byte("<=")
var zero []byte = []byte("0")
var one []byte = []byte("1")

func reply(messge []byte) []byte {
	crlf := []byte("\\r\\n")
	messge = append(messge, crlf...)
	return messge
}

func mergeTag(tip1 string, c []byte) []byte {
	b := []byte(tip1)
	b = append(b, c...)
	//b = append(b, delimiter...)
	return b
}

func mergeTag2(key []byte, tip2 string) []byte {
	a := []byte{}
	//b := key
	c := []byte(tip2)
	a = append(a, key...)
	a = append(a, delimiter...)
	a = append(a, c...)
	return a
}

func mergeTag3(tip1 []byte, tip3 []byte) []byte {
	m := []byte("")
	m = append(m, tip1...)
	m = append(m, delimiter...)
	m = append(m, tip3...)
	return m
}

func mergeTag4(tip1 []byte, tip3 []byte) []byte {
	m := []byte("")
	m = append(m, tip1...)
	m = append(m, delimiter...)
	m = append(m, tip3...)
	//m = append(m, delimiter...)
	return m
}

func BytesToInt64(buf []byte) int {
	_, i := binary.Varint(buf)
	return i
}

func BytesToFloat64(bytes []byte) float64 {
	float, err := strconv.ParseFloat(string(bytes), 64)
	if err != nil {
		fmt.Println(err)
	}
	return float
}

func Float64Tobytes(float float64) []byte {
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
