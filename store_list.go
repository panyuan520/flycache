package main

/**
import (
	"bytes"
	"reflect"
	"strconv"
)


func (this *Store) LAdd(key []byte, v reflect.Value) {
	l := v.Len()
	keys := [][]byte{}
	for i := 0; i < l; i++ {
		key3 := mergeTag2(key, strconv.Itoa(i))
		this.Save(key3, v.Index(i).Interface())
		keys = append(keys, key3)
	}
	key4 := bytes.Join(keys, partitionMark)
	this.Set(key, mergeTag("l", key4))
}
*/
