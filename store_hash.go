package main

/**
import (
	"bytes"
	"reflect"
)


func (this *Store) MAdd(key []byte, v reflect.Value) {
	l := [][]byte{}
	for _, key2 := range v.MapKeys() {
		key4 := mergeTag2(key, key2.String())
		l = append(l, key4)
		this.Save(key4, v.MapIndex(key2).Interface())
	}
	ls := bytes.Join(l, partitionMark)
	this.Set(key, mergeTag("m", ls))
}
*/
