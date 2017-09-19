package main

import (
	"bytes"
	"fmt"
	"sort"
)

type Eles struct {
	Key    []string
	Value  [][]byte
	Ivalue map[string]interface{}
	Bvalue map[string][]byte
}

func (this *Eles) Len() int { return len(this.Key) }
func (this *Eles) Swap(i, j int) {
	this.Key[i], this.Key[j] = this.Key[j], this.Key[i]
	this.Value[i], this.Value[j] = this.Value[j], this.Value[i]
}
func (this *Eles) Less(i, j int) bool              { return this.Compare(this.Value[i], this.Value[j]) }
func (this *Eles) Compare(c []byte, d []byte) bool { return compare(c, less, d) }

func (this *Eles) Sort() {
	sort.Sort(this)
}
func (this *Eles) Reverse() {
	sort.Sort(sort.Reverse(this))
}

func (this *Eles) Search(x []byte, tip []byte) int {
	index := len(this.Key) + 1
	if compare(tip, equal, less) || compare(tip, equal, lessEqual) {
		sort.Sort(this)
		index = sort.Search(len(this.Value), func(i int) bool { return compare(x, lessEqual, this.Value[i]) })
		for _, i := range this.Value {
			fmt.Println("debug1", string(i))
		}
	} else if compare(tip, equal, greater) || compare(tip, equal, greaterEqual) {
		sort.Sort(sort.Reverse(this))
		index = sort.Search(len(this.Value), func(i int) bool { return compare(x, greaterEqual, this.Value[i]) })
		for _, i := range this.Value {
			fmt.Println("debug2", string(i))
		}
	}
	if index < len(this.Value) && !bytes.HasSuffix(tip, []byte("=")) && compare(this.Value[index], equal, x) {
		index -= 1
	}
	return index
}

func (this *Eles) Match(x []byte) int {
	i := this.Search(x, lessEqual)
	if i < len(this.Value) && compare(this.Value[i], equal, x) {
		return i
	}
	return -1
}

func (this *Eles) Add(key string, value []byte, ivalue interface{}) {
	skey := string(key)
	this.Key = append(this.Key, skey)
	this.Value = append(this.Value, value)
	this.Ivalue[skey] = ivalue
	this.Bvalue[skey] = value
}

func NewEles() Eles {
	return Eles{Key: []string{}, Value: [][]byte{}, Ivalue: map[string]interface{}{}, Bvalue: map[string][]byte{}}
}

type Ids map[string]int

func (this Ids) extend(indexs []string) {
	if len(this) == 0 {
		for step, index := range indexs {
			this[index] = step
		}
	} else {
		ids := Ids{}
		for _, index := range indexs {
			if cid, ok := this[index]; ok {
				ids[index] = cid
			}
		}
		this = ids
	}
}

type Lt []interface{}

func (this *Lt) Add(index string, value interface{}) {
	*this = append(*this, value)
}

func NewLt() Lt {
	return Lt{}
}

type Hh map[string]interface{}

func (this Hh) Add(index string, value interface{}) {
	this[index] = value
}
func NewHh() Hh {
	return Hh{}
}
