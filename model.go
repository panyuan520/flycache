package main

import (
	"bytes"

	"reflect"
	"sort"
	"strconv"
)

type Hh struct {
	data  interface{}
	proxy interface{}
}

func (this *Hh) createNode(category byte, count int) interface{} {
	if category == tl {
		return make([]interface{}, count)
	}
	return map[string]interface{}{}
}

func (this *Hh) add(key []byte, data interface{}) bool {
	keys := bytes.Split(key, delimiter)
	subkey := string(keys[len(keys)-1])
	if value, ok := this.proxy.([]interface{}); ok {
		if step, err := strconv.Atoi(subkey); err == nil {
			value[step] = data
		} else {
			return false
		}

	} else if value, ok := this.proxy.(map[string]interface{}); ok {
		value[subkey] = data
	}
	return true
}

func (this *Hh) addNode(key []byte, category byte, total int) {
	keys := bytes.Split(key, delimiter)
	count := len(keys)
	ele := this.data
	for step := 1; step < count; step++ {
		skey := string(keys[step])
		if value, ok := ele.([]interface{}); ok {
			index := BtI(keys[step])
			value[index] = this.createNode(category, total)
			ele = value
		} else if value, ok := ele.(map[string]interface{}); ok {
			if value2, ok2 := value[skey]; !ok2 {
				value[skey] = this.createNode(category, total)
				ele = value[skey]
			} else {
				ele = value2
			}
		}
	}
	this.proxy = ele
}

func NewHh(category byte) Hh {
	if category == tl {
		return Hh{data: []interface{}{}, proxy: nil}
	}
	return Hh{data: map[string]interface{}{}, proxy: nil}
}

//创建索引表数据
type Index struct {
	data [][]byte
}

func (this *Index) loop(value []byte) int {
	return sort.Search(len(this.data), func(i int) bool { return compare(this.data[i], greaterEqual, value) })
}

func (this *Index) dump() []byte {
	return bytes.Join(this.data, partitionMark)
}

func (this *Index) add(data []byte, step int) {
	this.data = append(this.data, bytes.Join([][]byte{data, ItB(step)}, delimiter))
}

func NewIndex(category byte) *Index {
	return &Index{data: [][]byte{[]byte{category}}}
}

//以有序数据来插入
type SL struct {
	data     [][]byte
	old_data [][]byte
	index    []int
	category byte
}

func (this SL) Len() int { return len(this.data) }
func (this SL) Swap(i, j int) {
	this.data[i], this.data[j] = this.data[j], this.data[i]
	this.index[i], this.index[j] = this.index[j], this.index[i]
	this.old_data[i], this.old_data[j] = this.old_data[j], this.old_data[i]
}
func (this SL) Less(i, j int) bool { return bytes.Compare(this.old_data[i], this.old_data[j]) == -1 }

func NewSL(tmp [][]byte) SL {
	indexs := []int{}
	old_data := [][]byte{}
	category := tf
	for index, item := range tmp {
		indexs = append(indexs, index)
		old_data = append(old_data, item[1:len(item)])
		category = item[0]
	}
	sl := SL{data: tmp, index: indexs, old_data: old_data, category: category}
	sort.Sort(sl)
	return sl
}

//自定义插入数据
type Modelx interface{}

//开始校验数据
func valid(element interface{}) (interface{}, bool) {
	v := reflect.ValueOf(element)
	switch v.Kind() {
	case reflect.Bool:
		return mergeTag6(tb, strconv.FormatBool(v.Bool())), true
	case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int64:
		return mergeTag6(ti, strconv.FormatInt(v.Int(), 10)), true
	case reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64:
		return mergeTag6(ti, strconv.FormatUint(v.Uint(), 10)), true
	case reflect.Float32, reflect.Float64:
		return mergeTag6(tf, strconv.FormatFloat(v.Float(), 'E', -1, 64)), true
	case reflect.String:
		vs := v.String()
		if len(vs) > 0 {
			return mergeTag6(ts, v.String()), true
		} else {
			return mergeTag6(tn, v.String()), true
		}
	case reflect.Slice, reflect.Array: //循环遍历列表list
		l := v.Len()
		tmp := [][]byte{}
		for i := 0; i < l; i++ {
			if cv, ok := valid(v.Index(i).Interface()); ok {
				if ccv, ok := cv.([]byte); ok {
					tmp = append(tmp, ccv)
				} else {
					return v, false
				}
			} else {
				return v, false
			}
		}
		return NewSL(tmp), true

	case reflect.Map: //遍历Map数据值
		tmp := map[string]interface{}{}
		for _, i := range v.MapKeys() {
			if mv, ok := valid(v.MapIndex(i).Interface()); ok {
				tmp[i.String()] = mv
			} else {
				return v, false
			}
		}
		return tmp, true
	default:
		return element, false
	}
	return element, false
}
