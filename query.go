package main

import (
	"bytes"
	"reflect"
	"sort"
	"strings"
)

type Query struct {
	Table  []byte
	Keys   [][]byte
	Output []string
	Filter [][][]byte
	Order  [][][]byte
	store  *Store
}

func (this *Query) ParseSelect(content []byte) {
	seqs := bytes.Split(content, comma)
	for _, seq := range seqs {
		this.Keys = append(this.Keys, seq)
		this.Output = append(this.Output, string(seq))
	}
}

func (this *Query) ParseWhere(content []byte) {
	seqs := bytes.Split(content, comma)
	for _, seq := range seqs {
		wheres := bytes.Split(seq, questionMark)
		this.Filter = append(this.Filter, wheres)
		this.Keys = append(this.Keys, wheres[0])
	}
}

func (this *Query) ParseOrder(content []byte) {
	seqs := bytes.Split(content, comma)
	for _, seq := range seqs {
		orders := bytes.Split(seq, questionMark)
		this.Order = append(this.Order, orders)
		this.Keys = append(this.Keys, orders[0])
	}
}

func (this *Query) Init(key []byte) {
	seqs := bytes.Split(key, partitionMark)
	for _, seq := range seqs {
		category := seq[0]
		content := seq[1:]
		switch category {
		case byte('s'):
			this.ParseSelect(content)
		case byte('f'):
			this.Table = content
		case byte('w'):
			this.ParseWhere(content)
		case byte('o'):
			this.ParseOrder(content)
		}
	}
}

type Eles []interface{}

func (this Eles) Len() int                                  { return len(this) }
func (this Eles) Swap(i, j int)                             { this[i], this[j] = this[j], this[i] }
func (this Eles) Less(i, j int) bool                        { return this.Compare(this[i], this[j]) }
func (this Eles) Compare(c interface{}, d interface{}) bool { return this.XCompare(c, d, less) }
func (this Eles) XCompare(c interface{}, d interface{}, cp []byte) bool {
	v := reflect.ValueOf(c)
	v1 := reflect.ValueOf(d)
	switch v.Kind() {
	case reflect.Float64:
		if compare(cp, equal, less) {
			return v.Float() < v1.Float()
		} else if compare(cp, equal, lessEqual) {
			return v.Float() <= v1.Float()
		}
	case reflect.Uint:
		if compare(cp, equal, less) {
			return v.Uint() < v1.Uint()
		} else if compare(cp, equal, lessEqual) {
			return v.Uint() <= v1.Uint()
		}
	case reflect.Int:
		if compare(cp, equal, less) {
			return v.Int() < v1.Int()
		} else if compare(cp, equal, lessEqual) {
			return v.Int() <= v1.Int()
		}
	case reflect.String:
		if compare(cp, equal, less) {
			return strings.Compare(v.String(), v1.String()) == -1
		} else if compare(cp, equal, lessEqual) {
			return strings.Compare(v.String(), v1.String()) == -1 || strings.Compare(v.String(), v1.String()) == 0
		}
	}
	return false
}

//func (this Eles) IndexIndex(index []byte) int {
//	count := len(this)
//	in := sort.Search(count, func(i int) bool { return this.XCompare(index, this[i].Index, lessEqual) })
//	if in < count {
//		if compare(this[in].Index, equal, index) {
//			return in
//		}
//	}
//	return count
//}
func (this Eles) ValueIndex(comp, tag []byte) (int, int, int) {
	in := sort.Search(len(this), func(i int) bool { return this.XCompare(tag, this[i], lessEqual) })
	start := 0
	end := 0
	if in < len(this) {
		if compare(comp, equal, equal) {
			start = in
			end = in
		} else if compare(comp, equal, less) {
			start = 0
			end = in

		} else if compare(comp, equal, lessEqual) {
			start = 0
			end = in
			if this.XCompare(this[in], tag, equal) {
				end = in + 1
			}
		} else if compare(comp, equal, greater) {
			start = in
			end = len(this)
			if this.XCompare(this[in], tag, equal) {
				start = in + 1
			}
		} else if compare(comp, equal, greaterEqual) {
			start = in
			end = len(this)
		}
	}
	return in, start, end
}

func (this *Query) filter() interface{} {
	//开始设置需要处理的数据
	backends := map[string]Eles{}
	for _, key := range this.Keys {
		skey := string(key)
		if _, ok := backends[skey]; !ok {
			key2 := mergeTag4(this.Table, key)
			backends[skey] = this.store.RangeElement(key2)
		}
	}
	//	tree := Orders{}
	//	for step, tags := range this.Filter {
	//		if backend, ok := backends[string(tags[0])]; ok {
	//			if in, start, end := backend.L.ValueIndex(tags[1], tags[2]); in < len(backend.L) {
	//				backend2 := backend.L[start:end]
	//				if step == 0 {
	//					tree.L = append(tree.L, backend2...)
	//				} else {
	//					tree2 := Orders{}
	//					for _, bk := range backend2 {
	//						if in := tree.L.IndexIndex(bk.Index); in < len(tree.L) {
	//							tree2.L = append(tree2.L, bk)
	//						}
	//					}
	//					tree = tree2
	//				}
	//			} else {
	//				tree = Orders{}
	//				break
	//			}
	//		}
	//	}
	//	//需要返回的数据
	//	outputs := map[string][][]byte{}
	//	if len(this.Filter) > 0 && len(tree.L) == 0 {
	//		return outputs
	//	}
	//	//开始设置排序
	//	orders := Orders{}
	//	for _, tags := range this.Order {
	//		if backend, ok := backends[string(tags[0])]; ok {
	//			for _, item := range tree.L {
	//				orders.L = append(orders.L, Ele{Index: item.Index, Value: backend.M[string(item.Index)]})
	//			}
	//			if compare(tags[1], equal, one) {
	//				sort.Sort(sort.Reverse(orders.L))
	//			} else {
	//				sort.Sort(orders.L)
	//			}
	//		}
	//	}
	//	//开始输出数据
	//	for _, key := range this.Output {
	//		if backend, ok := backends[key]; ok {
	//			tmp := [][]byte{}
	//			for _, ele := range orders.L {
	//				tmp = append(tmp, backend.M[string(ele.Index)])
	//			}
	//			outputs[key] = tmp
	//		}
	//	}
	//	return outputs
	return nil
}
