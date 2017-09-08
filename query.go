package main

import (
	"bytes"
)

type Filter struct {
	column []byte
	start  []byte
	end    []byte
	tip    []byte
}

type Order struct {
	column []byte
	desc   []byte
}

type Query struct {
	namespace []byte
	output    [][]byte
	where     []Filter
	order     []Order
	store     *Store
}

func (this *Query) Init(key []byte) {
	seqs := bytes.Split(key, partitionMark)
	for _, seq := range seqs {
		category := seq[0]
		content := seq[1:]
		switch category {
		case byte('s'):
			seqs := bytes.Split(content, comma)
			for _, seq := range seqs {
				this.output = append(this.output, seq)
			}
		case byte('f'):
			this.namespace = content
		case byte('w'):
			seqs := bytes.Split(content, comma)
			for _, seq := range seqs {
				wheres := bytes.Split(seq, questionMark)
				if len(wheres) > 2 {
					prefix := mergeTag5(this.namespace, wheres[0])
					this.where = append(this.where, Filter{column: wheres[0], start: prefix, end: wheres[2], tip: wheres[1]})
				}
			}
		case byte('o'):
			seqs := bytes.Split(content, comma)
			for _, seq := range seqs {
				orders := bytes.Split(seq, questionMark)
				if len(orders) > 1 {
					prefix := mergeTag5(this.namespace, orders[0])
					this.order = append(this.order, Order{column: prefix, desc: orders[1]})
				}
			}
		}
	}
}

func (this *Query) Get(key []byte) Eles {
	skey := string(mergeTag3(key, cindex))
	if eles, ok := this.store.cache.Get(skey); ok {
		return eles.(Eles)
	}
	eles := this.store.RangePrefix(key)
	this.store.cache.Add(skey, eles)
	return eles
}

func (this *Query) Where() Ids {
	ids := Ids{}
	for _, where := range this.where {
		eles := this.Get(where.start)
		if index := eles.Search(where.end, where.tip); index < len(eles.Key) {
			ids.extend(eles.Key[0 : index+1])
		} else {
			ids = Ids{}
			break
		}
	}
	return ids
}

func (this *Query) Order(ids Ids) []string {
	for _, order := range this.order {
		beles := NewEles()
		eles := this.Get(order.column)
		for index, _ := range ids {
			beles.Add(index, eles.Bvalue[index], eles.Ivalue[index])
		}
		if compare(order.desc, equal, []byte("1")) {
			beles.Sort()
		} else {
			beles.Reverse()
		}
		return beles.Key
	}
	return []string{}
}

func (this *Query) Filter() interface{} {
	ids := this.Where()
	indexs := this.Order(ids)
	backend := map[string][]interface{}{}
	for _, out := range this.output {
		tmp := []interface{}{}
		prefix := mergeTag5(this.namespace, out)
		eles := this.Get(prefix)
		for _, index := range indexs {
			tmp = append(tmp, eles.Ivalue[index])
		}
		backend[string(out)] = tmp

	}
	return backend
}
