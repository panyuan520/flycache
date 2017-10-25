package main

import (
	"bytes"
	"math"
)

type Loo struct {
	index int
	data  []byte
}

type Loop struct {
	left          []byte
	exclude_left  bool
	right         []byte
	exclude_right bool
	only          []byte
	exclude_only  bool
	tmp           map[int]Loo
}

func (this *Loop) add(tip []byte, value []byte) {
	if compare(tip, equal, greater) {
		this.left = value
		this.exclude_left = false
	} else if compare(tip, equal, greaterEqual) {
		this.left = value
		this.exclude_left = true
	} else if compare(tip, equal, less) {
		this.right = value
		this.exclude_right = false
	} else if compare(tip, equal, lessEqual) {
		this.right = value
		this.exclude_right = true
	}
}

func (this *Loop) split(data [][]byte, index int) ([]byte, int) {
	if index == -1 {
		return blank, -1
	}
	if item, ok := this.tmp[index]; ok {
		return item.data, item.index
	}
	value := data[index]
	indexs := bytes.Split(value, delimiter)
	if len(indexs) > 1 {
		return indexs[0], BtI(indexs[1])
	}
	return value, -1
}

func (this *Loop) jit(x []int, y []int) (int, int) {
	if len(x) > 0 && len(y) > 0 {
		return x[len(x)-1], y[len(y)-1]
	} else if len(x) > 0 {
		return x[len(x)-1], x[0]
	} else if len(y) > 0 {
		return y[0], y[len(y)-1]
	}
	return -1, -1
}

func (this *Loop) search(category byte, data [][]byte, where Where) Where {
	x, y, count := -2, -2, len(data)
	if this.only != nil {
		if value, index := this.split(data, 0); index != -1 && compare(this.only, less, value) {
			x, y = -1, -1
		} else if value, index := this.split(data, count-1); index != -1 && compare(this.only, greater, value) {
			x, y = -1, -1
		} else {
			_, xy1, xy2 := this.btree(data, this.only)
			x, y = this.jit(xy1, xy2)
		}
	} else {
		if this.left != nil {
			if value, index := this.split(data, 0); index != -1 && compare(value, greater, this.left) {
				x = 0
			}
			if value, index := this.split(data, count-1); index != -1 && compare(value, less, this.left) {
				x, y = -1, -1
			}
			if x == -2 {
				xt, xy1, xy2 := this.btree(data, this.left)
				if x1, y1 := this.jit(xy1, xy2); x1 != -1 && y1 != -1 {
					if this.exclude_left {
						xt = x1
					} else {
						xt = y1 + 1
					}
				}
				x = xt
			}
		}

		if this.right != nil {
			if value, index := this.split(data, count-1); index != -1 && compare(value, less, this.right) {
				y = count
			}
			if value, index := this.split(data, 0); index != -1 && compare(value, greater, this.right) {
				x, y = -1, -1
			}
			if y == -2 {
				yt, xy1, xy2 := this.btree(data, this.right)
				if x1, y1 := this.jit(xy1, xy2); x1 != -1 && y1 != -1 {
					if this.exclude_right {
						yt = y1
					} else {
						yt = x1 - 1
					}
				}
				y = yt
			}
		}
	}
	tmp := map[int][]byte{}
	for i := x; i <= y; i++ {
		if value, index := this.split(data, i); index != -1 {
			if where.min == -1 {
				where.min = index
			}
			if where.max == -1 {
				where.max = index
			}
			if where.min > index {
				where.min = index
			}
			if where.max < index {
				where.max = index
			}
			if !where.lock {
				tmp[index] = value
			} else {
				if _, ok := where.index[index]; !ok {
					tmp[index] = value
				}
			}

		}
	}
	where.index = tmp
	where.lock = true
	return where
}

func (this *Loop) btree(data [][]byte, pt []byte) (int, []int, []int) {
	left, right, mid := 1, len(data), 0
	//比较最后和第一个
	for {
		// mid向下取整
		mid = int(math.Floor(float64((left + right) / 2)))
		if value1, index1 := this.split(data, mid-1); index1 != -1 {
			if value2, index2 := this.split(data, mid+1); index2 != -1 {
				if compare(pt, greaterEqual, value1) && compare(pt, lessEqual, value2) {
					break
				}
			}
		}
		if value, index := this.split(data, mid); index != -1 {
			if compare(value, greater, pt) {
				// 如果当前元素大于k，那么把right指针移到mid - 1的位置
				right = mid - 1
			} else if compare(value, less, pt) {
				// 如果当前元素小于k，那么把left指针移到mid + 1的位置
				left = mid + 1
			}
		}
		// 判断如果left大于right，那么这个元素是不存在的。返回-1并且退出循环
		if left > right {
			mid = -1
			break
		}
	}

	count := len(data)
	equals1, equals2 := []int{}, []int{}
	// 输入元素的下标向前推
	for i := mid; i >= 0; i-- {
		if value, index := this.split(data, i); index != -1 && compare(value, equal, pt) {
			equals1 = append(equals1, i)
		} else {
			break
		}
	}
	//输入元素的下标向后推
	for i := mid + 1; i < count; i++ {
		if value, index := this.split(data, i); index != -1 && compare(value, equal, pt) {
			equals2 = append(equals2, i)
		} else {
			break
		}
	}
	return mid, equals1, equals2
}

func NewLoop() Loop {
	return Loop{left: []byte{}, exclude_left: false, right: []byte{}, exclude_right: false, tmp: map[int]Loo{}}
}

type Where struct {
	data  map[string]Loop
	index map[int][]byte
	max   int
	min   int
	lock  bool
}

func (this Where) add(column, tip, value []byte) {
	scolumn := string(column)
	if loop, ok := this.data[scolumn]; ok {
		loop.add(tip, value)
		this.data[scolumn] = loop
	} else {
		loop := NewLoop()
		loop.add(tip, value)
		this.data[scolumn] = loop
	}
}

func NewWhere() Where {
	return Where{data: map[string]Loop{}, index: map[int][]byte{}, min: -1, max: -1, lock: false}
}

type Order struct {
	column []byte //字段
	sort   bool   //true or false
}

type Query struct {
	table  []byte   //表
	output [][]byte //输出
	where  Where    //范围查找
	order  []Order  //排序
	store  *Store   //查询集合
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
			this.table = content
		case byte('w'):
			where := NewWhere()
			seqs := bytes.Split(content, comma)
			for _, seq := range seqs {
				wheres := bytes.Split(seq, questionMark)
				if len(wheres) > 2 {
					column := mergeTag5(this.table, wheres[0])
					where.add(column, wheres[1], wheres[2])
				}
			}
			this.where = where
		case byte('o'):
			seqs := bytes.Split(content, comma)
			for _, seq := range seqs {
				orders := bytes.Split(seq, questionMark)
				if len(orders) > 1 {
					prefix := mergeTag5(this.table, orders[0])
					if compare(orders[1], equal, zero) {
						this.order = append(this.order, Order{column: prefix, sort: false})
					} else {
						this.order = append(this.order, Order{column: prefix, sort: true})
					}

				}
			}
		}
	}
}

func (this *Query) sort(sorts [][]byte, step int) []byte {
	if len(sorts) < step {
		return blank
	}
	return sorts[step]
}

func (this *Query) index(key []byte) (interface{}, bool) {
	indexs := bytes.Split(key, partitionMark)
	if len(indexs) > 2 {
		children := indexs[2]
		return bytes.Split(children, comma), true
	}
	return nil, false
}

func (this *Query) tmp(key []byte) (byte, [][]byte) {
	tkey := mergeTag("tmp", key)
	if content, err := this.store.GetBytes(tkey); err == nil && len(content) > 0 {
		return content[0], bytes.Split(content[1:], partitionMark)
	}
	return tf, nil
}

func (this *Query) split(value []byte) (byte, []byte) {
	category := value[0]
	columns := bytes.Split(value[1:], partitionMark)
	if len(columns) > 0 {
		return category, columns[0]
	}
	return category, nil
}

func (this *Query) Filter() interface{} {
	backend := map[string]interface{}{}
	//开始范围查找
	if len(this.where.data) > 0 {
		for column, symbols := range this.where.data {
			if category, tmp := this.tmp([]byte(column)); tmp != nil && len(tmp) > 0 {
				this.where = symbols.search(category, tmp, this.where)
			}
		}
	}
	//开始排序
	sorts := []int{}
	if len(this.order) > 0 {
		for _, order := range this.order {
			if _, tmp := this.tmp(order.column); tmp != nil && len(tmp) > 0 {
				for _, value := range tmp {
					if indexs := bytes.Split(value, delimiter); len(indexs) > 1 {
						index := BtI(indexs[1])
						if _, ok := this.where.index[index]; ok {
							sorts = append(sorts, index)
						}
					}
				}
			}
		}
	}

	if len(this.output) > 0 {
		for _, column := range this.output {
			tmp := []interface{}{}
			prefix := mergeTag5(this.table, column)
			for _, step := range sorts {
				key := mergeTag5(prefix, ItB(step))
				if value, err := this.store.GetBytes(key); err == nil {
					tmp = append(tmp, this.store.Forward(value))
				}
			}
			backend[string(column)] = tmp
		}
	}
	return backend
}
