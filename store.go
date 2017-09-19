package main

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"strconv"

	lru "github.com/hashicorp/golang-lru"
	db "github.com/tecbot/gorocksdb"
)

type Store struct {
	ro    *db.ReadOptions
	wo    *db.WriteOptions
	db    *db.DB
	cache *lru.ARCCache
}

func NewStore(path string, cache int, compress string) (*Store, error) {
	opts := db.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetTargetFileSizeBase(16 * 1024 * 1024)

	switch compress {
	case "snappy":
		opts.SetCompression(db.SnappyCompression)
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err = os.MkdirAll(path, 0700); err != nil {
			return nil, err
		}
	}

	if rockdb, err := db.OpenDb(opts, path); err == nil {
		lcache, _ := lru.NewARC(cache)
		return &Store{
			ro:    db.NewDefaultReadOptions(),
			wo:    db.NewDefaultWriteOptions(),
			db:    rockdb,
			cache: lcache,
		}, nil
	} else {
		return nil, err
	}
}

func (this *Store) RangePrefix(key []byte) Eles {
	eles := NewEles()
	this.Iter(key, eles.Add)
	return eles
}

func (this *Store) RangeElement(key []byte) Lt {
	l := NewLt()
	this.Ele(key, l.Add)
	return l
}

func (this *Store) RangeKeyElement(key []byte) Hh {
	m := NewHh()
	this.Ele(key, m.Add)
	return m
}

type RangeCall func(index string, value interface{})

func (this *Store) Ele(key []byte, fn RangeCall) {
	keys := bytes.Split(key, partitionMark)
	for _, key := range keys {
		if content, err := this.GetBytes(key); err == nil {
			if index := LastIndex(key); len(index) > 0 {
				fn(index, this.Forward(content))
			}
		}
	}
}

type IterCall func(index string, bvalue []byte, ivalue interface{})

func (this *Store) Iter(key []byte, fn IterCall) {
	keys := bytes.Split(key, partitionMark)
	for _, key := range keys {
		if content, err := this.GetBytes(key); err == nil {
			if index := LastIndex(key); len(index) > 0 {
				fn(index, content[1:], this.Forward(content))
			}
		}
	}
}

func (this *Store) Forward(value []byte) interface{} {
	if len(value) < 2 {
		return nil
	}
	category := value[0]
	content := value[1:]
	switch category {
	case ts:
		return string(content)
	case ti:
		return BtI(content)
	case tf:
		return BtF(content)
	case tl:
		return this.RangeElement(content)
	case tm:
		return this.RangeKeyElement(content)
	case tn:
		return ""
	}
	return nil
}

func (this *Store) Save(key []byte, element interface{}) {
	v := reflect.ValueOf(element)
	switch v.Kind() {
	case reflect.Bool:
		this.XSet(key, strconv.FormatBool(v.Bool()), tb)
	case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int64:
		this.XSet(key, strconv.FormatInt(v.Int(), 10), ti)
	case reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64:
		this.XSet(key, strconv.FormatUint(v.Uint(), 10), ti)
	case reflect.Float32, reflect.Float64:
		this.XSet(key, strconv.FormatFloat(v.Float(), 'E', -1, 64), tf)
	case reflect.String:
		this.XSet(key, v.String(), ts)
	case reflect.Slice, reflect.Array:
		this.LAdd(key, v)
	case reflect.Map:
		this.MAdd(key, v)
	default:
		fmt.Println(v.Kind(), "type not fund")
	}
}
