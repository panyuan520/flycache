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
	cache *lru.Cache
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
		lcache, _ := lru.New(cache)
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

func (this *Store) ParseCategory(value []byte) interface{} {
	category := value[0]
	content := value[1:]
	switch category {
	case byte('s'):
		return string(content)
	case byte('i'):
		return BytesToInt64(content)
	case byte('f'):
		return BytesToFloat64(content)
	case byte('l'):
		return this.RangeElement(content)
	case byte('m'):
		return this.RangeKeyElement(content)
	case byte('n'):
		return ""
	}
	return nil
}

func (this *Store) RangeElement(key []byte) Eles {
	l := Eles{}
	keys := bytes.Split(key, partitionMark)
	for _, key := range keys {
		if content, err := this.GetBytes(key); err == nil {
			value4 := this.ParseCategory(content)
			l = append(l, value4)
		}
	}
	return l
}

func (this *Store) RangeKeyElement(key []byte) interface{} {
	m := make(map[string]interface{})
	keys := bytes.Split(key, partitionMark)
	for _, key := range keys {
		if content, err := this.GetBytes(key); err == nil {
			keys := bytes.Split(key, delimiter)
			if len(keys) > 0 {
				value4 := this.ParseCategory(content)
				m[string(keys[len(keys)-1])] = value4
			}
		}
	}
	return m
}

func (this *Store) Save(key []byte, element interface{}) {
	v := reflect.ValueOf(element)
	switch v.Kind() {
	case reflect.Bool:
		this.XSet(key, strconv.FormatBool(v.Bool()), "b")
	case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int64:
		this.XSet(key, strconv.FormatInt(v.Int(), 10), "i")
	case reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64:
		this.XSet(key, strconv.FormatUint(v.Uint(), 10), "i")
	case reflect.Float32, reflect.Float64:
		this.XSet(key, strconv.FormatFloat(v.Float(), 'E', -1, 64), "f")
	case reflect.String:
		this.XSet(key, v.String(), "s")
	case reflect.Slice, reflect.Array:
		this.LAdd(key, v)
	case reflect.Map:
		this.MAdd(key, v)
	default:
		fmt.Println(v.Kind(), "type not fund")
	}
}
