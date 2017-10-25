package main

import (
	"bytes"

	"os"

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

func (this *Store) ele(category byte, key []byte) interface{} {
	hh := NewHh(category)
	it := this.db.NewIterator(this.ro)
	key = mergeTag3(key, blank)
	it.Seek(key)
	defer it.Close()
	for it = it; it.ValidForPrefix(key); it.Next() {
		key := it.Key()
		dkey := key.Data()
		value := it.Value()
		dvalue := value.Data()
		category := dvalue[0]
		if category != tl && category != tm {
			hh.add(dkey, this.Forward(dvalue))
		} else {
			keys := bytes.Split(dvalue[1:], partitionMark)
			hh.addNode(dkey, category, BtI(keys[1]))
		}
		key.Free()
		value.Free()
	}
	return hh.data
}

func (this *Store) loop(start []byte, end []byte, filter map[int][]byte) []interface{} {
	it := this.db.NewIterator(this.ro)
	it.Seek(start)
	defer it.Close()
	hh := []interface{}{}
	for it = it; it.ValidForPrefix(end); it.Next() {
		//key := it.Key()
		//dkey := key.Data()
		value := it.Value()
		dvalue := value.Data()
		//if _, ok := filter[dkey]; ok {
		hh = append(hh, this.Forward(dvalue))

		//}
		//key.Free()
		value.Free()
	}
	return hh
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
		return B2F32(content)
	case tl:
		return this.ele(tl, content)
	case tm:
		return this.ele(tm, content)
	case tn:
		return ""
	}
	return nil
}

func (this *Store) Save(key []byte, element interface{}) bool {
	if cvv, ok := element.([]byte); ok {
		return this.Set(key, cvv)
	} else if sl, ok := element.(SL); ok {
		tmp := NewIndex(sl.category)
		for step := 0; step < len(sl.data); step++ {
			index := sl.index[step]
			this.Save(mergeTag2(key, ItS(index)), sl.data[step])
			tmp.add(sl.old_data[step], index)
		}
		value := bytes.Join([][]byte{mergeTag("l", key), ItB(len(sl.data))}, partitionMark)
		this.Save(key, value)
		//开始插入索引只
		this.Save(mergeTag("tmp", key), tmp.dump())
		return true
	} else if ml, ok := element.(map[string]interface{}); ok {
		for key2, value := range ml {
			this.Save(mergeTag2(key, key2), value)
		}
		return this.Save(key, mergeTag("m", key))
	}
	return false
}
