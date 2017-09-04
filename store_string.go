package main

import (
	"fmt"
)

func (this *Store) GetBytes(key []byte) ([]byte, error) {
	return this.db.GetBytes(this.ro, key)
}

func (this *Store) Get(key []byte) (interface{}, error) {
	skey := string(key)
	if content, ok := this.cache.Get(skey); ok {
		return content, nil
	}
	if value, error := this.db.GetBytes(this.ro, key); error == nil {
		if len(value) > 0 {
			content := this.ParseCategory(value)
			this.cache.Add(skey, content)
			return content, nil
		}
	}
	return nil, nil
}

func (this *Store) Set(key []byte, val []byte) error {
	return this.db.Put(this.wo, key, val)
}

func (this *Store) XSet(key []byte, content string, tip string) {
	tips := []byte(tip)
	if len(content) == 0 {
		tips = []byte("n")
	}
	contents := []byte(content)
	tips = append(tips, contents...)
	if err := this.Set(key, tips); err != nil {
		fmt.Println(err)
	}
}

func (this *Store) DeleteRange(key []byte) {
	it := this.db.NewIterator(this.ro)
	defer it.Close()
	it.Seek(key)
	for it = it; it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()
		this.Delete(key.Data(), value.Data())
		value.Free()
		value.Free()
	}
}

func (this *Store) Delete(key []byte, value []byte) error {
	tip := value[0]
	switch tip {
	case byte('i'), byte('s'), byte('f'), byte('b'):
		return this.db.Delete(this.wo, key)
	case byte('l'), byte('m'):
		this.db.Delete(this.wo, key)
		this.DeleteRange(value[1:])
	}
	this.cache.Remove(string(key))
	return nil
}
