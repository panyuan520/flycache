package main

import (
	"bytes"
	"net"

	"github.com/vmihailenco/msgpack"
)

type Modelx interface{}

func (this *Server) marshaler(content interface{}) ([]byte, error) {
	if b, err := msgpack.Marshal(content); err == nil {
		return b, nil
	} else {
		return nil, err
	}
}

func (this *Server) OG(conn net.Conn, key []byte) {
	//针对key:value获取
	if content, err := this.store.Get(key); err == nil {
		if marsher, err := this.marshaler(content); err == nil {
			conn.Write(reply(marsher))
		}
	} else {
		conn.Write(reply([]byte("0")))
	}
}

func (this *Server) OS(conn net.Conn, content []byte) {
	//针对key:value设置
	contents := bytes.Split(content, delimit2)
	if len(contents) == 2 {
		var modelx Modelx
		//开始解码数据集
		if err := msgpack.Unmarshal(contents[1], &modelx); err == nil {
			this.store.Save(contents[0], modelx)
			conn.Write(reply([]byte("1")))
			return
		}
	}
	conn.Write(reply([]byte("0")))
}

func (this *Server) OD(conn net.Conn, key []byte) {
	//删除 针对所有
	if content, err := this.store.GetBytes(key); err == nil {
		//开始删除数据
		if err := this.store.Delete(key, content); err == nil {
			conn.Write(reply([]byte("1")))
		} else {
			conn.Write(reply([]byte("0")))
		}
	} else {
		conn.Write(reply([]byte("0")))
	}

}

func (this *Server) OF(conn net.Conn, key []byte) {
	//查询针对排序，键值查询
	// example:
	// sql select key1,key2,key3 from namespace where key1:1, key2:1 ordeyby k1:1
	// key |skey1,key2,k3|ftable1|wkey1?=?1,key2?=?2|okey1?=?1
	query := &Query{store: this.store}
	query.Init(key)
	content := query.Filter()
	if marsher, err := this.marshaler(content); err == nil {
		conn.Write(reply(marsher))
	}

}
