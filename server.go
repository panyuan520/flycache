package main

import (
	"bufio"
	//"bytes"
	"fmt"
	"log"
	"net"
	"reflect"
	"strings"
)

type HandlerFunc func(net.Conn, []byte)

type Server struct {
	conf  Config
	store *Store
	cmds  map[byte]HandlerFunc
}

func (this *Server) ListenServer() {
	listen, err := net.Listen("tcp", this.conf.Server.Bind+":"+this.conf.Server.Port)
	if err != nil {
		log.Fatal("tcp server is no start.")
	}
	defer listen.Close()

	for {
		conn, _ := listen.Accept()
		go this.recvCommand(conn)

	}
}

func (this *Server) handle(conn net.Conn, message []byte) {
	cmd := message[0]
	cmdFunc, ok := this.cmds[cmd]
	if !ok {
		conn.Write(reply([]byte("bad command")))
	} else {
		cmdFunc(conn, message[1:])
	}
}

func (this *Server) recvCommand(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	scanner.Split(crunchSplitFunc)
	for scanner.Scan() {
		content := scanner.Bytes()
		this.handle(conn, content)
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("recvComand", err)
	}

}

func (this *Server) registerCommand() {
	this.cmds = make(map[byte]HandlerFunc)
	otypes := reflect.TypeOf(this)
	ovalues := reflect.ValueOf(this)
	for i := 0; i < otypes.NumMethod(); i++ {
		name := otypes.Method(i).Name
		if strings.HasPrefix(name, "O") {
			names := []byte(name)
			func(names []byte, method reflect.Value) {
				this.cmds[names[1]] = HandlerFunc(func(conn net.Conn, c []byte) {
					in := []reflect.Value{reflect.ValueOf(conn), reflect.ValueOf(c)}
					method.Call(in)
				})
			}(names, ovalues.Method(i))
		}
	}

}

func NewServer(conf Config) (*Server, error) {
	if store, err := NewStore(conf.Database.Path, conf.Database.Cache, conf.Database.Compress); err == nil {
		s := &Server{conf: conf, store: store}
		s.registerCommand()
		return s, nil
	} else {
		return nil, err
	}

}
