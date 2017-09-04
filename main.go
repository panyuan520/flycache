package main

import (
	"flag"
	"log"
	"runtime"

	gcfg "gopkg.in/gcfg.v1"
)

type Config struct {
	Server struct {
		Bind string
		Port string
	}
	Database struct {
		Db       int
		Path     string
		Compress string
		Cache    int
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var name string
	flag.StringVar(&name, "conf", "rocks.conf", "rocksdb config file")
	flag.Parse()

	var conf Config
	err := gcfg.ReadFileInto(&conf, name)
	if err != nil {
		log.Fatal(err)
	}

	if s, err := NewServer(conf); err != nil {
		log.Fatal(err)
	} else {
		s.ListenServer()
	}

}
