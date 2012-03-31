package main

import (
	"./cache"
	"./cache/remoteCache"
	"./cache/trieCache"
	"fmt"
	"log"
	"os"
	"time"
)

// test ...
func main() {
	var caches []cache.Cache
	var remote string
	var logger *log.Logger
	nthreads := 4
	if len(os.Args) == 4 && os.Args[1] == "-d" {
		remote = os.Args[2] + ":" + os.Args[3]
		logger = log.New(os.Stdout, "test ", log.LstdFlags)
	} else if len(os.Args) == 3 {
		remote = os.Args[1] + ":" + os.Args[2]
	}
	if remote == "" {
		caches = []cache.Cache{trieCache.New()}
	} else {
		cache1, err := remoteCache.New(logger, remote)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		cache2, err := remoteCache.New(logger, remote)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		caches = []cache.Cache{cache1, cache2}
	}
	done := make(chan bool)
	for i := 0; i < nthreads; i++ {
		go test1(done, caches[i%len(caches)], i)
	}
	for i := 0; i < nthreads; i++ {
		<-done
	}
}

func test1(done chan bool, c cache.Cache, i int) {
	key1 := toBytes(fmt.Sprintf("key%d", i%2+1))
	key2 := toBytes(fmt.Sprintf("key%d", (i+1)%2+1))
	value1 := toBytes(fmt.Sprintf("value%d", i*2+1))
	value2 := toBytes(fmt.Sprintf("value%d", i*2+2))
	tag := fmt.Sprintf("%d", i+1)
	t := cache.Now()
	exp := cache.Future(5 * time.Second)
	value, flags, version, err := c.Get(key1, t)
	println(tag, "Get", string(key1), "->", string(value), flags, version, errStr(err))
	version, err = c.Set(key1, value1, 0, exp, t)
	println(tag, "Set", string(key1), string(value1), "->", version, errStr(err))
	err = c.Delete(key1, version, t)
	println(tag, "Delete", string(key1), "->", errStr(err))
	version, err = c.Add(key1, value1, 0, exp, t)
	println(tag, "Add", string(key1), string(value1), "->", version, errStr(err))
	version, err = c.Replace(key1, value2, 0, version, exp, t)
	println(tag, "Replace", string(key1), string(value1), "->", version, errStr(err))
	value, flags, version, err = c.Get(key2, t)
	println(tag, "Get", string(key2), "->", string(value), flags, version, errStr(err))
	version, err = c.Replace(key2, value2, 0, version, exp, t)
	println(tag, "Replace", string(key2), string(value2), "->", version, errStr(err))
	done <- true
}

func errStr(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}

func toBytes(str string) []byte {
	bytes := make([]byte, len(str))
	copy(bytes, str)
	return bytes
}
