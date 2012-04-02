package main

import (
	"./cache"
	"./cache/remoteCache"
	"./cache/trieCache"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {
	nthreads, nconnections, remote, logger := parseArgs(os.Args)
	var caches []cache.Cache
	if remote == "" {
		caches = []cache.Cache{trieCache.New()}
	} else {
		caches = make([]cache.Cache, nconnections)
		for i, _ := range caches {
			c, err := remoteCache.New(logger, remote)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
			caches[i] = c
		}
	}
	done := make(chan bool)
	for i := 0; i < nthreads; i++ {
		go test1(done, caches[i%len(caches)], i)
	}
	for i := 0; i < nthreads; i++ {
		<-done
	}
}

func parseArgs(args []string) (int, int, string, *log.Logger) {
	var logger *log.Logger
	var nthreads, nconnections int = 4, 2
	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "-d":
			logger = log.New(os.Stdout, "test ", log.LstdFlags)
		case "-t":
			i++
			if i < len(args) {
				var err error
				nthreads, err = strconv.Atoi(args[i])
				if err != nil {
					fmt.Fprintln(os.Stderr, err.Error())
					os.Exit(1)
				}
			}
		case "-c":
			i++
			if i < len(args) {
				var err error
				nconnections, err = strconv.Atoi(args[i])
				if err != nil {
					fmt.Fprintln(os.Stderr, err.Error())
					os.Exit(1)
				}
			}
		default:
			i++
			if i < len(args) {
				return nthreads, nconnections, args[i-1] + ":" + args[i], logger
			}
		}
	}
	return nthreads, nconnections, "", logger
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
