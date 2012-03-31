// server ...
package main

import (
	"./cache"
	"./cache/trieCache"
	"./protocol"
	"io"
	"log"
	"net"
	"os"
	"time"
)

func main() {
	var logger *log.Logger
	port := "22122"
	if len(os.Args) > 1 && os.Args[1] == "-d" {
		logger = log.New(os.Stdout, "server ", log.LstdFlags)
		if len(os.Args) > 2 {
			port = os.Args[2]
		}
	} else if len(os.Args) > 1 {
		port = os.Args[1]
	}
	c := trieCache.New()
	go expirer(c)
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err.Error())
	}
	for {
		conn, err := listener.Accept()
		if conn == nil {
			panic(err.Error())
		}
		go handler(logger, c, conn)
	}
}

func expirer(c cache.Cache) {
	for {
		time.Sleep(15000000000)
		c.Expire(cache.Now())
	}
}

func handler(logger *log.Logger, c cache.Cache, conn net.Conn) {
	header := make([]byte, 24)
	for {
		packet, err := protocol.ReadPacket(logger, conn, protocol.MagicRequest, header)
		t := cache.Now()
		if err != nil {
			if err != io.EOF {
				logError(logger, err)
			}
			conn.Close()
			return
		}
		var status uint16 = protocol.StatusInternalError
		var version cache.Version = 0
		var extras, key, value []byte = nil, nil, errorValue
		switch packet.Cmd() {
		case protocol.CmdGet:
			var flags uint32
			value, flags, version, err = c.Get(packet.Key, t)
			switch err {
			case nil:
				status = protocol.StatusNoError
				extras = protocol.MakeFlags(flags)
			case cache.NotFound:
				status = protocol.StatusKeyNotFound
				value = errorValue
			default:
				value = errorValue
			}
		case protocol.CmdSet:
			exp := cache.Future(time.Duration(packet.Expiry()))
			version, err = c.Set(packet.Key, packet.Value, packet.Flags(), exp, t)
			switch err {
			case nil:
				status = protocol.StatusNoError
				value = nil
			}
		case protocol.CmdAdd:
			exp := cache.Future(time.Duration(packet.Expiry()))
			version, err = c.Add(packet.Key, packet.Value, packet.Flags(), exp, t)
			switch err {
			case nil:
				status = protocol.StatusNoError
				value = nil
			case cache.EntryExists:
				status = protocol.StatusKeyExists
			}
		case protocol.CmdReplace:
			exp := cache.Future(time.Duration(packet.Expiry()))
			version, err = c.Replace(packet.Key, packet.Value, packet.Flags(), cache.Version(packet.Version()), exp, t)
			switch err {
			case nil:
				status = protocol.StatusNoError
				value = nil
			case cache.NotFound:
				status = protocol.StatusKeyNotFound
			case cache.VersionMismatch:
				status = protocol.StatusItemNotStored
			}
		case protocol.CmdDelete:
			err = c.Delete(packet.Key, cache.Version(packet.Version()), t)
			switch err {
			case nil:
				status = protocol.StatusNoError
				value = nil
			case cache.NotFound:
				status = protocol.StatusKeyNotFound
			case cache.VersionMismatch:
				status = protocol.StatusItemNotStored
			}
		case protocol.CmdQuit:
			protocol.WriteResponse(logger, conn, packet, protocol.StatusNoError, 0, nil, nil, nil)
			conn.Close()
			return
		}
		err = protocol.WriteResponse(logger, conn, packet, status, uint64(version), extras, key, value)
		if err != nil {
			logError(logger, err)
			conn.Close()
			return
		}
	}
}

func logError(logger *log.Logger, err error) {
	if logger != nil {
		logger.Println(err.Error())
	}
}

var errorValue = toBytes("Error")

func toBytes(str string) []byte {
	bytes := make([]byte, len(str))
	copy(bytes, str)
	return bytes
}
