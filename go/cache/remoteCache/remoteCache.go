// Package remoteCache provides access to a remote cache using
// subset of the memcache protocol.
package remoteCache

import (
	"../../cache"
	"../../protocol"
	"log"
	"net"
)

type remoteCache chan *remoteRequest

type remoteRequest struct {
	cmd                        byte
	opaque                     uint32
	version                    uint64
	header, extras, key, value []byte
	response                   chan *protocol.Packet
}

// New returns a Cache that connects to addr.
// logger, if non-nil, logs the traffic to and from the remote cache.
func New(logger *log.Logger, addr string) (cache.Cache, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := remoteCache(make(chan *remoteRequest))
	go remoteHandler(logger, c, conn)
	return c, nil
}

func remoteHandler(logger *log.Logger, c remoteCache, conn net.Conn) {
	for {
		req := <-c
		err := protocol.WriteRequest(logger, conn, req.cmd, req.opaque, req.version, req.header, req.extras, req.key, req.value)
		if err != nil {
			req.response <- nil
			continue
		}
		packet, _ := protocol.ReadPacket(logger, conn, protocol.MagicResponse, req.header)
		req.response <- packet
	}
}

func (c remoteCache) Get(key []byte, time cache.Timestamp) ([]byte, uint32, cache.Version, error) {
	req := &remoteRequest{
		cmd:      protocol.CmdGet,
		header:   make([]byte, 24),
		key:      key,
		response: make(chan *protocol.Packet),
	}
	c <- req
	switch packet := <-req.response; {
	case packet == nil:
		break
	case packet.Status() == protocol.StatusNoError:
		return packet.Value, packet.Flags(), cache.Version(packet.Version()), nil
	case packet.Status() == protocol.StatusKeyNotFound:
		return nil, 0, 0, cache.NotFound
	}
	return nil, 0, 0, cache.UnknownError
}

func (c remoteCache) Set(key, data []byte, flags uint32, expires, time cache.Timestamp) (cache.Version, error) {
	req := &remoteRequest{
		cmd:      protocol.CmdSet,
		header:   make([]byte, 24),
		extras:   protocol.MakeFlagsExpiry(flags, uint64(expires), uint64(time)),
		key:      key,
		value:    data,
		response: make(chan *protocol.Packet),
	}
	c <- req
	switch packet := <-req.response; {
	case packet == nil:
		break
	case packet.Status() == protocol.StatusNoError:
		return cache.Version(packet.Version()), nil
	}
	return 0, cache.UnknownError
}

func (c remoteCache) Add(key, data []byte, flags uint32, expires, time cache.Timestamp) (cache.Version, error) {
	req := &remoteRequest{
		cmd:      protocol.CmdAdd,
		header:   make([]byte, 24),
		extras:   protocol.MakeFlagsExpiry(flags, uint64(expires), uint64(time)),
		key:      key,
		value:    data,
		response: make(chan *protocol.Packet),
	}
	c <- req
	switch packet := <-req.response; {
	case packet == nil:
		break
	case packet.Status() == protocol.StatusNoError:
		return cache.Version(packet.Version()), nil
	case packet.Status() == protocol.StatusKeyExists:
		return 0, cache.EntryExists
	}
	return 0, cache.UnknownError
}

func (c remoteCache) Replace(key, data []byte, flags uint32, version cache.Version, expires, time cache.Timestamp) (cache.Version, error) {
	req := &remoteRequest{
		cmd:      protocol.CmdReplace,
		version:  uint64(version),
		header:   make([]byte, 24),
		extras:   protocol.MakeFlagsExpiry(flags, uint64(expires), uint64(time)),
		key:      key,
		value:    data,
		response: make(chan *protocol.Packet),
	}
	c <- req
	switch packet := <-req.response; {
	case packet == nil:
		break
	case packet.Status() == protocol.StatusNoError:
		return cache.Version(packet.Version()), nil
	case packet.Status() == protocol.StatusKeyNotFound:
		return 0, cache.NotFound
	case packet.Status() == protocol.StatusItemNotStored:
		return 0, cache.VersionMismatch
	}
	return 0, cache.UnknownError
}

func (c remoteCache) Delete(key []byte, version cache.Version, time cache.Timestamp) error {
	req := &remoteRequest{
		cmd:      protocol.CmdDelete,
		version:  uint64(version),
		header:   make([]byte, 24),
		key:      key,
		response: make(chan *protocol.Packet),
	}
	c <- req
	switch packet := <-req.response; {
	case packet == nil:
		break
	case packet.Status() == protocol.StatusNoError:
		return nil
	case packet.Status() == protocol.StatusKeyNotFound:
		return cache.NotFound
	case packet.Status() == protocol.StatusItemNotStored:
		return cache.VersionMismatch
	}
	return cache.UnknownError
}

func (c remoteCache) Touch(key []byte, expires cache.Timestamp) error {
	return cache.UnknownError
}

func (c remoteCache) Expire(time cache.Timestamp) {
}
