// Package protocol supports a subset of the memcache protocol.
package protocol

import (
	"errors"
	"io"
	"log"
	"time"
)

const (
	// Magic byte values.
	MagicRequest  = 0x80
	MagicResponse = 0x81

	// Response status values.
	StatusNoError       = 0
	StatusKeyNotFound   = 1
	StatusKeyExists     = 2
	StatusItemNotStored = 5
	StatusNotSupported  = 131
	StatusInternalError = 132

	// Command opcode byte values.
	CmdGet     = 0
	CmdSet     = 1
	CmdAdd     = 2
	CmdReplace = 3
	CmdDelete  = 4
	CmdQuit    = 7
)

var BadMagic = errors.New("Bad magic")

// Packet represents a packet in the memcache protocol.
type Packet struct {
	header             []byte
	Extras, Key, Value []byte
}

// ReadPacket reads a packet in the memcache protocol.  magic is the
// magic byte to expect.  If header is large enough to contain the
// packet header (24 bytes), it will be used to hold the header.
func ReadPacket(logger *log.Logger, reader io.Reader, magic byte, header []byte) (*Packet, error) {
	if header == nil || len(header) < 24 {
		header = make([]byte, 24)
	} else {
		header = header[:24]
	}
	err := readChunk(logger, reader, header)
	if err != nil {
		return nil, err
	}
	if header[0] != magic {
		return nil, BadMagic
	}
	packet := &Packet{header: header}
	extrasLen := int(header[4])
	keyLen := int(getUint16(header, 2))
	totalLen := int(getUint32(header, 8))
	valueLen := totalLen - extrasLen - keyLen
	if extrasLen > 0 {
		packet.Extras = make([]byte, extrasLen)
		err = readChunk(logger, reader, packet.Extras)
		if err != nil {
			return nil, err
		}
	}
	if keyLen > 0 {
		packet.Key = make([]byte, keyLen)
		err = readChunk(logger, reader, packet.Key)
		if err != nil {
			return nil, err
		}
	}
	if valueLen > 0 {
		packet.Value = make([]byte, valueLen)
		err = readChunk(logger, reader, packet.Value)
		if err != nil {
			return nil, err
		}
	}
	return packet, nil
}

// WriteRequest writes a request packet in the memcache protocol.
func WriteRequest(logger *log.Logger, writer io.Writer, cmd byte, opaque uint32, version uint64, header, extras, key, value []byte) error {
	if header == nil || len(header) < 24 {
		header = make([]byte, 24)
	} else {
		header = header[:24]
	}
	header[0] = MagicRequest
	header[1] = cmd
	setUint16(header, 2, uint16(len(key)))
	header[4] = uint8(len(extras))
	header[5] = 0
	header[6] = 0
	header[7] = 0
	setUint32(header, 8, uint32(len(key)+len(extras)+len(value)))
	setUint32(header, 12, opaque)
	setUint64(header, 16, version)
	logChunk(logger, ">", header)
	_, err := writer.Write(header)
	if err != nil {
		return err
	}
	logChunk(logger, ">", extras)
	_, err = writer.Write(extras)
	if err != nil {
		return err
	}
	logChunk(logger, ">", key)
	_, err = writer.Write(key)
	if err != nil {
		return err
	}
	logChunk(logger, ">", value)
	_, err = writer.Write(value)
	return err
}

// WriteResponse writes a response packet in the memcache protocol.
// packet is the request packet.
func WriteResponse(logger *log.Logger, writer io.Writer, packet *Packet, status uint16, version uint64, extras, key, value []byte) error {
	packet.header[0] = MagicResponse
	setUint16(packet.header, 2, uint16(len(key)))
	packet.header[4] = uint8(len(extras))
	packet.header[5] = 0
	setUint16(packet.header, 6, status)
	setUint32(packet.header, 8, uint32(len(key)+len(extras)+len(value)))
	setUint64(packet.header, 16, version)
	logChunk(logger, ">", packet.header)
	_, err := writer.Write(packet.header)
	if err != nil {
		return err
	}
	logChunk(logger, ">", extras)
	_, err = writer.Write(extras)
	if err != nil {
		return err
	}
	logChunk(logger, ">", key)
	_, err = writer.Write(key)
	if err != nil {
		return err
	}
	logChunk(logger, ">", value)
	_, err = writer.Write(value)
	return err
}

func readChunk(logger *log.Logger, reader io.Reader, bytes []byte) error {
	for {
		n, err := reader.Read(bytes)
		if err != nil {
			return err
		}
		logChunk(logger, "<", bytes[:n])
		if n >= len(bytes) {
			return nil
		}
		bytes = bytes[n:]
	}
	return nil
}

func logChunk(logger *log.Logger, prefix string, bytes []byte) {
	if logger == nil {
		return
	}
	for i := 0; i < len(bytes); i += 16 {
		switch (len(bytes) - i) / 4 {
		case 0:
			logger.Printf("%s %x", prefix, bytes[i:])
		case 1:
			logger.Printf("%s %x %x", prefix, bytes[i:i+4], bytes[i+4:])
		case 2:
			logger.Printf("%s %x %x %x", prefix, bytes[i:i+4], bytes[i+4:i+8], bytes[i+8:])
		case 3:
			logger.Printf("%s %x %x %x %x", prefix, bytes[i:i+4], bytes[i+4:i+8], bytes[i+8:i+12], bytes[i+12:])
		default:
			logger.Printf("%s %x %x %x %x", prefix, bytes[i:i+4], bytes[i+4:i+8], bytes[i+8:i+12], bytes[i+12:i+16])
		}
	}
}

func getUint16(bytes []byte, index int) uint16 {
	return uint16(bytes[index])<<8 + uint16(bytes[index+1])
}

func getUint32(bytes []byte, index int) uint32 {
	return uint32(bytes[index])<<24 + uint32(bytes[index+1])<<16 + uint32(bytes[index+2])<<8 + uint32(bytes[index+3])
}

func getUint64(bytes []byte, index int) uint64 {
	return uint64(bytes[index])<<56 + uint64(bytes[index+1])<<48 + uint64(bytes[index+2])<<40 + uint64(bytes[index+3])<<32 + uint64(bytes[index+4])<<24 + uint64(bytes[index+5])<<16 + uint64(bytes[index+6])<<8 + uint64(bytes[index+7])
}

func setUint16(bytes []byte, index int, value uint16) {
	bytes[index] = byte(value >> 8)
	bytes[index+1] = byte(value)
}

func setUint32(bytes []byte, index int, value uint32) {
	bytes[index] = byte(value >> 24)
	bytes[index+1] = byte(value >> 16)
	bytes[index+2] = byte(value >> 8)
	bytes[index+3] = byte(value)
}

func setUint64(bytes []byte, index int, value uint64) {
	bytes[index] = byte(value >> 56)
	bytes[index+1] = byte(value >> 48)
	bytes[index+2] = byte(value >> 40)
	bytes[index+3] = byte(value >> 32)
	bytes[index+4] = byte(value >> 24)
	bytes[index+5] = byte(value >> 16)
	bytes[index+6] = byte(value >> 8)
	bytes[index+7] = byte(value)
}

// Cmd returns the command opcode byte.
func (packet *Packet) Cmd() byte {
	return packet.header[1]
}

// Status returns the response status.
func (packet *Packet) Status() uint16 {
	return getUint16(packet.header, 6)
}

// Opaque returns the opaque value.
func (packet *Packet) Opaque() uint32 {
	return getUint32(packet.header, 12)
}

// Version returns the data version check or CAS.
func (packet *Packet) Version() uint64 {
	return getUint64(packet.header, 16)
}

// Flags returns the flags, assuming the packet includes the flags.
func (packet *Packet) Flags() uint32 {
	return getUint32(packet.Extras, 0)
}

// Expiry returns the time to expiration in nanoseconds, assuming the
// packet includes the expiry.
func (packet *Packet) Expiry() uint64 {
	if len(packet.Extras) == 4 {
		return uint64(getUint32(packet.Extras, 0)) * uint64(time.Second)
	}
	return uint64(getUint32(packet.Extras, 4)) * uint64(time.Second)
}

// MakeExpiry makes the extras bytes containing the time to expiration.
func MakeExpiry(expiry, t uint64) []byte {
	extras := make([]byte, 4)
	setUint32(extras, 0, uint32((expiry-t)/uint64(time.Second)))
	return extras
}

// MakeFlags makes the extras bytes containing the flags.
func MakeFlags(flags uint32) []byte {
	extras := make([]byte, 4)
	setUint32(extras, 0, flags)
	return extras
}

// MakeFlagsExpiry makes the extras bytes containing the flags and the
// time to expiration.
func MakeFlagsExpiry(flags uint32, expiry, t uint64) []byte {
	extras := make([]byte, 8)
	setUint32(extras, 0, flags)
	setUint32(extras, 4, uint32((expiry-t)/uint64(time.Second)))
	return extras
}
