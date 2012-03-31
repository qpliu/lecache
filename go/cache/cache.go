// Cache ...
package cache

import (
	"errors"
	"time"
)

// NotFound ...
var NotFound = errors.New("Not found")

// EntryExists ...
var EntryExists = errors.New("Entry exists")

// VersionMismatch ...
var VersionMismatch = errors.New("Version mismatch")

// UnknownError ...
var UnknownError = errors.New("Unknown error")

// Version ...
type Version uint64

// Timestamp ...
type Timestamp uint64

// Cache ...
type Cache interface {
	Get(key []byte, time Timestamp) ([]byte, uint32, Version, error)
	Set(key, data []byte, flags uint32, expires, time Timestamp) (Version, error)
	Add(key, data []byte, flags uint32, expires, time Timestamp) (Version, error)
	Replace(key, data []byte, flags uint32, version Version, expires, time Timestamp) (Version, error)
	Delete(key []byte, version Version, time Timestamp) error
	Touch(key []byte, expires Timestamp) error
	Expire(time Timestamp)
}

// Now ...
func Now() Timestamp {
	return Timestamp(time.Now().Unix())
}

// Future ...
func Future(duration time.Duration) Timestamp {
	return Timestamp(time.Now().Add(duration).Unix())
}
