package cache

import (
	"errors"
	"time"
)

// NotFound is an error returned by Cache.Get, Cache.Replace, Cache.Delete, and Cache.Touch.
var NotFound = errors.New("Not found")

// EntryExists is an error returned by Cache.Add.
var EntryExists = errors.New("Entry exists")

// VersionMismatch is an error returned by Cache.Replace and Cache.Delete.
var VersionMismatch = errors.New("Version mismatch")

// UnknownError is some error other than NotFound, EntryExists or VersionMismatch.
var UnknownError = errors.New("Unknown error")

// Version is the version of a cache entry, incremented each time the entry is updated.
type Version uint64

type Timestamp uint64

// Cache type.  The key is a byte array.
// Each entry has a byte array value, 32 bits of flags, and a version.
type Cache interface {
	Get(key []byte, time Timestamp) ([]byte, uint32, Version, error)
	Set(key, data []byte, flags uint32, expires, time Timestamp) (Version, error)
	Add(key, data []byte, flags uint32, expires, time Timestamp) (Version, error)
	Replace(key, data []byte, flags uint32, version Version, expires, time Timestamp) (Version, error)
	Delete(key []byte, version Version, time Timestamp) error
	Touch(key []byte, expires Timestamp) error
	Expire(time Timestamp)
}

// Now returns a Timestamp with the current time.
func Now() Timestamp {
	return Timestamp(time.Now().Unix())
}

// Future returns a Timestamp with a future time.
func Future(duration time.Duration) Timestamp {
	return Timestamp(time.Now().Add(duration).Unix())
}
