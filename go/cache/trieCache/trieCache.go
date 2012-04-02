// Package trieCache provides an in-memory cache, using a trie,
// written as an exercise in learning Go.
package trieCache

import (
	"../../cache"
	"sync"
)

type trieCache struct {
	lock         sync.RWMutex
	set          bool
	expires      cache.Timestamp
	version      cache.Version
	startVersion cache.Version
	data         []byte
	flags        uint32
	trie         [256]*trieCache
}

// New returns a new empty Cache.
func New() cache.Cache {
	return &trieCache{}
}

// Get the value, flags, and version associated with the key.
// time is the current time.  Returns cache.NotFound is not such
// entry exists or if it has expired.
func (tc *trieCache) Get(key []byte, time cache.Timestamp) ([]byte, uint32, cache.Version, error) {
	for _, k := range key {
		tc.lock.RLock()
		tcnext := tc.trie[k]
		tc.lock.RUnlock()
		if tcnext == nil {
			return nil, 0, 0, cache.NotFound
		}
		tc = tcnext
	}
	tc.lock.RLock()
	defer tc.lock.RUnlock()
	if !tc.set || time >= tc.expires {
		return nil, 0, 0, cache.NotFound
	}
	return tc.data, tc.flags, tc.version, nil
}

// Set the value associated with the key, adding it if not already present,
// expiring at expires.  timestamp is the current time.  Returns the new
// version of the entry.
func (tc *trieCache) Set(key, data []byte, flags uint32, expires, time cache.Timestamp) (cache.Version, error) {
	for _, k := range key {
		tc.lock.RLock()
		tcnext := tc.trie[k]
		if tcnext != nil {
			defer tc.lock.RUnlock()
		} else {
			tc.lock.RUnlock()
			tc.lock.Lock()
			defer tc.lock.Unlock()
			if tc.trie[k] == nil {
				tc.trie[k] = &trieCache{version: tc.startVersion, startVersion: tc.startVersion}
			}
			tcnext = tc.trie[k]
		}
		tc = tcnext
	}
	tc.lock.Lock()
	defer tc.lock.Unlock()
	tc.set = true
	tc.expires = expires
	tc.version++
	tc.data = data
	tc.flags = flags
	return tc.version, nil
}

// Add the key to the cache with the given value, expiring at expires.
// timestamp is the current time.  Returns the new version of the entry.
// Returns cache.EntryExists if the key is already in the cache.
func (tc *trieCache) Add(key, data []byte, flags uint32, expires, time cache.Timestamp) (cache.Version, error) {
	for _, k := range key {
		tc.lock.RLock()
		tcnext := tc.trie[k]
		if tcnext != nil {
			defer tc.lock.RUnlock()
		} else {
			tc.lock.RUnlock()
			tc.lock.Lock()
			defer tc.lock.Unlock()
			if tc.trie[k] == nil {
				tc.trie[k] = &trieCache{version: tc.startVersion, startVersion: tc.startVersion}
			}
			tcnext = tc.trie[k]
		}
		tc = tcnext
	}
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if tc.set && time < tc.expires {
		return 0, cache.EntryExists
	}
	tc.set = true
	tc.expires = expires
	tc.version++
	tc.data = data
	tc.flags = flags
	return tc.version, nil
}

// Replace the value associated with the key with the new value,
// provided that the version matches the version in the cache,
// expiring at expires.  timestamp is the current time.  Returns the new
// version of the entry.  Returns cache.NotFound if the entry does not
// exist.  Returns cache.VersionMismatch if the version if the cache
// does not match.
func (tc *trieCache) Replace(key []byte, data []byte, flags uint32, version cache.Version, expires, time cache.Timestamp) (cache.Version, error) {
	for _, k := range key {
		tc.lock.RLock()
		defer tc.lock.RUnlock()
		tcnext := tc.trie[k]
		if tcnext == nil {
			return 0, cache.NotFound
		}
		tc = tcnext
	}
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if !tc.set {
		return 0, cache.NotFound
	}
	if time >= tc.expires {
		tc.set = false
		tc.data = nil
		return 0, cache.NotFound
	}
	if tc.version != version {
		return 0, cache.VersionMismatch
	}
	tc.expires = expires
	tc.version++
	tc.data = data
	tc.flags = flags
	return tc.version, nil
}

// Delete the key form the cache,
// provided that the version matches the version in the cache.  timestamp
// is the current time.  Returns NotFound if the entry does not exist.
// Returns cache.VersionMismatch if the version if the cache does not match.
func (tc *trieCache) Delete(key []byte, version cache.Version, time cache.Timestamp) error {
	for _, k := range key {
		tc.lock.RLock()
		defer tc.lock.RUnlock()
		tcnext := tc.trie[k]
		if tcnext == nil {
			return cache.NotFound
		}
		tc = tcnext
	}
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if !tc.set {
		return cache.NotFound
	}
	if time >= tc.expires {
		tc.set = false
		tc.data = nil
		return cache.NotFound
	}
	if tc.version != version {
		return cache.VersionMismatch
	}
	tc.set = false
	tc.data = nil
	return nil
}

// Change the expiration time for the key.
// Returns cache.NotFound is the entry does not exist.
func (tc *trieCache) Touch(key []byte, expires cache.Timestamp) error {
	for _, k := range key {
		tc.lock.RLock()
		defer tc.lock.RUnlock()
		tcnext := tc.trie[k]
		if tcnext == nil {
			return cache.NotFound
		}
		tc = tcnext
	}
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if !tc.set {
		return cache.NotFound
	}
	tc.expires = expires
	return nil
}

// Prune expired entries.  timestamp is the current time.
func (tc *trieCache) Expire(time cache.Timestamp) {
	expire(tc, time)
}

func expire(tc *trieCache, time cache.Timestamp) bool {
	empty := true
	for i := 0; i < 256; i++ {
		tc.lock.RLock()
		c := tc.trie[i]
		tc.lock.RUnlock()
		if c != nil && !expire(c, time) {
			empty = false
		}
	}
	expired := false
	tc.lock.RLock()
	if tc.set {
		if time < tc.expires {
			empty = false
		} else {
			expired = true
		}
	}
	tc.lock.RUnlock()
	if !empty && !expired {
		return false
	}
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if tc.set && time < tc.expires {
		tc.set = false
		tc.data = nil
	}
	if !empty {
		return false
	}
	for i := 0; i < 256; i++ {
		c := tc.trie[i]
		if c == nil {
		} else if expire(c, time) {
			tc.trie[i] = nil
			tc.startVersion = max(tc.startVersion, c.startVersion, c.version)
		} else {
			empty = false
		}
	}
	return empty
}

func max(v1 cache.Version, v2 cache.Version, v3 cache.Version) cache.Version {
	if v1 > v2 {
		if v1 > v3 {
			return v1
		}
		return v3
	}
	if v2 > v3 {
		return v2
	}
	return v3
}
