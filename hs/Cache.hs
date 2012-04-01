module Cache(
    Cache(..),
    CacheError(..),
    Expires(Expires),Timestamp(Timestamp),Version(Version)
    ) where

import Data.ByteString(ByteString)
import Data.Word(Word64)

-- | Cache type.  Keys are ByteStrings.  t is the timestamp type.  e is the type of the cached values.
class Cache cache where
    -- | Get the value and version of the entry associated with the key.  The Timestamp is the current time.  Returns NotFound if no such entry exists or if it has expired.
    get :: Ord t => cache t e -> ByteString -> Timestamp t -> IO (Either CacheError (e,Version))
    -- | Set the value associated with the key, adding it if not already present, expiring at the Expires time.  The Timestamp is the current time.  Returns the new version of the entry.
    set :: Ord t => cache t e -> ByteString -> e -> Expires t -> Timestamp t -> IO (Either CacheError Version)
    -- | Add the key to the cache with the given value, expiring at the Expires time.  The Timestamp is the current time.  Returns the new version of the entry.  Returns EntryExists if the key is already in the cache.
    add :: Ord t => cache t e -> ByteString -> e -> Expires t -> Timestamp t -> IO (Either CacheError Version)
    -- | Replace the value associated with the key with the new value, provided that the version matches the version in the cache, expiring at the Expires time.  The Timestamp is the current time.  Returns the new version of the entry.  Returns NotFound if the entry does not exist.  Returns VersionMismatch if the version if the cache does not match.
    replace :: Ord t => cache t e -> ByteString -> Version -> e -> Expires t -> Timestamp t -> IO (Either CacheError Version)
    -- | Delete the key form the cache, provided that the version matches the version in the cache.  The Timestamp is the current time.  Returns NotFound if the entry does not exist.  Returns VersionMismatch if the version if the cache does not match.
    delete :: Ord t => cache t e -> ByteString -> Version -> Timestamp t -> IO (Maybe CacheError)
    -- | Change the expiration time for the key.  Returns the entry version.  Returns NotFound is the entry does not exist.
    touch :: Ord t => cache t e -> ByteString -> Expires t -> IO (Either CacheError Version)
    -- | Prune expired entries.  The Timestamp is the current time.
    expire :: Ord t => cache t e -> Timestamp t -> IO ()

-- | Version type.
newtype Version = Version Word64
    deriving (Eq, Show)

-- | Expires type.
newtype Expires t = Expires t

-- | Timestamp type.
newtype Timestamp t = Timestamp t

-- | Error type.
data CacheError = EntryExists | NotFound | VersionMismatch | UnknownError
    deriving (Eq, Show)
