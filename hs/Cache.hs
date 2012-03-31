-- | Cache implements an in-memory cache, using a trie,
-- written as an exercise to learn STM.
module Cache(
    -- * Types
    Cache(..),
    CacheError(..),
    Expires(Expires),Timestamp(Timestamp),Version(Version)
    ) where

import Data.ByteString(ByteString)
import Data.Word(Word64)

-- | Cache type.  Ord t => t is the timestamp type.  e is the type of
-- the data entries.
class Cache cache where
    get :: Ord t => cache t e -> ByteString -> Timestamp t -> IO (Either CacheError (e,Version))
    set :: Ord t => cache t e -> ByteString -> e -> Expires t -> Timestamp t -> IO (Either CacheError Version)
    add :: Ord t => cache t e -> ByteString -> e -> Expires t -> Timestamp t -> IO (Either CacheError Version)
    replace :: Ord t => cache t e -> ByteString -> Version -> e -> Expires t -> Timestamp t -> IO (Either CacheError Version)
    delete :: Ord t => cache t e -> ByteString -> Version -> Timestamp t -> IO (Maybe CacheError)
    touch :: Ord t => cache t e -> ByteString -> Expires t -> IO (Either CacheError Version)
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
