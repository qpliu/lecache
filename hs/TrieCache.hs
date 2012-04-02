-- | TrieCache implements an in-memory cache, using a trie,
-- written as an exercise in learning STM.
module TrieCache(
    -- * Types
    TrieCache,
    -- * Functions
    newTrieCache
    ) where

import Control.Concurrent.STM(STM,TArray,TVar,atomically,newTVar,readTVar,writeTVar)
import Data.Array.MArray(MArray(newArray),readArray,writeArray,getAssocs,getElems)
import Data.ByteString(ByteString,index)
import qualified Data.ByteString as B
import Data.Either(lefts,rights)
import Data.Word(Word8,Word64)

import Cache(
    Cache(get,set,add,replace,delete,touch,expire),
    CacheError(EntryExists,NotFound,VersionMismatch,UnknownError),
    Expires(Expires),Timestamp(Timestamp),Version(Version))

-- | TrieCache.  t is the timestamp type.  e is the type of the cached values.
newtype TrieCache t e = TrieCache (TVar (CacheEntry t e))

type Entry t e = Maybe (t,e)

data CacheEntry t e = CacheEntry {
    ceEntry :: Entry t e,
    ceVersion :: Version,
    ceTrie :: TArray Word8 (Either Version (CacheEntry t e))
    }

-- | Create a new empty TrieCache.
newTrieCache :: Ord t => IO (TrieCache t e)
newTrieCache = atomically $ do
    ce <- newCacheEntry (Version 0)
    cache <- newTVar ce
    return (TrieCache cache)

newCacheEntry :: Ord t => Version -> STM (CacheEntry t e)
newCacheEntry startVersion = do
    trie <- newArray (minBound,maxBound) (Left startVersion)
    return CacheEntry { ceEntry = Nothing, ceVersion = startVersion, ceTrie = trie }

expired :: Ord t => CacheEntry t e -> t -> Bool
expired ce timestamp = maybe False ((timestamp >=) . fst) (ceEntry ce)

noEntry :: Ord t => CacheEntry t e -> t -> Bool
noEntry ce timestamp = maybe True ((timestamp >=) . fst) (ceEntry ce)

incVersion :: CacheEntry t e -> Version
incVersion CacheEntry { ceVersion = Version version } = Version (version + 1)

instance Cache TrieCache where
    get (TrieCache cache) key (Timestamp t) = atomically $ do
        ce <- readTVar cache
        get' 0 ce
      where
        get' i ce
            | i < B.length key = do
                subCe <- readArray (ceTrie ce) (key `index` i)
                either (const (return (Left NotFound))) (get' (i + 1)) subCe
            | noEntry ce t = return (Left NotFound)
            | otherwise =
                let Just (_,e) = ceEntry ce in return (Right (e, ceVersion ce))

    set (TrieCache cache) key e (Expires exp) (Timestamp t) = atomically $ do
        ce <- readTVar cache
        (updateCe,result) <- set' 0 (Right ce)
        maybe (return ()) (writeTVar cache) updateCe
        return result
      where
        set' i (Left startVersion) = do
            ce <- newCacheEntry startVersion
            (updateCe,result) <- set' i (Right ce)
            return (maybe (Just ce) Just updateCe,result)
        set' i (Right ce)
            | i < B.length key = do
                subCe <- readArray (ceTrie ce) (key `index` i)
                (updateCe,result) <- set' (i + 1) subCe
                maybe (return ()) (writeArray (ceTrie ce) (key `index` i) . Right) updateCe
                return (Nothing,result)
            | otherwise = do
                let version = incVersion ce
                return (Just ce { ceEntry = Just (exp,e), ceVersion = version }, Right version)

    add (TrieCache cache) key e (Expires exp) (Timestamp t) = atomically $ do
        ce <- readTVar cache
        (updateCe,result) <- add' 0 (Right ce)
        maybe (return ()) (writeTVar cache) updateCe
        return result
      where
        add' i (Left startVersion) = do
            ce <- newCacheEntry startVersion
            (updateCe,result) <- add' i (Right ce)
            return (maybe (Just ce) Just updateCe,result)
        add' i (Right ce)
            | i < B.length key = do
                subCe <- readArray (ceTrie ce) (key `index` i)
                (updateCe,result) <- add' (i + 1) subCe
                maybe (return ()) (writeArray (ceTrie ce) (key `index` i) . Right) updateCe
                return (if expired ce t then Just ce { ceEntry = Nothing } else Nothing,result)
            | noEntry ce t =
                let version = incVersion ce
                in  return (Just ce { ceEntry = Just (exp,e), ceVersion = version }, Right version)
            | otherwise = return (Nothing,Left EntryExists)

    replace (TrieCache cache) key version e (Expires exp) (Timestamp t) = atomically $ do
        ce <- readTVar cache
        (updateCe,result) <- replace' 0 ce
        maybe (return ()) (writeTVar cache) updateCe
        return result
      where
        replace' i ce
          | i < B.length key = do
                subCe <- readArray (ceTrie ce) (key `index` i)
                (updateCe,result) <- either (const (return (Nothing,Left NotFound))) (replace' (i + 1)) subCe
                maybe (return ()) (writeArray (ceTrie ce) (key `index` i) . Right) updateCe
                return (if expired ce t then Just ce { ceEntry = Nothing } else Nothing,result)
          | noEntry ce t = return (Nothing,Left NotFound)
          | version /= ceVersion ce = return (Nothing,Left VersionMismatch)
          | otherwise =
                let version = incVersion ce
                in  return (Just ce { ceEntry = Just (exp,e), ceVersion = version }, Right version)

    delete (TrieCache cache) key version (Timestamp t) = atomically $ do
        ce <- readTVar cache
        (updateCe,result) <- delete' 0 ce
        maybe (return ()) (writeTVar cache) updateCe
        return result
      where
        delete' i ce
          | i < B.length key = do
                subCe <- readArray (ceTrie ce) (key `index` i)
                (updateCe,result) <- either (const (return (Nothing,Just NotFound))) (delete' (i + 1)) subCe
                maybe (return ()) (writeArray (ceTrie ce) (key `index` i) . Right) updateCe
                return (Nothing,result)
          | expired ce t =
                return (Just ce { ceEntry = Nothing }, Just NotFound)
          | noEntry ce t = return (Nothing, Just NotFound)
          | version /= ceVersion ce = return (Nothing, Just VersionMismatch)
          | otherwise = return (Just ce { ceEntry = Nothing }, Nothing)

    touch (TrieCache cache) key (Expires exp) = atomically $ do
        ce <- readTVar cache
        (updateCe,result) <- touch' 0 ce
        maybe (return ()) (writeTVar cache) updateCe
        return result
      where
        touch' i ce
          | i < B.length key = do
                subCe <- readArray (ceTrie ce) (key `index` i)
                (updateCe,result) <- either (const (return (Nothing,Left NotFound))) (touch' (i + 1)) subCe
                maybe (return ()) (writeArray (ceTrie ce) (key `index` i) . Right) updateCe
                return (Nothing,result)
          | otherwise =
                maybe (return (Nothing,Left NotFound))
                    (\ (_,e) -> return (Just ce { ceEntry = Just (exp,e) },Right (ceVersion ce)))
                    (ceEntry ce)

    expire (TrieCache cache) (Timestamp t) = do
        ce <- atomically $ readTVar cache
        expire' ce
        atomically $ do
            ce <- readTVar cache
            if expired ce t
                then writeTVar cache ce { ceEntry = Nothing }
                else return ()
      where
        expire' ce = mapM_ (expire'' ce) [minBound .. maxBound]
        expire'' ce i = do
            subCe <- atomically $ readArray (ceTrie ce) i
            either (const (return ())) expire' subCe
            atomically $ do
                subCe <- readArray (ceTrie ce) i
                maybeUpdateCe <- either (const (return Nothing)) prune subCe
                maybe (return ()) (writeArray (ceTrie ce) i) maybeUpdateCe
        prune ce
          | expired ce t = do
                subCes <- getElems (ceTrie ce)
                if null (rights subCes)
                    then return (Just (Left (foldl maxVersion (ceVersion ce) (lefts subCes))))
                    else return (Just (Right ce { ceEntry = Nothing }))
          | noEntry ce t = do
                subCes <- getElems (ceTrie ce)
                if null (rights subCes)
                    then return (Just (Left (foldl maxVersion (ceVersion ce) (lefts subCes))))
                    else return Nothing
          | otherwise = return Nothing
        maxVersion version1@(Version v1) version2@(Version v2)
          | v1 > v2 = version1
          | otherwise = version2
