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
import Data.Maybe(catMaybes)
import Data.Word(Word8,Word64)

import Cache(
    Cache(get,set,add,replace,delete,touch,expire),
    CacheError(EntryExists,NotFound,VersionMismatch,UnknownError),
    Expires(Expires),Timestamp(Timestamp),Version(Version))

-- | TrieCache.  t is the timestamp type.  e is the type of the cached values.
-- One flaw is that version numbers are reset when expired subtries are
-- pruned.  One fix would be to add a start version to CacheEntry that
-- gets updated every time its subtries are pruned.
newtype TrieCache t e = TrieCache (TVar (CacheEntry t e))

type Entry t e = Maybe (t,e)

data CacheEntry t e = CacheEntry {
    ceEntry :: Entry t e,
    ceVersion :: Version,
    ceTrie :: TArray Word8 (Maybe (CacheEntry t e))
    }

-- | Create a new empty TrieCache.
newTrieCache :: Ord t => IO (TrieCache t e)
newTrieCache = atomically $ do
    ce <- newCacheEntry
    cache <- newTVar ce
    return (TrieCache cache)

newCacheEntry :: Ord t => STM (CacheEntry t e)
newCacheEntry = do
    trie <- newArray (minBound,maxBound) Nothing
    return CacheEntry { ceEntry = Nothing, ceVersion = Version 0, ceTrie = trie }

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
                maybeCe <- readArray (ceTrie ce) (key `index` i)
                maybe (return (Left NotFound)) (get' (i + 1)) maybeCe
            | noEntry ce t = return (Left NotFound)
            | otherwise =
                let Just (_,e) = ceEntry ce in return (Right (e, ceVersion ce))

    set (TrieCache cache) key e (Expires exp) (Timestamp t) = atomically $ do
        ce <- readTVar cache
        (maybeCe,result) <- set' 0 (Just ce)
        maybe (return ()) (writeTVar cache) maybeCe
        return result
      where
        set' i Nothing = do
            ce <- newCacheEntry
            (maybeCe,result) <- set' i (Just ce)
            return (maybe (Just ce) Just maybeCe,result)
        set' i (Just ce)
            | i < B.length key = do
                maybeCe <- readArray (ceTrie ce) (key `index` i)
                (maybeCe',result) <- set' (i + 1) maybeCe
                maybe (return ()) (writeArray (ceTrie ce) (key `index` i) . Just) maybeCe'
                return (Nothing,result)
            | otherwise = do
                let version = incVersion ce
                return (Just ce { ceEntry = Just (exp,e), ceVersion = version }, Right version)

    add (TrieCache cache) key e (Expires exp) (Timestamp t) = atomically $ do
        ce <- readTVar cache
        (maybeCe,result) <- add' 0 (Just ce)
        maybe (return ()) (writeTVar cache) maybeCe
        return result
      where
        add' i Nothing = do
            ce <- newCacheEntry
            (maybeCe,result) <- add' i (Just ce)
            return (maybe (Just ce) Just maybeCe,result)
        add' i (Just ce)
            | i < B.length key = do
                maybeCe <- readArray (ceTrie ce) (key `index` i)
                (maybeCe',result) <- add' (i + 1) maybeCe
                maybe (return ()) (writeArray (ceTrie ce) (key `index` i) . Just) maybeCe'
                return (if expired ce t then Just ce { ceEntry = Nothing } else Nothing,result)
            | noEntry ce t =
                let version = incVersion ce
                in  return (Just ce { ceEntry = Just (exp,e), ceVersion = version }, Right version)
            | otherwise = return (Nothing,Left EntryExists)

    replace (TrieCache cache) key version e (Expires exp) (Timestamp t) = atomically $ do
        ce <- readTVar cache
        (maybeCe,result) <- replace' 0 ce
        maybe (return ()) (writeTVar cache) maybeCe
        return result
      where
        replace' i ce
          | i < B.length key = do
                maybeCe <- readArray (ceTrie ce) (key `index` i)
                (maybeCe',result) <- maybe (return (Nothing,Left NotFound)) (replace' (i + 1)) maybeCe
                maybe (return ()) (writeArray (ceTrie ce) (key `index` i) . Just) maybeCe'
                return (if expired ce t then Just ce { ceEntry = Nothing } else Nothing,result)
          | noEntry ce t = return (Nothing,Left NotFound)
          | version /= ceVersion ce = return (Nothing,Left VersionMismatch)
          | otherwise =
                let version = incVersion ce
                in  return (Just ce { ceEntry = Just (exp,e), ceVersion = version }, Right version)

    delete (TrieCache cache) key version (Timestamp t) = atomically $ do
        ce <- readTVar cache
        (maybeCe,result) <- delete' 0 ce
        maybe (return ()) (writeTVar cache) maybeCe
        return result
      where
        delete' i ce
          | i < B.length key = do
                maybeCe <- readArray (ceTrie ce) (key `index` i)
                (maybeCe',result) <- maybe (return (Nothing,Just NotFound)) (delete' (i + 1)) maybeCe
                maybe (return ()) (writeArray (ceTrie ce) (key `index` i) . Just) maybeCe'
                return (Nothing,result)
          | expired ce t =
                return (Just ce { ceEntry = Nothing }, Just NotFound)
          | noEntry ce t = return (Nothing, Just NotFound)
          | version /= ceVersion ce = return (Nothing, Just VersionMismatch)
          | otherwise = return (Just ce { ceEntry = Nothing }, Nothing)

    touch (TrieCache cache) key (Expires exp) = atomically $ do
        ce <- readTVar cache
        (maybeCe,result) <- touch' 0 ce
        maybe (return ()) (writeTVar cache) maybeCe
        return result
      where
        touch' i ce
          | i < B.length key = do
                maybeCe <- readArray (ceTrie ce) (key `index` i)
                (maybeCe',result) <- maybe (return (Nothing,Left NotFound)) (touch' (i + 1)) maybeCe
                maybe (return ()) (writeArray (ceTrie ce) (key `index` i) . Just) maybeCe'
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
            maybeCe <- atomically $ readArray (ceTrie ce) i
            maybe (return ()) expire' maybeCe
            atomically $ do
                maybeCe <- readArray (ceTrie ce) i
                maybeMaybeCe <- maybe (return Nothing) prune maybeCe
                maybe (return ()) (writeArray (ceTrie ce) i) maybeMaybeCe
        prune ce
          | expired ce t = do
                leaf <- isLeaf ce
                if leaf
                    then return (Just Nothing)
                    else return (Just (Just ce { ceEntry = Nothing }))
          | noEntry ce t = do
                leaf <- isLeaf ce
                if leaf then return (Just Nothing) else return Nothing
          | otherwise = return Nothing
        isLeaf ce = do
            maybeCes <- getElems (ceTrie ce)
            return (null (catMaybes maybeCes))
