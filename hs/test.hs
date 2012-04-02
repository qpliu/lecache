import Control.Concurrent(Chan,forkIO,newChan,readChan,writeChan)
import Data.ByteString(ByteString,pack)
import Data.Char(ord)
import Data.Time(UTCTime,getCurrentTime,addUTCTime,diffUTCTime)
import Debug.Trace(putTraceMsg)
import Network(withSocketsDo)
import System.Environment(getArgs)

import Cache(
    Cache(get,set,add,replace,delete),
    Expires(Expires),Timestamp(Timestamp),Version(Version))
import Cache(Cache(get,set,replace,delete))
import Logger(nullLogger,debugLogger)
import RemoteCache(newRemoteCache)
import TrieCache(newTrieCache)

main :: IO ()
main = withSocketsDo $ do
    (nthreads,nconnections,remote,logger) <- fmap (parseArgs (4,2,Nothing,const nullLogger)) getArgs
    case remote of
      Just (hostname,servicename) -> do
        caches <- sequence $ [newRemoteCache (logger ("test" ++ show i)) hostname servicename ((,) 0 . toBytes) (show . snd) diffUTCTime | i <- [1..nconnections]]
        test caches nthreads
      _ -> do
        cache <- newTrieCache
        test [cache] nthreads
  where
    parseArgs (nthreads,nconnections,remote,logger) ("-c":arg:args) =
        parseArgs (nthreads,read arg,remote,logger) args
    parseArgs (nthreads,nconnections,remote,logger) ("-d":args) =
        parseArgs (nthreads,nconnections,remote,debugLogger) args
    parseArgs (nthreads,nconnections,remote,logger) ("-t":arg:args) =
        parseArgs (read arg,nconnections,remote,logger) args
    parseArgs (nthreads,nconnections,remote,logger) (host:port:_) =
        (nthreads,nconnections,Just (host,port),logger)
    parseArgs result _ = result

test :: Cache cache => [cache UTCTime String] -> Int -> IO ()
test caches nthreads = do
    done <- newChan
    mapM_ (forkIO . test1 done) $ zip (cycle caches) [1..nthreads]
    sequence_ $ replicate nthreads (readChan done)

test1 :: Cache cache => Chan () -> (cache UTCTime String,Int) -> IO ()
test1 done (cache,i) = do
    let key1 = toBytes $ "key" ++ show ((i+1) `mod` 2 + 1)
    let key2 = toBytes $ "key" ++ show ((i+2) `mod` 2 + 1)
    let value1 = "value" ++ show ((i-1)*2 + 1)
    let value2 = "value" ++ show ((i-1)*2 + 2)
    let tag = show i
    t <- getCurrentTime
    let exp = fromInteger 5 `addUTCTime` t
    r <- get cache key1 (Timestamp t)
    putTraceMsg (tag ++ " get " ++ show key1 ++ ":" ++ show r)
    r <- set cache key1 value1 (Expires exp) (Timestamp t)
    putTraceMsg (tag ++ " set " ++ show key1 ++ "=" ++ value1 ++ ":" ++ show r)
    let v = either (const $ Version 0) id r
    r <- delete cache key1 v (Timestamp t)
    putTraceMsg (tag ++ " delete " ++ show key1 ++ ":" ++ show r)
    r <- add cache key1 value1 (Expires exp) (Timestamp t)
    putTraceMsg (tag ++ " add " ++ show key1 ++ "=" ++ value1 ++ ":" ++ show r)
    let v = either (const $ Version 0) id r
    r <- replace cache key1 v value2 (Expires exp) (Timestamp t)
    putTraceMsg (tag ++ " replace " ++ show key1 ++ "=" ++ value2 ++ ":" ++ show r)
    r <- get cache key2 (Timestamp t)
    putTraceMsg (tag ++ " get " ++ show key2 ++ ":" ++ show r)
    let v = either (const $ Version 0) snd r
    r <- replace cache key2 v value2 (Expires exp) (Timestamp t)
    putTraceMsg (tag ++ " replace " ++ show key2 ++ "=" ++ value2 ++ ":" ++ show r)
    writeChan done ()

toBytes :: String -> ByteString
toBytes str = pack $ map (fromIntegral . ord) str
