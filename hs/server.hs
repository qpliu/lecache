import Control.Concurrent(forkIO,threadDelay)
import Data.ByteString(ByteString,pack)
import Data.Char(ord)
import Data.Time(UTCTime,addUTCTime,getCurrentTime)
import Data.Word(Word32)
import Network(PortNumber,Socket,sClose,withSocketsDo)
import System.Environment(getArgs)

import Cache(
    Cache(get,set,add,replace,delete,expire),
    CacheError(EntryExists,NotFound,VersionMismatch,UnknownError),
    Expires(Expires),Timestamp(Timestamp),Version(Version))
import Listener(listener)
import Logger(Logger,nullLogger,debugLogger)
import Protocol(
    readPacket, writeResponse,
    packetCmd, packetVersion, packetExtras, packetKey, packetValue,
    makeFlags, getExpiry, getFlags,
    magicRequest,
    cmdGet, cmdSet, cmdAdd, cmdReplace, cmdDelete, cmdQuit,
    statusNoError, statusKeyNotFound, statusKeyExists,
    statusItemNotStored, statusNotSupported, statusInternalError)
import TrieCache(newTrieCache)

main :: IO ()
main = withSocketsDo $ do
    (logger,port) <- fmap (parseArgs nullLogger defaultPort) getArgs
    cache <- newTrieCache
    forkIO $ sequence_ $ repeat $ expirer cache
    listener port (handler logger cache)
  where
    expirer cache = do
        threadDelay 15000000
        t <- getCurrentTime
        expire cache (Timestamp t)

defaultPort :: PortNumber
defaultPort = fromIntegral 22122

parseArgs :: Logger -> PortNumber -> [String] -> (Logger,PortNumber)
parseArgs logger portNumber ("-d":args) =
    parseArgs (debugLogger "server") portNumber args
parseArgs logger portNumber (arg:args) =
    parseArgs logger (parsePort portNumber arg) args
parseArgs logger portNumber [] = (logger,portNumber)

parsePort :: PortNumber -> String -> PortNumber
parsePort portNumber arg =
    case reads arg of
        [(number,"")] -> fromIntegral number
        _ -> portNumber

handler :: Cache cache => Logger -> cache UTCTime (Word32,ByteString) -> Socket -> IO ()
handler logger cache socket = do
    maybePacket <- readPacket logger socket magicRequest
    time <- getCurrentTime
    maybe (sClose socket) (handle (Timestamp time)) maybePacket
  where
    handle time packet =
        handleCmd (packetCmd packet)
                  (packetExtras packet) (packetKey packet) (packetValue packet)
      where
        handleCmd cmd [] (Just key) Nothing
          | cmd == cmdGet = do
                result <- get cache key time
                sendGetResult result
          | cmd == cmdDelete = do
                result <- delete cache key requestVersion time
                sendDeleteResult result
          | otherwise = sendStatus statusNotSupported errorValue
          where
            sendGetResult (Right ((flags,value),Version version)) = do
                writeResponse logger socket packet statusNoError version
                              [makeFlags flags] Nothing (Just value)
                handler logger cache socket
            sendGetResult (Left NotFound) =
                sendStatus statusKeyNotFound errorValue
            sendGetResult _ =
                sendStatus statusInternalError errorValue
            sendDeleteResult Nothing = sendStatus statusNoError Nothing
            sendDeleteResult (Just NotFound) =
                sendStatus statusKeyNotFound errorValue
            sendDeleteResult (Just VersionMismatch) =
                sendStatus statusItemNotStored errorValue
            sendDeleteResult _ = sendStatus statusInternalError errorValue
        handleCmd cmd [extras] (Just key) (Just value)
          | cmd == cmdSet = do
                result <- set cache key (flags,value) expires time
                sendResult result
          | cmd == cmdAdd = do
                result <- add cache key (flags,value) expires time
                sendResult result
          | cmd == cmdReplace = do
                result <- replace cache key requestVersion (flags,value) expires time
                sendResult result
          | otherwise = sendStatus statusNotSupported errorValue
          where
            expires = Expires $ getExpiry extras 4 `addUTCTime` (let Timestamp t = time in t)
            flags = getFlags extras 0
            sendResult (Right (Version version)) = do
                writeResponse logger socket packet statusNoError version
                              [] Nothing Nothing
                handler logger cache socket
            sendResult (Left EntryExists) =
                sendStatus statusKeyExists errorValue
            sendResult (Left NotFound) =
                sendStatus statusKeyNotFound errorValue
            sendResult (Left VersionMismatch) =
                sendStatus statusItemNotStored errorValue
        handleCmd cmd [] Nothing Nothing
          | cmd == cmdQuit = do
                writeResponse logger socket packet statusNoError 0 [] Nothing Nothing
                sClose socket
          | otherwise = sendStatus statusNotSupported errorValue
        handleCmd _ _ _ _ = sendStatus statusNotSupported errorValue
        errorValue = Just $ pack $ map (fromIntegral . ord) "error"
        requestVersion = Version $ packetVersion packet
        sendStatus status value = do
            writeResponse logger socket packet status 0 [] Nothing value
            handler logger cache socket
