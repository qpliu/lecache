module RemoteCache(
    -- * Types
    RemoteCache,
    -- * Functions
    newRemoteCache
    ) where

import Control.Concurrent(
    Chan,MVar,forkIO,newEmptyMVar,putMVar,takeMVar,newChan,readChan,writeChan)
import Data.ByteString(ByteString)
import qualified Data.ByteString as B
import Data.Time(NominalDiffTime)
import Data.Word(Word32,Word64)
import Network.Socket(
    Socket,SocketType(Stream),HostName,ServiceName,
    connect,socket,getAddrInfo,addrFamily,addrAddress,addrProtocol,defaultProtocol)
import Network.Socket.ByteString(recv,sendMany)

import Cache(
    Cache(get,set,add,replace,delete,expire,touch),
    CacheError(EntryExists,NotFound,VersionMismatch,UnknownError),
    Expires(Expires),Timestamp(Timestamp),Version(Version))
import Logger(Logger)
import Protocol(
    Packet, readPacket, writeRequest,
    packetStatus, packetVersion, packetExtras, packetValue,
    makeExpiry, getFlags, makeFlags,
    magicResponse,
    Cmd, cmdGet, cmdSet, cmdAdd, cmdReplace, cmdDelete,
    statusNoError, statusKeyNotFound, statusKeyExists,
    statusItemNotStored, statusNotSupported, statusInternalError)

-- | RemoteCache accesses a remote cache, using a memcache protocol subset.
data RemoteCache t e = RemoteCache {
    rcSerialize :: e -> (Word32,ByteString),
    rcDeserialize :: (Word32,ByteString) -> e,
    rcConvertExpiry :: t -> t -> NominalDiffTime,
    rcChan :: Chan Req
    }

data Req = Req {
    reqCmd :: Cmd,
    reqVersion :: Word64,
    reqExtras :: [ByteString],
    reqKey :: ByteString,
    reqValue :: Maybe ByteString,
    reqResult :: MVar (Maybe Packet)
    }

-- | Create a new RemoteCache that connects to the given host and port.
-- The Logger logs the requests and responses.  serializeValue and
-- deserializeValue convert the entry values to and from the bytes
-- used with the remote cache.  convertExpiry converts timestamps to
-- NominalDiffTime.
newRemoteCache :: Ord t => Logger -> HostName -> ServiceName -> (e -> (Word32,ByteString)) -> ((Word32,ByteString) -> e) -> (t -> t -> NominalDiffTime) -> IO (RemoteCache t e)
newRemoteCache logger hostname service serializeValue deserializeValue convertExpiry = do
    (addrInfo:_) <- getAddrInfo Nothing (Just hostname) (Just service)
    sock <- socket (addrFamily addrInfo) Stream defaultProtocol
    connect sock (addrAddress addrInfo)
    chan <- newChan
    forkIO (process sock chan)
    return RemoteCache {
        rcSerialize = serializeValue,
        rcDeserialize = deserializeValue,
        rcConvertExpiry = convertExpiry,
        rcChan = chan
        }
  where
    process sock chan = do
        req <- readChan chan
        writeRequest logger sock (reqCmd req) 0 (reqVersion req) (reqExtras req) (Just $ reqKey req) (reqValue req)
        packet <- readPacket logger sock magicResponse
        putMVar (reqResult req) packet
        process sock chan

instance Cache RemoteCache where
    get RemoteCache { rcDeserialize = deser, rcChan = chan } key _ = do
        mVar <- newEmptyMVar
        writeChan chan Req {
                        reqCmd = cmdGet,
                        reqVersion = 0,
                        reqExtras = [],
                        reqKey = key,
                        reqValue = Nothing,
                        reqResult = mVar
                        }
        maybePacket <- takeMVar mVar
        return $ makeResult (fmap packetStatus maybePacket) (fmap packetVersion maybePacket) (fmap packetExtras maybePacket) (fmap packetValue maybePacket)
      where
        makeResult (Just status) (Just version) (Just extras) (Just (Just value))
          | status == statusNoError =
                Right (deser (getFlags (B.concat extras) 0, value), Version version)
          | status == statusKeyNotFound = Left NotFound
          | otherwise = Left UnknownError
        makeResult (Just status) _ _ _
          | status == statusKeyNotFound = Left NotFound
          | otherwise = Left UnknownError
        makeResult _ _ _ _ = Left UnknownError

    set RemoteCache { rcSerialize = ser, rcConvertExpiry = expiry, rcChan = chan } key e (Expires exp) (Timestamp t) = do
        let (flags,value) = ser e
        mVar <- newEmptyMVar
        writeChan chan Req {
                        reqCmd = cmdSet,
                        reqVersion = 0,
                        reqExtras = [makeFlags flags, makeExpiry $ expiry exp t],
                        reqKey = key,
                        reqValue = Just value,
                        reqResult = mVar
                        }
        maybePacket <- takeMVar mVar
        return $ makeResult (fmap packetStatus maybePacket) (fmap packetVersion maybePacket)
      where
        makeResult (Just status) (Just version)
          | status == statusNoError = Right (Version version)
          | otherwise = Left UnknownError
        makeResult _ _ = Left UnknownError

    add RemoteCache { rcSerialize = ser, rcConvertExpiry = expiry, rcChan = chan } key e (Expires exp) (Timestamp t) = do
        let (flags,value) = ser e
        mVar <- newEmptyMVar
        writeChan chan Req {
                        reqCmd = cmdAdd,
                        reqVersion = 0,
                        reqExtras = [makeFlags flags, makeExpiry $ expiry exp t],
                        reqKey = key,
                        reqValue = Just value,
                        reqResult = mVar
                        }
        maybePacket <- takeMVar mVar
        return $ makeResult (fmap packetStatus maybePacket) (fmap packetVersion maybePacket)
      where
        makeResult (Just status) (Just version)
          | status == statusNoError = Right (Version version)
          | status == statusKeyExists = Left EntryExists
          | otherwise = Left UnknownError
        makeResult _ _ = Left UnknownError

    replace RemoteCache { rcSerialize = ser, rcConvertExpiry = expiry, rcChan = chan } key (Version version) e (Expires exp) (Timestamp t) = do
        let (flags,value) = ser e
        mVar <- newEmptyMVar
        writeChan chan Req {
                        reqCmd = cmdReplace,
                        reqVersion = version,
                        reqExtras = [makeFlags flags, makeExpiry $ expiry exp t],
                        reqKey = key,
                        reqValue = Just value,
                        reqResult = mVar
                        }
        maybePacket <- takeMVar mVar
        return $ makeResult (fmap packetStatus maybePacket) (fmap packetVersion maybePacket)
      where
        makeResult (Just status) (Just version)
          | status == statusNoError = Right (Version version)
          | status == statusKeyNotFound = Left NotFound
          | status == statusItemNotStored = Left VersionMismatch
          | otherwise = Left UnknownError
        makeResult _ _ = Left UnknownError

    delete RemoteCache { rcChan = chan } key (Version version) _ = do
        mVar <- newEmptyMVar
        writeChan chan Req {
                        reqCmd = cmdDelete,
                        reqVersion = version,
                        reqExtras = [],
                        reqKey = key,
                        reqValue = Nothing,
                        reqResult = mVar
                        }
        maybePacket <- takeMVar mVar
        return $ makeResult (fmap packetStatus maybePacket)
      where
        makeResult (Just status)
          | status == statusNoError = Nothing
          | status == statusKeyNotFound = Just NotFound
          | status == statusItemNotStored = Just VersionMismatch
          | otherwise = Just UnknownError
        makeResult _ = Just UnknownError

    touch _ _ _ = return (Left UnknownError)
    expire _ _ = return ()
