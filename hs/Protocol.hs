-- | Protocol implements a subset of the memcache binary protocol
module Protocol(
    -- * Types
    Packet,
    -- * Functions
    readPacket, writeRequest, writeResponse,
    packetCmd, packetStatus, packetOpaque, packetVersion,
    packetExtras, packetKey, packetValue,
    makeExpiry, makeFlags, getExpiry, getFlags,
    -- * Constants
    Magic, magicRequest, magicResponse,
    Cmd, cmdGet, cmdSet, cmdAdd, cmdReplace, cmdDelete, cmdQuit,
    Status, statusNoError, statusKeyNotFound, statusKeyExists,
    statusItemNotStored, statusNotSupported, statusInternalError
    ) where

import Data.ByteString(ByteString,index,pack)
import qualified Data.ByteString as B
import Data.Maybe(catMaybes)
import Data.Time(NominalDiffTime)
import Data.Word(Word8,Word16,Word32,Word64)
import Network.Socket(Socket)
import Network.Socket.ByteString(recv,sendMany)

import Logger(Logger)

-- | Packet
data Packet = Packet ByteString [ByteString] (Maybe ByteString) (Maybe ByteString)

-- | readPacket
readPacket :: Logger -> Socket -> Magic -> IO (Maybe Packet)
readPacket logger socket (Magic magic) = do
    maybeHeader <- readChunk logger socket 24
    maybe (return Nothing) readExtras maybeHeader
  where
    readExtras header
      | header `index` 0 /= magic = return Nothing
      | extrasLength header > 0 = do
            maybeExtras <- readChunk logger socket (extrasLength header)
            maybe (return Nothing) (readKey header . Packet header . (:[])) maybeExtras
      | otherwise = readKey header (Packet header [])
    readKey header packet
      | keyLength header > 0 = do
            maybeKey <- readChunk logger socket (keyLength header)
            maybe (return Nothing) (readValue header . packet . Just) maybeKey
      | otherwise = readValue header (packet Nothing)
    readValue header packet
      | valueLength header > 0 = do
            maybeValue <- readChunk logger socket (valueLength header)
            maybe (return Nothing) (return . Just . packet . Just) maybeValue
      | otherwise = return (Just (packet Nothing))
    extrasLength header = fromIntegral (header `index` 4)
    keyLength header = fromIntegral (header `readWord16` 2)
    totalLength header = fromIntegral (header `readWord32` 8)
    valueLength header = totalLength header - extrasLength header - keyLength header

readChunk :: Logger -> Socket -> Int -> IO (Maybe ByteString)
readChunk logger socket size = readChunk' size []
  where
    readChunk' n list = do
        bytes <- recv socket n
        logger "<" bytes
        if B.length bytes == 0
          then return Nothing
          else
            if B.length bytes < n
              then readChunk' (n - B.length bytes) (bytes:list)
              else
                if null list
                  then return $ Just bytes
                  else return $ Just $ B.concat $ reverse (bytes:list)

-- | writeRequest
writeRequest :: Logger -> Socket -> Cmd -> Word32 -> Word64 -> [ByteString] -> Maybe ByteString -> Maybe ByteString -> IO ()
writeRequest logger socket (Cmd cmd) opaque version extras key value = do
    let bytes = (pack [magic, cmd,
                       keyLength `shift` 1, keyLength `shift` 0,
                       extrasLength `shift` 0, 0,
                       0, 0,
                       totalLength `shift` 3, totalLength `shift` 2,
                       totalLength `shift` 1, totalLength `shift` 0,
                       opaque `shift` 3, opaque `shift` 2,
                       opaque `shift` 1, opaque `shift` 0,
                       version `shift` 7, version `shift` 6,
                       version `shift` 5, version `shift` 4,
                       version `shift` 3, version `shift` 2,
                       version `shift` 1, version `shift` 0]
        	 : extras ++ catMaybes [key,value])
    mapM_ (logger ">") bytes
    sendMany socket bytes
  where
    Magic magic = magicRequest
    keyLength = maybe 0 B.length key
    extrasLength = sum $ map B.length extras
    totalLength = keyLength + extrasLength + maybe 0 B.length value

-- | writeResponse
writeResponse :: Logger -> Socket -> Packet -> Status -> Word64 -> [ByteString] -> Maybe ByteString -> Maybe ByteString -> IO ()
writeResponse logger socket (Packet header _ _ _) (Status status) version extras key value = do
    let bytes = (pack [magic, header `index` 1,
                       keyLength `shift` 1, keyLength `shift` 0,
                       extrasLength `shift` 0, 0,
                       status `shift` 1, status `shift` 0,
                       totalLength `shift` 3, totalLength `shift` 2,
                       totalLength `shift` 1, totalLength `shift` 0,
                       header `index` 12, header `index` 13,
                       header `index` 14, header `index` 15,
                       version `shift` 7, version `shift` 6,
                       version `shift` 5, version `shift` 4,
                       version `shift` 3, version `shift` 2,
                       version `shift` 1, version `shift` 0]
        	 : extras ++ catMaybes [key,value])
    mapM_ (logger ">") bytes
    sendMany socket bytes
  where
    Magic magic = magicResponse
    keyLength = maybe 0 B.length key
    extrasLength = sum $ map B.length extras
    totalLength = keyLength + extrasLength + maybe 0 B.length value

-- | packetCmd
packetCmd :: Packet -> Cmd
packetCmd (Packet header _ _ _) = Cmd $ header `index` 1

-- | packetStatus
packetStatus :: Packet -> Status
packetStatus (Packet header _ _ _) = Status $ header `readWord16` 6

-- | packetOpaque
packetOpaque :: Packet -> Word32
packetOpaque (Packet header _ _ _) = header `readWord32` 12

-- | packetVersion
packetVersion :: Packet -> Word64
packetVersion (Packet header _ _ _) = header `readWord64` 16

-- | packetExtras
packetExtras :: Packet -> [ByteString]
packetExtras (Packet _ extras _ _) = extras

-- | packetKey
packetKey :: Packet -> Maybe ByteString
packetKey (Packet _ _ key _) = key

-- | packetValue
packetValue :: Packet -> Maybe ByteString
packetValue (Packet _ _ _ value) = value

-- | makeExpiry
makeExpiry :: NominalDiffTime -> ByteString
makeExpiry diffTime =
    pack [dt `shift` 3, dt `shift` 2, dt `shift` 1, dt `shift` 0]
  where
    dt = floor diffTime

-- | makeFlags
makeFlags :: Word32 -> ByteString
makeFlags f = pack [f `shift` 3, f `shift` 2, f `shift` 1, f `shift` 0]

-- | getExpiry
getExpiry :: ByteString -> Int -> NominalDiffTime
getExpiry extras index = fromInteger $ fromIntegral $ extras `readWord32` index

-- | getFlags
getFlags :: ByteString -> Int -> Word32
getFlags extras index = extras `readWord32` index

shift :: (Integral i,Integral r) => i -> i -> r
shift a b = fromIntegral $ a `div` 256^b

readWord16 :: ByteString -> Int -> Word16
readWord16 bytes i =
    256*fromIntegral (bytes `index` i) + fromIntegral (bytes `index` (i + 1))

readWord32 :: ByteString -> Int -> Word32
readWord32 bytes i =
          256^3*fromIntegral (bytes `index` i)
        + 256^2*fromIntegral (bytes `index` (i + 1))
        + 256  *fromIntegral (bytes `index` (i + 2))
        +       fromIntegral (bytes `index` (i + 3))

readWord64 :: ByteString -> Int -> Word64
readWord64 bytes i =
          256^7*fromIntegral (bytes `index` i)
        + 256^6*fromIntegral (bytes `index` (i + 1))
        + 256^5*fromIntegral (bytes `index` (i + 2))
        + 256^4*fromIntegral (bytes `index` (i + 3))
        + 256^3*fromIntegral (bytes `index` (i + 4))
        + 256^2*fromIntegral (bytes `index` (i + 5))
        + 256  *fromIntegral (bytes `index` (i + 6))
        +       fromIntegral (bytes `index` (i + 7))

newtype Magic = Magic Word8

magicRequest :: Magic
magicRequest = Magic 0x80

magicResponse :: Magic
magicResponse = Magic 0x81

newtype Cmd = Cmd Word8 deriving Eq

cmdGet :: Cmd
cmdGet = Cmd 0

cmdSet :: Cmd
cmdSet = Cmd 1

cmdAdd :: Cmd
cmdAdd = Cmd 2

cmdReplace :: Cmd
cmdReplace = Cmd 3

cmdDelete :: Cmd
cmdDelete = Cmd 4

cmdQuit :: Cmd
cmdQuit = Cmd 7

newtype Status = Status Word16 deriving Eq

statusNoError :: Status
statusNoError = Status 0

statusKeyNotFound :: Status
statusKeyNotFound = Status 1

statusKeyExists :: Status
statusKeyExists = Status 2

statusItemNotStored :: Status
statusItemNotStored = Status 5

statusNotSupported :: Status
statusNotSupported = Status 131

statusInternalError :: Status
statusInternalError = Status 132
