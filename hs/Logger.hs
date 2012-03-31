-- | Logger
module Logger(
    -- * Types
    Logger,
    -- * Functions
    nullLogger,debugLogger
    ) where

import Data.ByteString(ByteString,index)
import qualified Data.ByteString as B
import Data.Time(getCurrentTime)
import Debug.Trace(putTraceMsg)
import Text.Printf(printf)

type Logger = String -> ByteString -> IO ()

nullLogger :: Logger
nullLogger _ _ = return ()

debugLogger :: String -> Logger
debugLogger prefix marker bytes = do
    t <- getCurrentTime
    printBytes 0 $ printf "%s %s %s" prefix marker (show t)
  where
    printBytes i line
      | i + 16 < B.length bytes = do
            putTraceMsg line
            printBytes (i + 16) $ printf "%s %s %.2x%.2x%.2x%.2x %.2x%.2x%.2x%.2x %.2x%.2x%.2x%.2x %.2x%.2x%.2x%.2x"
                    prefix marker
                    (bytes `index` i)        (bytes `index` (i + 1))
                    (bytes `index` (i + 2))  (bytes `index` (i + 3))
                    (bytes `index` (i + 4))  (bytes `index` (i + 5))
                    (bytes `index` (i + 6))  (bytes `index` (i + 7))
                    (bytes `index` (i + 8))  (bytes `index` (i + 9))
                    (bytes `index` (i + 10)) (bytes `index` (i + 11))
                    (bytes `index` (i + 12)) (bytes `index` (i + 13))
                    (bytes `index` (i + 14)) (bytes `index` (i + 15))
      | i >= B.length bytes = putTraceMsg line
      | i `mod` 16 == 0 = do
            putTraceMsg line
            printBytes (i + 1) $ printf "%s %s %.2x" prefix marker (bytes `index` i)
      | i `mod` 4 == 0 =
            printBytes (i + 1) $ line ++ printf " %.2x" (bytes `index` i)
      | otherwise =
            printBytes (i + 1) $ line ++ printf "%.2x" (bytes `index` i)
