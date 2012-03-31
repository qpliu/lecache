-- | Listener ...
module Listener(
    -- * Functions
    listener
    ) where

import Control.Concurrent(forkIO)
import Network(PortNumber,PortID(PortNumber),withSocketsDo,listenOn)
import Network.Socket(Socket,accept)

-- | listener
listener :: PortNumber -> (Socket -> IO ()) -> IO ()
listener port handler = withSocketsDo $ do
    socket <- listenOn (PortNumber port)
    sequence_ $ repeat $ accept socket >>= (forkIO . handler . fst)
