{-# LANGUAGE OverloadedStrings, LambdaCase, BangPatterns, BlockArguments #-}

module ClusterBenchmark where

import Control.Concurrent
import Control.Monad
import Control.Monad.Trans
import Data.Time
import Database.Redis hiding (append)
import qualified Data.ByteString.Char8 as BS
import System.Environment (lookupEnv)
import Data.Maybe (fromMaybe)
import Text.Read (readMaybe)

clusterBenchMark :: IO ()
clusterBenchMark = do
    ----------------------------------------------------------------------
    -- Preparation
    --
    nClients <- fromMaybe 128 . (>>= readMaybe) <$> lookupEnv "NUM_CLIENTS"
    perClientNumRequests <- fromMaybe 10000 . (>>= readMaybe) <$> lookupEnv "NUM_REQ_PER_CLIENT"
    maxConnections <- fromMaybe 50 . (>>= readMaybe) <$> lookupEnv "MAX_CONNECTIONS"
    host <- fromMaybe "localhost" . (>>= readMaybe) <$> lookupEnv "HOST"
    port <- fromMaybe 30001 . (>>= readMaybe) <$> lookupEnv "PORT"
    let  connectInfo = defaultClusterConnectInfo{
              connectHost = host, 
              connectPort = PortNumber port,
              connectMaxConnections = maxConnections
            }
    print ("" :: String)
    print ("----------------------------------------------" :: String)
    print ("Connection Info:" :: String)
    print connectInfo
    print ("Number of clients:" :: String)
    print nClients
    print ("Number of requests per client:" :: String)
    print perClientNumRequests
    print ("----------------------------------------------" :: String)
    print ("" :: String)
    conn <- connectCluster connectInfo
    runRedis conn $ do
        _ <- flushall
        _ <- ping >>= \case
          Right _ -> return ()
          Left e -> error $ show e
        _ <- del ["somestream"]
        _ <- del ["somestream1"]
        xadd "somestream" "123" (keyValueGenerator 50 "k" "v")>>= \case
          Left x -> error $ show x
          _ -> do
            sadd "k51" ["v51"] >>= \case
                Left _ -> error "error"
                _ -> return ()
        return ()
    
    ----------------------------------------------------------------------
    -- Spawn clients
    --
    start <- newEmptyMVar
    done  <- newEmptyMVar
    replicateM_ nClients $ forkIO $ do
        forever $ do
          startT <- getCurrentTime
          (reps,action) <- liftIO $ takeMVar start
          replicateM_ reps action
          stopT <- getCurrentTime
          liftIO $ putMVar done $ diffUTCTime stopT startT
    
    let timeAction _name nActions action = do
          startT <- getCurrentTime
          -- each clients runs ACTION nRepetitions times
          mapM_ (\i -> putMVar start (nActions,action $ BS.pack ("stream-{"++ show i ++"}"))) [1..nClients]
          timePerClient <- replicateM nClients $ takeMVar done
          stopT <- getCurrentTime
          print ("Total Clock Time Taken for benchmark" :: String)
          print $ diffUTCTime stopT startT
          print ("Total Clock Time Taken for benchmark Per Client" :: String)
          print timePerClient

    ----------------------------------------------------------------------
    -- Benchmarks
    --
    timeAction ("XREAD and XDEL"::String) perClientNumRequests $ \key -> do
        xreadResponses <- runRedis conn $ xreadOpts [(key,"0-0")] (XReadOpts { block = Nothing, recordCount = Just 1000, noack = False}) >>= \case
            Right (Just a) -> return a
            Right Nothing -> return []
            _ -> error "error"
        !ids <- return $ map recordId $ concatMap records xreadResponses  
        if length ids== 0
          then return ()
            else
              runRedis conn $ xdel key ids >>= \case
                  Left _ -> error "error"
                  Right count -> if count == fromIntegral (length ids)
                                  then return ()
                                  else error "error"
        return ()

keyGenerator :: Int -> BS.ByteString -> [BS.ByteString]
keyGenerator 0 _  = []
keyGenerator x key =
    let bSx = (BS.pack $ show x)
    in [BS.append key bSx] <> keyGenerator (x-1) key


keyValueGenerator :: Int -> BS.ByteString -> BS.ByteString -> [(BS.ByteString, BS.ByteString)]
keyValueGenerator 0 _ _  = []
keyValueGenerator x key val =
    let bSx = (BS.pack $ show x)
    in [(BS.append key bSx, BS.append val bSx)] <> keyValueGenerator (x-1) key val


keyListValueGenerator :: Int -> Int -> BS.ByteString -> BS.ByteString -> [(BS.ByteString, [BS.ByteString])]
keyListValueGenerator 0 _ _ _  = []
keyListValueGenerator keyL valL key val =
    let bSKeyL = (BS.pack $ show keyL)
    in [(BS.append key bSKeyL, getValueList (BS.append val bSKeyL))] <> keyListValueGenerator (keyL-1) valL key val
    where
        getValueList value = map (\ x -> BS.append value (BS.pack $ show x)) [1..valL]
