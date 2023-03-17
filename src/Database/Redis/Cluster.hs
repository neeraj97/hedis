{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RecordWildCards #-}
module Database.Redis.Cluster
  ( Connection(..)
  , NodeRole(..)
  , NodeConnection(..)
  , Node(..)
  , ShardMap(..)
  , HashSlot
  , Shard(..)
  , TimeoutException(..)
  , TcpInfo(..)
  , Host
  , NodeID
  , connect
  , destroyNodeResources
  , requestPipelined
  , nodes
  , Pipeline
  , newPipelineVar
  , NodeResource(..)
) where

import qualified Data.ByteString as B
import Data.Char(toLower)
import qualified Data.ByteString.Char8 as Char8
import qualified Data.IORef as IOR
import Data.Maybe(listToMaybe, mapMaybe, fromMaybe)
import Data.List(nub, sortBy, find)
import Data.Map(fromListWith, assocs)
import Data.Function(on)
import Control.Exception(Exception, SomeException, throwIO, BlockedIndefinitelyOnMVar(..), catches, Handler(..), try, fromException)
import Data.Pool(Pool, createPool, withResource, destroyAllResources)
import Control.Concurrent.Async(race)
import Control.Concurrent(threadDelay)
import Control.Concurrent.MVar(MVar, newMVar, readMVar, modifyMVar, modifyMVar_)
import Control.Monad(zipWithM, when)
import Database.Redis.Cluster.HashSlot(HashSlot, keyToSlot)
import qualified Database.Redis.ConnectionContext as CC
import qualified Data.HashMap.Strict as HM
import qualified Data.IntMap.Strict as IntMap
import           Data.Typeable
import qualified Data.Time.Clock as Time
import qualified Scanner
import System.IO.Unsafe(unsafeInterleaveIO)
import System.Environment(lookupEnv)
import Say(sayString)
import Text.Read (readMaybe)

import Database.Redis.Protocol(Reply(Error), renderRequest, reply)
import qualified Database.Redis.Cluster.Command as CMD
import Network.TLS (ClientParams)

-- This module implements a clustered connection whilst maintaining
-- compatibility with the original Hedis codebase. In particular it still
-- performs implicit pipelining using `unsafeInterleaveIO` as the single node
-- codebase does. To achieve this each connection carries around with it a
-- pipeline of commands. Every time `sendRequest` is called the command is
-- added to the pipeline and an IO action is returned which will, upon being
-- evaluated, execute the entire pipeline. If the pipeline is already executed
-- then it just looks up it's response in the executed pipeline.

-- | A connection to a redis cluster, it is compoesed of a map from Node IDs to
-- | 'NodeConnection's, a 'Pipeline', and a 'ShardMap'
type IsReadOnly = Bool

data Connection = Connection (MVar NodeConnectionMap) (MVar ShardMap) CMD.InfoMap IsReadOnly TcpInfo

-- | A connection to a single node in the cluster, similar to 'ProtocolPipelining.Connection'
data NodeConnection = NodeConnection (Pool NodeResource) NodeID

newtype NodeResource = NodeResource (CC.ConnectionContext, (IOR.IORef (Maybe B.ByteString)))

instance Show NodeConnection where
    show (NodeConnection _ id1) = "nodeId: " <> show id1

instance Eq NodeConnection where
    (NodeConnection _ id1) == (NodeConnection _ id2) = id1 == id2

instance Ord NodeConnection where
    compare (NodeConnection _ id1) (NodeConnection _ id2) = compare id1 id2

data PipelineState = Pending [[B.ByteString]] | Executed [Reply] | TransactionPending [[B.ByteString]]
newtype Pipeline = Pipeline (MVar PipelineState)

data NodeRole = Master | Slave deriving (Show, Eq, Ord)

type Host = String
type Port = Int
type NodeID = B.ByteString
data Node = Node NodeID NodeRole Host Port deriving (Show, Eq, Ord)

type MasterNode = Node
type SlaveNode = Node
data Shard = Shard MasterNode [SlaveNode] deriving (Show, Eq, Ord)

newtype ShardMap = ShardMap (IntMap.IntMap Shard) deriving (Show)

type NodeConnectionMap = HM.HashMap NodeID NodeConnection

-- Object for storing Tcp Connection Info which will be used when cluster is refreshed
data TcpInfo = TcpInfo
    { connectAuth           :: Maybe B.ByteString
    , connectTLSParams      :: Maybe ClientParams
    , idleTime              :: Time.NominalDiffTime
    , maxResources          :: Int
    , timeoutOpt            :: Maybe Int
    } deriving Show

newtype MissingNodeException = MissingNodeException [B.ByteString] deriving (Show, Typeable)
instance Exception MissingNodeException

newtype UnsupportedClusterCommandException = UnsupportedClusterCommandException [B.ByteString] deriving (Show, Typeable)
instance Exception UnsupportedClusterCommandException

newtype CrossSlotException = CrossSlotException [[B.ByteString]] deriving (Show, Typeable)
instance Exception CrossSlotException

data NoNodeException = NoNodeException deriving (Show, Typeable)
instance Exception NoNodeException

data TimeoutException = TimeoutException String deriving (Show, Typeable)
instance Exception TimeoutException

data RefreshNeededException = NodeMissing [B.ByteString]
    | AllNodeMissing
    | RequestTimingOut [[B.ByteString]]
    deriving (Show, Typeable)
instance Exception RefreshNeededException

connect :: (Host -> CC.PortID -> Maybe Int -> IO CC.ConnectionContext) -> [CMD.CommandInfo] -> MVar ShardMap -> Bool -> ([NodeConnection] -> IO ShardMap) -> TcpInfo -> IO Connection
connect withAuth commandInfos shardMapVar isReadOnly refreshShardMap (tcpInfo@TcpInfo{ timeoutOpt, maxResources, idleTime }) = do
        shardMap <- readMVar shardMapVar
        (eNodeConns, shouldRetry) <- nodeConnections shardMap
        -- whenever one of the node connection is not established,
        -- will refresh the slots and retry node connections.
        -- This would handle fail over, IP change use cases.
        nodeConns <-
          if shouldRetry
            then if not (HM.null eNodeConns)
                    then do
                      newShardMap <- refreshShardMap (HM.elems eNodeConns)
                      refreshShardMapVar newShardMap
                      simpleNodeConnections newShardMap
                    else
                      throwIO NoNodeException
            else
              return eNodeConns
        nodeConnsVar <- newMVar nodeConns
        return $ Connection nodeConnsVar shardMapVar (CMD.newInfoMap commandInfos) isReadOnly tcpInfo where
    simpleNodeConnections :: ShardMap -> IO (HM.HashMap NodeID NodeConnection)
    simpleNodeConnections shardMap = HM.fromList <$> mapM connectNode (nub $ nodes shardMap)
    nodeConnections :: ShardMap -> IO (HM.HashMap NodeID NodeConnection, Bool)
    nodeConnections shardMap = do
      info <- mapM (try . connectNode) (nub $ nodes shardMap)
      return $
        foldl (\(acc, accB) x -> case x of
                    Right (v, nc) -> (HM.insert v nc acc, accB)
                    Left (_ :: SomeException) -> (acc, True)
           ) (mempty, False) info
    connectNode :: Node -> IO (NodeID, NodeConnection)
    connectNode node@(Node n _ _ _) = do
        pool <- createPool (createNodeResource node) destroyNodeResource 1 idleTime maxResources
        return (n, NodeConnection pool n)
    createNodeResource :: Node -> IO NodeResource
    createNodeResource (Node _ _ host port) = do
        ctx <- withAuth host (CC.PortNumber $ toEnum port) timeoutOpt
        ref <- IOR.newIORef Nothing
        return $ NodeResource (ctx, ref)
    destroyNodeResource :: NodeResource -> IO ()
    destroyNodeResource (NodeResource (ctx, _)) = CC.disconnect ctx
    refreshShardMapVar :: ShardMap -> IO ()
    refreshShardMapVar shardMap = hasLocked "refreshShardMapVar in Cluster.connect" $ modifyMVar_ shardMapVar (const (pure shardMap))

newPipelineVar :: IO (MVar Pipeline)
newPipelineVar = do
        stateVar <- newMVar $ Pending []
        newMVar $ Pipeline stateVar

destroyNodeResources :: Connection -> IO ()
destroyNodeResources (Connection nodeConnMapVar _ _ _ _) = (readMVar nodeConnMapVar) >>= (mapM_ disconnectNode . HM.elems) where
    disconnectNode (NodeConnection nodePool _) = destroyAllResources nodePool

-- Add a request to the current pipeline for this connection. The pipeline will
-- be executed implicitly as soon as any result returned from this function is
-- evaluated.
requestPipelined :: IO ShardMap -> Connection -> MVar Pipeline -> [B.ByteString] -> IO Reply
requestPipelined refreshShardmapAction conn@(Connection _ shardMapVar _ _ _) pipelineVar nextRequest = modifyMVar pipelineVar $ \(Pipeline stateVar) -> do
    (newStateVar, repliesIndex) <- hasLocked "modify Pipeline stateVar in evaluatePipeline" $ modifyMVar stateVar $ \case
        Pending requests | isMulti nextRequest -> do
            replies <- evaluatePipeline shardMapVar refreshShardmapAction conn requests
            s' <- newMVar $ TransactionPending [nextRequest]
            return (Executed replies, (s', 0))
        Pending requests | length requests > 1000 -> do
            replies <- evaluatePipeline shardMapVar refreshShardmapAction conn (nextRequest:requests)
            return (Executed replies, (stateVar, length requests))
        Pending requests ->
            return (Pending (nextRequest:requests), (stateVar, length requests))
        TransactionPending requests ->
            if isExec nextRequest then do
              replies <- evaluateTransactionPipeline shardMapVar refreshShardmapAction conn (nextRequest:requests)
              return (Executed replies, (stateVar, length requests))
            else
              return (TransactionPending (nextRequest:requests), (stateVar, length requests))
        e@(Executed _) -> do
            s' <- newMVar $ Pending [nextRequest]
            return (e, (s', 0))
    evaluateAction <- unsafeInterleaveIO $ do
        replies <- hasLocked "locked evaluating replies" $ modifyMVar newStateVar $ \case
            Executed replies -> return (Executed replies, replies)
            Pending requests-> do
                replies <- evaluatePipeline shardMapVar refreshShardmapAction conn requests
                return (Executed replies, replies)
            TransactionPending requests-> do
                replies <- evaluateTransactionPipeline shardMapVar refreshShardmapAction conn requests
                return (Executed replies, replies)
        return $ replies !! repliesIndex
    return (Pipeline newStateVar, evaluateAction)

isMulti :: [B.ByteString] -> Bool
isMulti ("MULTI" : _) = True
isMulti _ = False

isExec :: [B.ByteString] -> Bool
isExec ("EXEC" : _) = True
isExec _ = False

data PendingRequest = PendingRequest Int [B.ByteString]
data CompletedRequest = CompletedRequest Int [B.ByteString] Reply

rawRequest :: PendingRequest -> [B.ByteString]
rawRequest (PendingRequest _ r) =  r

responseIndex :: CompletedRequest -> Int
responseIndex (CompletedRequest i _ _) = i

rawResponse :: CompletedRequest -> Reply
rawResponse (CompletedRequest _ _ r) = r

requestForResponse :: CompletedRequest -> [B.ByteString]
requestForResponse (CompletedRequest _ r _) = r

-- The approach we take here is similar to that taken by the redis-py-cluster
-- library, which is described at https://redis-py-cluster.readthedocs.io/en/master/pipelines.html
--
-- Essentially we group all the commands by node (based on the current shardmap)
-- and then execute a pipeline for each node (maintaining the order of commands
-- on a per node basis but not between nodes). Once we've done this, if any of
-- the commands have resulted in a MOVED error we refresh the shard map, then
-- we run through all the responses and retry any MOVED or ASK errors. This retry
-- step is not pipelined, there is a request per error. This is probably
-- acceptable in most cases as these errors should only occur in the case of
-- cluster reconfiguration events, which should be rare.
evaluatePipeline :: MVar ShardMap -> IO ShardMap -> Connection -> [[B.ByteString]] -> IO [Reply]
evaluatePipeline shardMapVar refreshShardmapAction conn requests = do
        shardMap <- hasLocked "reading shardmap in evaluatePipeline" $ readMVar shardMapVar
        erequestsByNode <- try $ getRequestsByNode shardMap
        requestsByNode <- case erequestsByNode of
            Right reqByNode-> pure reqByNode
            Left (_ :: RefreshNeededException) -> do
                putStrLn "Handling RefreshNeededException"
                refreshShardMapVar
                newShardMap <- readMVar shardMapVar
                getRequestsByNode newShardMap
        -- catch the exception thrown at each node level
        -- send the command to random node.
        -- merge the current responses with new responses.
        eresps <- mapM (try . uncurry executeRequests) requestsByNode
        -- take a random connection where there are no exceptions.
        -- PERF_CONCERN: Since usually we send only one request at time, this won't be
        -- heavy perf issue. but still should be evaluated and figured out with complete rewrite.

        -- throwing exception for timeouts thus closing the connection instead of retrying.
        -- otherwise if there is any response in the connection buffer it'll get forwarded to other requests that are reusing the same connection.
        -- leading to jumbled up responses
        resps <- concat <$>
          mapM (\(resp, (cc, r)) -> case resp of
              Right v -> return v
              Left (err :: SomeException) ->
                case fromException err of
                  Just (er :: TimeoutException) -> throwIO er
                  _ -> getRandomConnection cc conn >>= (`executeRequests` r)
            ) (zip eresps requestsByNode)
        -- check for any moved in both responses and continue the flow.
        when (any (moved . rawResponse) resps) refreshShardMapVar
        retriedResps <- mapM (retry 0) resps
        return $ map rawResponse $ sortBy (on compare responseIndex) retriedResps
  where
    getRequestsByNode :: ShardMap -> IO [(NodeConnection, [PendingRequest])]
    getRequestsByNode shardMap = do
        commandsWithNodes <- zipWithM (requestWithNode shardMap) (reverse [0..(length requests - 1)]) requests
        return $ assocs $ fromListWith (++) commandsWithNodes
    requestWithNode :: ShardMap -> Int -> [B.ByteString] -> IO (NodeConnection, [PendingRequest])
    requestWithNode shardMap index request = do
        nodeConn <- nodeConnectionForCommand conn shardMap request
        return (nodeConn, [PendingRequest index request])
    executeRequests :: NodeConnection -> [PendingRequest] -> IO [CompletedRequest]
    executeRequests nodeConn nodeRequests = do
        replies <- requestNode nodeConn $ map rawRequest nodeRequests
        return $ map (\(PendingRequest i r, rep) -> CompletedRequest i r rep) (zip nodeRequests replies)
    retry :: Int -> CompletedRequest -> IO CompletedRequest
    retry retryCount resp@(CompletedRequest index request thisReply) = do
        retryReply <- case thisReply of
            (Error errString) | B.isPrefixOf "MOVED" errString -> do
                shardMap <- hasLocked "reading shard map in retry MOVED" $ readMVar shardMapVar
                nodeConn <- nodeConnectionForCommand conn shardMap (requestForResponse resp)
                head <$> requestNode nodeConn [request]
            (askingRedirection -> Just (host, port)) -> do
                shardMap <- hasLocked "reading shardmap in retry ASK" $ readMVar shardMapVar
                maybeAskNode <- nodeConnWithHostAndPort shardMap conn host port
                case maybeAskNode of
                    Just askNode -> last <$> requestNode askNode [["ASKING"], requestForResponse resp]
                    Nothing -> case retryCount of
                        0 -> do
                            _ <- refreshShardMapVar
                            rawResponse <$> retry (retryCount + 1) resp
                        _ -> throwIO $ MissingNodeException (requestForResponse resp)
            _ -> return thisReply
        return (CompletedRequest index request retryReply)
    refreshShardMapVar :: IO ()
    refreshShardMapVar = refreshShardmapAction >> return ()

-- Retry a batch of requests if any of the responses is a redirect instruction.
-- If multiple requests are passed in they're assumed to be a MULTI..EXEC
-- transaction and will all be retried.
retryBatch :: MVar ShardMap -> IO ShardMap -> Connection -> Int -> [[B.ByteString]] -> [Reply] -> IO [Reply]
retryBatch shardMapVar refreshShardmapAction conn retryCount requests replies =
    -- The last reply will be the `EXEC` reply containing the redirection, if
    -- there is one.
    case last replies of
        (Error errString) | B.isPrefixOf "MOVED" errString -> do
            let (Connection _ _ infoMap _ _) = conn
            keys <- mconcat <$> mapM (requestKeys infoMap) requests
            hashSlot <- hashSlotForKeys (CrossSlotException requests) keys
            nodeConn <- nodeConnForHashSlot shardMapVar conn (MissingNodeException (head requests)) hashSlot
            requestNode nodeConn requests
        (askingRedirection -> Just (host, port)) -> do
            shardMap <- readMVar shardMapVar
            maybeAskNode <- nodeConnWithHostAndPort shardMap conn host port
            case maybeAskNode of
                Just askNode -> tail <$> requestNode askNode (["ASKING"] : requests)
                Nothing -> case retryCount of
                    0 -> do
                        _ <- refreshShardmapAction
                        retryBatch shardMapVar refreshShardmapAction conn (retryCount + 1) requests replies
                    _ -> throwIO $ MissingNodeException (head requests)
        _ -> return replies

-- Like `evaluateOnPipeline`, except we expect to be able to run all commands
-- on a single shard. Failing to meet this expectation is an error.
evaluateTransactionPipeline :: MVar ShardMap -> IO ShardMap -> Connection -> [[B.ByteString]] -> IO [Reply]
evaluateTransactionPipeline shardMapVar refreshShardmapAction conn requests' = do
    let requests = reverse requests'
    let (Connection _ _ infoMap _ _) = conn
    keys <- mconcat <$> mapM (requestKeys infoMap) requests
    -- In cluster mode Redis expects commands in transactions to all work on the
    -- same hashslot. We find that hashslot here.
    -- We could be more permissive and allow transactions that touch multiple
    -- hashslots, as long as those hashslots are on the same node. This allows
    -- a new failure case though: if some of the transactions hashslots are
    -- moved to a different node we could end up in a situation where some of
    -- the commands in a transaction are applied and some are not. Better to
    -- fail early.
    hashSlot <- hashSlotForKeys (CrossSlotException requests) keys
    nodeConn <- nodeConnForHashSlot shardMapVar conn (MissingNodeException (head requests)) hashSlot
    -- catch the exception thrown, send the command to random node.
    -- This change is required to handle the cluster topology change.
    eresps <- try $ requestNode nodeConn requests
    resps <-
      case eresps of
        Right v -> return v
        Left (_ :: SomeException) -> getRandomConnection nodeConn conn >>= (`requestNode` requests)
    -- The Redis documentation has the following to say on the effect of
    -- resharding on multi-key operations:
    --
    --     Multi-key operations may become unavailable when a resharding of the
    --     hash slot the keys belong to is in progress.
    --
    --     More specifically, even during a resharding the multi-key operations
    --     targeting keys that all exist and all still hash to the same slot
    --     (either the source or destination node) are still available.
    --
    --     Operations on keys that don't exist or are - during the resharding -
    --     split between the source and destination nodes, will generate a
    --     -TRYAGAIN error. The client can try the operation after some time,
    --     or report back the error.
    --
    --     https://redis.io/topics/cluster-spec#multiple-keys-operations
    --
    -- An important take-away here is that MULTI..EXEC transactions can fail
    -- with a redirect in which case we need to repeat the full transaction on
    -- the node we're redirected too.
    --
    -- A second important takeway is that MULTI..EXEC transactions might
    -- temporarily fail during resharding with a -TRYAGAIN error. We can only
    -- make arbitrary decisions about how long to paus before the retry and how
    -- often to retry, so instead we'll propagate the error to the library user
    -- and let them decide how they would like to handle the error.
    when (any moved resps)
      (refreshShardmapAction >> return ())
    retriedResps <- retryBatch shardMapVar refreshShardmapAction conn 0 requests resps
    return retriedResps

nodeConnForHashSlot :: Exception e => MVar ShardMap -> Connection -> e -> HashSlot -> IO NodeConnection
nodeConnForHashSlot shardMapVar conn exception hashSlot = do
    let (Connection nodeConnsVar _ _ _ _) = conn
    ShardMap shardMap <- readMVar shardMapVar
    nodeConns <- readMVar nodeConnsVar
    node <-
        case IntMap.lookup (fromEnum hashSlot) shardMap of
            Nothing -> throwIO exception
            Just (Shard master _) -> return master
    case HM.lookup (nodeId node) nodeConns of
        Nothing -> throwIO exception
        Just nodeConn' -> return nodeConn'

hashSlotForKeys :: Exception e => e -> [B.ByteString] -> IO HashSlot
hashSlotForKeys exception keys =
    case nub (keyToSlot <$> keys) of
        -- If none of the commands contain a key we can send them to any
        -- node. Let's pick the first one.
        [] -> return 0
        [hashSlot] -> return hashSlot
        _ -> throwIO $ exception

requestKeys :: CMD.InfoMap -> [B.ByteString] -> IO [B.ByteString]
requestKeys infoMap request =
    case CMD.keysForRequest infoMap request of
        Nothing -> throwIO $ UnsupportedClusterCommandException request
        Just k -> return k

askingRedirection :: Reply -> Maybe (Host, Port)
askingRedirection (Error errString) = case Char8.words errString of
    ["ASK", _, hostport] -> case Char8.split ':' hostport of
       [host, portString] -> case Char8.readInt portString of
         Just (port,"") -> Just (Char8.unpack host, port)
         _ -> Nothing
       _ -> Nothing
    _ -> Nothing
askingRedirection _ = Nothing

moved :: Reply -> Bool
moved (Error errString) = case Char8.words errString of
    "MOVED":_ -> True
    _ -> False
moved _ = False


nodeConnWithHostAndPort :: ShardMap -> Connection -> Host -> Port -> IO (Maybe NodeConnection)
nodeConnWithHostAndPort shardMap (Connection nodeConnsVar _ _ _ _) host port =
    case nodeWithHostAndPort shardMap host port of
        Nothing -> return Nothing
        Just node -> do
            nodeConns <- readMVar nodeConnsVar
            return (HM.lookup (nodeId node) nodeConns)

nodeConnectionForCommand :: Connection -> ShardMap -> [B.ByteString] -> IO NodeConnection
nodeConnectionForCommand (Connection nodeConnsMVar _ infoMap connReadOnly _) (ShardMap shardMap) request = do
    let mek = case request of
          ("MULTI" : key : _) -> Just [key]
          ("EXEC" : key : _) -> Just [key]
          _ -> Nothing
        isCmdReadOnly = isCommandReadonly infoMap request
    keys <- case CMD.keysForRequest infoMap request of
        Nothing -> throwIO $ UnsupportedClusterCommandException request
        Just k -> return k
    let shards = nub $ mapMaybe ((flip IntMap.lookup shardMap) . fromEnum . keyToSlot) (fromMaybe keys mek)
    node <- case (shards, connReadOnly) of
        ([],_) -> throwIO $ MissingNodeException request
        ([Shard master _], False) ->
            return master
        ([Shard master []], True) ->
            return master
        ([Shard master (slave: _)], True) ->
            if isCmdReadOnly
                then return slave
                else return master
        _ -> throwIO $ CrossSlotException [request]
    nodeConns <- readMVar nodeConnsMVar
    maybe (throwIO $ MissingNodeException request) return (HM.lookup (nodeId node) nodeConns)
    where
        isCommandReadonly :: CMD.InfoMap -> [B.ByteString] -> Bool
        isCommandReadonly (CMD.InfoMap iMap) (command: _) =
            let
                info = HM.lookup (map toLower $ Char8.unpack command) iMap
            in maybe False (CMD.ReadOnly `elem`) (CMD.flags <$> info)
        isCommandReadonly _ _ = False

cleanRequest :: [B.ByteString] -> [B.ByteString]
cleanRequest ("MULTI" : _) = ["MULTI"]
cleanRequest ("EXEC" : _) = ["EXEC"]
cleanRequest req = req

requestNode :: NodeConnection -> [[B.ByteString]] -> IO [Reply]
requestNode (NodeConnection pool _) requests = withResource pool (`requestNodeResource` requests)

requestNodeResource :: NodeResource -> [[B.ByteString]] -> IO [Reply]
requestNodeResource (NodeResource (ctx, lastRecvRef)) requests = do
    envTimeout <- round . (\x -> (x :: Time.NominalDiffTime) * 1000000) . realToFrac . fromMaybe (0.5 :: Double) . (>>= readMaybe) <$> lookupEnv "REDIS_REQUEST_NODE_TIMEOUT"
    eresp <- race requestNodeImpl (threadDelay envTimeout)
    case eresp of
      Left e -> return e
      Right _ -> putStrLn "timeout happened" *> throwIO (RequestTimingOut requests)
    where
    requestNodeImpl :: IO [Reply]
    requestNodeImpl = do
        let reqs = map cleanRequest requests
        mapM_ (sendNode . renderRequest) reqs
        _ <- CC.flush ctx
        sequence $ take (length requests) (repeat recvNode)
    sendNode :: B.ByteString -> IO ()
    sendNode = CC.send ctx
    recvNode :: IO Reply
    recvNode = do
        maybeLastRecv <- IOR.readIORef lastRecvRef
        scanResult <- case maybeLastRecv of
            Just lastRecv -> Scanner.scanWith (CC.recv ctx) reply lastRecv
            Nothing -> Scanner.scanWith (CC.recv ctx) reply B.empty

        case scanResult of
            Scanner.Fail{}       -> CC.errConnClosed
            Scanner.More{}    -> error "Hedis: parseWith returned Partial"
            Scanner.Done rest' r -> do
                IOR.writeIORef lastRecvRef (Just rest')
                return r

{-# INLINE nodes #-}
nodes :: ShardMap -> [Node]
nodes (ShardMap shardMap) = concatMap snd $ IntMap.toList $ fmap shardNodes shardMap where
    shardNodes :: Shard -> [Node]
    shardNodes (Shard master slaves) = master:slaves


nodeWithHostAndPort :: ShardMap -> Host -> Port -> Maybe Node
nodeWithHostAndPort shardMap host port = listToMaybe $ filter (\(Node _ _ nodeHost nodePort) -> port == nodePort && host == nodeHost) $ nodes shardMap

nodeId :: Node -> NodeID
nodeId (Node theId _ _ _) = theId

hasLocked :: String -> IO a -> IO a
hasLocked msg action =
  action `catches`
  [ Handler $ \exc@BlockedIndefinitelyOnMVar -> sayString ("[MVar]: " ++ msg) >> throwIO exc
  ]

getRandomConnection :: NodeConnection -> Connection -> IO NodeConnection
getRandomConnection nc conn = do
  let (Connection hmnVar _ _ _ _) = conn
  hmn <- readMVar hmnVar
  let conns = HM.elems hmn
  return $ fromMaybe (head conns) $ find (nc /= ) conns