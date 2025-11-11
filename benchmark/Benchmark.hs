{-# LANGUAGE OverloadedStrings, LambdaCase, OverloadedLists #-}

module Main where

import qualified Data.ByteString.Char8 as BS
import qualified ClusterBenchmark as CB
import Data.FileEmbed (embedFile)

nRequests, nClients :: Int
nRequests = 100000
nClients  = 50


main :: IO ()
main = CB.clusterBenchMark
