{-# LANGUAGE OverloadedStrings, LambdaCase, OverloadedLists #-}

module Main where

import qualified ClusterBenchmark as CB

nRequests, nClients :: Int
nRequests = 100000
nClients  = 50


main :: IO ()
main = CB.clusterBenchMark
