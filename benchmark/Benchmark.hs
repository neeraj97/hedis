{-# LANGUAGE OverloadedStrings, LambdaCase, TemplateHaskell, ScopedTypeVariables #-}

module Main where

import qualified Data.ByteString.Char8 as BS
import qualified ClusterBenchmark as CB
import Data.FileEmbed (embedFile)

nRequests, nClients :: Int
nRequests = 100000
nClients  = 50

fCallLib :: BS.ByteString
fCallLib = $(embedFile "test/fcall_test.lua")

main :: IO ()
main = CB.clusterBenchMark