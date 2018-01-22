{-# LANGUAGE OverloadedStrings #-}

-- | Benchmarks.

module Main where

import           Criterion.Main
import           Data.String
import qualified Database.RocksDB as Rocks
import           System.FilePath
import           System.IO.Temp

main :: IO ()
main =
  withSystemTempDirectory
    "rocks-bench"
    (\dir -> do
       db <-
         Rocks.open
           ((Rocks.defaultOptions (dir </> "demo.db"))
            {Rocks.optionsCreateIfMissing = True})
       defaultMain
         [ bgroup
             "Put"
             [ bench
               ("put " ++ show n ++ " times")
               (whnfIO
                  (ntimes n (Rocks.put db Rocks.defaultWriteOptions "foo" "bar")))
             | n <- [1, 10, 100, 1000]
             ]
         , bgroup
             "Put/Get"
             [ bench
               ("put/get " ++ show n ++ " times")
               (whnfIO
                  (ntimes
                     n
                     (do Rocks.put db Rocks.defaultWriteOptions "foo" "bar"
                         _ <- Rocks.get db Rocks.defaultReadOptions "foo"
                         pure ())))
             | n <- [1, 10, 100, 1000]
             ]
         , bgroup
             "Write"
             [ bench
               ("write " ++ show n ++ " items")
               (whnfIO
                  (ntimes
                     n
                     (Rocks.write
                        db
                        Rocks.defaultWriteOptions
                        [Rocks.Put "foo" (fromString (show i)) | i <- [0 .. n]])))
             | n <- [1, 10, 100, 1000]
             ]
         , bgroup
             "Iterate"
             [ bench
               ("iterate " ++ show n ++ " items")
               (whnfIO
                  (ntimes
                     n
                     (do Rocks.write
                           db
                           Rocks.defaultWriteOptions
                           [Rocks.Put "foo" (fromString (show i)) | i <- [0 .. n]]
                         iter <- Rocks.createIter db Rocks.defaultReadOptions
                         Rocks.iterSeek iter "foo"
                         let loop = do
                               mv <- Rocks.iterEntry iter
                               case mv of
                                 Just _ -> do
                                   Rocks.iterNext iter
                                   loop
                                 Nothing -> pure ()
                         loop
                         Rocks.releaseIter iter
                         pure ())))
             | n <- [1, 10, 100, 1000]
             ]
         ]
       Rocks.close db)

ntimes :: Int -> IO () -> IO ()
ntimes n0 m = go n0
  where
    go 0 = pure ()
    go n = m >> go (n - 1)
