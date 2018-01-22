{-# LANGUAGE CPP #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Comprehensive aggressive test-suite.

module Main where

import           Control.DeepSeq
import           Control.Exception
import           Control.Monad.IO.Class
import           Data.Bifunctor
import qualified Database.RocksDB as Rocks
import           System.Directory
import           System.FilePath
import           System.IO.Temp
import           Test.Hspec
import           Test.QuickCheck

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "Open/create/close" open
  describe "Get/put" getput
  describe "Obscure conditions" obscure

getput :: Spec
getput = do
  it
    "Get/put succeeds"
    (do let key = "some key"
            val = "Hello, World!"
        result <-
          withTempDirCleanedUp
            (\dir -> do
               dbh <-
                 Rocks.open
                   ((Rocks.defaultOptions (dir </> "demo.db"))
                    {Rocks.optionsCreateIfMissing = True})
               Rocks.put dbh Rocks.defaultWriteOptions key val
               v <- Rocks.get dbh Rocks.defaultReadOptions key
               Rocks.close dbh
               pure v)
        shouldBe result (Just val))
  it
    "Overwrite key works"
    (do let key = "some key"
            val = "Hello, World!"
        result <-
          withTempDirCleanedUp
            (\dir -> do
               dbh <-
                 Rocks.open
                   ((Rocks.defaultOptions (dir </> "demo.db"))
                    {Rocks.optionsCreateIfMissing = True})
               Rocks.put dbh Rocks.defaultWriteOptions key "doomed"
               Rocks.put dbh Rocks.defaultWriteOptions key val
               v <- Rocks.get dbh Rocks.defaultReadOptions key
               Rocks.close dbh
               pure v)
        shouldBe result (Just val))
  it
    "Non-existent key returns Nothing"
    (do let key = "some key"
        result <-
          withTempDirCleanedUp
            (\dir -> do
               dbh <-
                 Rocks.open
                   ((Rocks.defaultOptions (dir </> "demo.db"))
                    {Rocks.optionsCreateIfMissing = True})
               v <- Rocks.get dbh Rocks.defaultReadOptions key
               Rocks.close dbh
               pure v)
        shouldBe result Nothing)
  it
    "Put/get on closed DB should fail"
    (do result <-
          fmap
            (second (const ()) .
             first (const () :: Rocks.RocksDBException -> ()))
            (liftIO
               (try
                  (withTempDirCleanedUp
                     (\dir -> do
                        dbh <-
                          Rocks.open
                            ((Rocks.defaultOptions (dir </> "demo.db"))
                             {Rocks.optionsCreateIfMissing = False})
                        Rocks.close dbh
                        Rocks.put dbh Rocks.defaultWriteOptions "foo" "foo"
                        Rocks.get dbh Rocks.defaultReadOptions "foo"))))
        shouldBe result (Left () :: Either () ()))

open :: Spec
open = do
  it
    "Open missing (should fail)"
    (do result <-
          fmap
            (second (const ()) .
             first (const () :: Rocks.RocksDBException -> ()))
            (liftIO
               (try
                  (withTempDirCleanedUp
                     (\dir -> do
                        dbh <-
                          Rocks.open
                            ((Rocks.defaultOptions (dir </> "demo.db"))
                             {Rocks.optionsCreateIfMissing = False})
                        Rocks.close dbh))))
        shouldBe result (Left () :: Either () ()))
  it
    "Double open (should fail)"
    (do result <-
          fmap
            (second (const ()) .
             first (const () :: Rocks.RocksDBException -> ()))
            (liftIO
               (try
                  (withTempDirCleanedUp
                     (\dir -> do
                        dbh <-
                          Rocks.open
                            ((Rocks.defaultOptions (dir </> "demo.db"))
                             {Rocks.optionsCreateIfMissing = False})
                        dbh' <-
                          Rocks.open
                            ((Rocks.defaultOptions (dir </> "demo.db"))
                             {Rocks.optionsCreateIfMissing = False})
                        Rocks.close dbh'
                        Rocks.close dbh))))
        shouldBe result (Left () :: Either () ()))
  it
    "Open then close"
    (do result <-
          fmap
            (second (const ()) .
             first (show :: Rocks.RocksDBException -> String))
            (liftIO
               (try
                  (withTempDirCleanedUp
                     (\dir -> do
                        dbh <-
                          Rocks.open
                            ((Rocks.defaultOptions (dir </> "demo.db"))
                             {Rocks.optionsCreateIfMissing = True})
                        Rocks.close dbh))))
        shouldBe result (Right () :: Either String ()))
  it
    "Open (unicode filename)"
    (do result <-
          fmap
            (second (const ()) .
             first (show :: Rocks.RocksDBException -> String))
            (liftIO
               (try
                  (withTempDirCleanedUp
                     (\dir -> do
                        unicode <-
                          getUnicodeString <$> liftIO (generate arbitrary)
                        dbh <-
                          Rocks.open
                            ((Rocks.defaultOptions (dir </> unicode))
                             {Rocks.optionsCreateIfMissing = True})
                        Rocks.close dbh))))
        shouldBe result (Right () :: Either String ()))
  it
    "Open then close then open again"
    (do result <-
          fmap
            (second (const ()) .
             first (show :: Rocks.RocksDBException -> String))
            (liftIO
               (try
                  (withTempDirCleanedUp
                     (\dir -> do
                        dbh <-
                          Rocks.open
                            ((Rocks.defaultOptions (dir </> "demo.db"))
                             {Rocks.optionsCreateIfMissing = True})
                        Rocks.close dbh
                        dbh' <-
                          Rocks.open
                            ((Rocks.defaultOptions (dir </> "demo.db"))
                             {Rocks.optionsCreateIfMissing = True})
                        Rocks.close dbh'))))
        shouldBe result (Right () :: Either String ()))
  it
    "Double close succeeds"
    (do result <-
          fmap
            (second (const ()) .
             first (show :: Rocks.RocksDBException -> String))
            (liftIO
               (try
                  (withTempDirCleanedUp
                     (\dir -> do
                        dbh <-
                          Rocks.open
                            ((Rocks.defaultOptions (dir </> "demo.db"))
                             {Rocks.optionsCreateIfMissing = True})
                        Rocks.close dbh
                        Rocks.close dbh))))
        shouldBe result (Right () :: Either String ()))

obscure :: Spec
#if !defined(mingw32_HOST_OS)
obscure =
  it
    "Weird global singleton string matching stuff for double-locking warnings"
    -- This test will fail when RocksDB fixes this issue
    -- (see https://stackoverflow.com/questions/37310588/rocksdb-io-error-lock-no-locks-available#comment83145041_37312033).
    -- It exists so that we get notified when that fixing happens.
    (do result <-
          fmap
            (second (const ()) .
             first (const () :: Rocks.RocksDBException -> ()))
            (liftIO
               (try
                  (withTempDirCleanedUp
                     (\dir -> do
                        dbh <-
                          Rocks.open
                            ((Rocks.defaultOptions (dir </> "demo.db"))
                             {Rocks.optionsCreateIfMissing = False})
                        removeDirectoryRecursive dir
                        dbh' <-
                          Rocks.open
                            ((Rocks.defaultOptions (dir </> "demo.db"))
                             {Rocks.optionsCreateIfMissing = False})
                        Rocks.close dbh'
                        Rocks.close dbh))))
        shouldBe result (Left () :: Either () ()))
#else
obscure = pure ()
#endif

----------------------------------------------------------------------
-- Helpers

-- | Run the action with a temporary directory, force the result, remove the directory.
withTempDirCleanedUp :: NFData a => (FilePath -> IO a) -> IO a
withTempDirCleanedUp f =
  withSystemTempDirectory
    "rocks-test"
    (\dir -> do
       !v <- fmap force (f dir)
       removeDirectoryRecursive dir
       pure v)
