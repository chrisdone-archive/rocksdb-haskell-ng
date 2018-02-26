{-# LANGUAGE CPP #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Comprehensive aggressive test-suite.

module Main where

import           Control.DeepSeq
import           Control.Exception
import           Control.Monad.IO.Class
import           Data.Bifunctor
import           Data.Functor
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
  describe "Batch write" batch
  describe "Delete" delete
  describe "Compression" compression
  describe "Iterators" iterators
  describe "Snapshots" snapshots
  describe "Obscure conditions" obscure

batch :: Spec
batch = do
  it
    "Batch put"
    (do let key = "some key"
            val = "Hello, World!"
            key2 = "some other key"
            val2 = "Hello!"
        result <-
          withTempDirCleanedUp
            (\dir -> do
               dbh <-
                 Rocks.open
                   ((Rocks.defaultOptions (dir </> "demo.db"))
                    {Rocks.optionsCreateIfMissing = True})
               Rocks.write
                 dbh
                 Rocks.defaultWriteOptions
                 [Rocks.Put key val, Rocks.Put key2 val2]
               v <- Rocks.get dbh Rocks.defaultReadOptions key
               v2 <- Rocks.get dbh Rocks.defaultReadOptions key2
               Rocks.close dbh
               pure (v, v2))
        shouldBe result ((Just val, Just val2)))
  it
    "Batch put/del"
    (do let key = "some key"
            val = "Hello, World!"
            key2 = "some other key"
            val2 = "Hello!"
        result <-
          withTempDirCleanedUp
            (\dir -> do
               dbh <-
                 Rocks.open
                   ((Rocks.defaultOptions (dir </> "demo.db"))
                    {Rocks.optionsCreateIfMissing = True})
               Rocks.write
                 dbh
                 Rocks.defaultWriteOptions
                 [Rocks.Put key val, Rocks.Put key2 val2, Rocks.Del key]
               v <- Rocks.get dbh Rocks.defaultReadOptions key
               v2 <- Rocks.get dbh Rocks.defaultReadOptions key2
               Rocks.close dbh
               pure (v, v2))
        shouldBe result ((Nothing, Just val2)))

delete :: Spec
delete =
  it
    "Get/put/delete succeeds"
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
               Rocks.delete dbh Rocks.defaultWriteOptions key
               v <- Rocks.get dbh Rocks.defaultReadOptions key
               Rocks.close dbh
               pure v)
        shouldBe result Nothing)

iterators :: Spec
iterators = do
  it
    "Invalid iterator release"
    (shouldThrow
       (withTempDirCleanedUp
          (\dir -> do
             dbh <-
               Rocks.open
                 ((Rocks.defaultOptions (dir </> "demo.db"))
                  {Rocks.optionsCreateIfMissing = True})
             iterator <- Rocks.createIter dbh Rocks.defaultReadOptions
             Rocks.close dbh
             Rocks.releaseIter iterator))
       (== Rocks.DatabaseIsClosed "releaseIter"))
  it
    "Used iterator after release"
    (shouldThrow
       (withTempDirCleanedUp
          (\dir -> do
             dbh <-
               Rocks.open
                 ((Rocks.defaultOptions (dir </> "demo.db"))
                  {Rocks.optionsCreateIfMissing = True})
             finally
               (do iterator <- Rocks.createIter dbh Rocks.defaultReadOptions
                   Rocks.releaseIter iterator
                   _ <- Rocks.iterEntry iterator
                   pure ())
               (Rocks.close dbh)))
       (== Rocks.IteratorIsClosed "iterEntry"))
  it
    "Used iterator after release (alternative)"
    (shouldThrow
       (withTempDirCleanedUp
          (\dir -> do
             dbh <-
               Rocks.open
                 ((Rocks.defaultOptions (dir </> "demo.db"))
                  {Rocks.optionsCreateIfMissing = True})
             finally
               (do iterator <- Rocks.createIter dbh Rocks.defaultReadOptions
                   Rocks.releaseIter iterator
                   Rocks.iterSeek iterator "k")
               (Rocks.close dbh)))
       (== Rocks.IteratorIsClosed "iterSeek"))
  it
    "Valid iterator release"
    (do r <-
          withTempDirCleanedUp
            (\dir -> do
               dbh <-
                 Rocks.open
                   ((Rocks.defaultOptions (dir </> "demo.db"))
                    {Rocks.optionsCreateIfMissing = True})
               iterator <- Rocks.createIter dbh Rocks.defaultReadOptions
               Rocks.releaseIter iterator
               Rocks.close dbh)
        shouldBe r ())
  it
    "No iterator release"
    (do r <-
          withTempDirCleanedUp
            (\dir -> do
               dbh <-
                 Rocks.open
                   ((Rocks.defaultOptions (dir </> "demo.db"))
                    {Rocks.optionsCreateIfMissing = True})
               _ <- Rocks.createIter dbh Rocks.defaultReadOptions
               Rocks.close dbh)
        shouldBe r ())
  it
    "Iterator next/next/next no problem"
    (do let vals = [("k1", "foo"), ("k2", "bar")]
        result <-
          withTempDirCleanedUp
            (\dir -> do
               dbh <-
                 Rocks.open
                   ((Rocks.defaultOptions (dir </> "demo.db"))
                    {Rocks.optionsCreateIfMissing = True})
               Rocks.write
                 dbh
                 Rocks.defaultWriteOptions
                 (map (uncurry Rocks.Put) vals)
               iterator <- Rocks.createIter dbh Rocks.defaultReadOptions
               Rocks.iterNext iterator
               Rocks.iterNext iterator
               Rocks.iterNext iterator
               Rocks.releaseIter iterator
               Rocks.close dbh
               pure ())
        shouldBe result ())
  it
    "Iterator without seek"
    (do let vals = [("k1", "foo"), ("k2", "bar")]
        result <-
          withTempDirCleanedUp
            (\dir -> do
               dbh <-
                 Rocks.open
                   ((Rocks.defaultOptions (dir </> "demo.db"))
                    {Rocks.optionsCreateIfMissing = True})
               Rocks.write
                 dbh
                 Rocks.defaultWriteOptions
                 (map (uncurry Rocks.Put) vals)
               iterator <- Rocks.createIter dbh Rocks.defaultReadOptions
               let loop = do
                     mv <- Rocks.iterEntry iterator
                     case mv of
                       Just v -> do
                         Rocks.iterNext iterator
                         fmap (v :) loop
                       Nothing -> pure []
               vs <- loop
               Rocks.releaseIter iterator
               Rocks.close dbh
               pure vs)
        shouldBe result [])
  it
    "Iterator after batch"
    (do let vals = [("k1", "foo"), ("k2", "bar")]
        result <-
          withTempDirCleanedUp
            (\dir -> do
               dbh <-
                 Rocks.open
                   ((Rocks.defaultOptions (dir </> "demo.db"))
                    {Rocks.optionsCreateIfMissing = True})
               Rocks.write
                 dbh
                 Rocks.defaultWriteOptions
                 (map (uncurry Rocks.Put) vals)
               iterator <- Rocks.createIter dbh Rocks.defaultReadOptions
               Rocks.iterSeek iterator "k"
               let loop = do
                     mv <- Rocks.iterEntry iterator
                     case mv of
                       Just v -> do
                         Rocks.iterNext iterator
                         fmap (v :) loop
                       Nothing -> pure []
               vs <- loop
               Rocks.releaseIter iterator
               Rocks.close dbh
               pure vs)
        shouldBe result vals)

snapshots :: Spec
snapshots = do
  it
    "Invalid snapshot release"
    (shouldThrow
       (withTempDirCleanedUp
          (\dir -> do
             dbh <-
               Rocks.open
                 ((Rocks.defaultOptions (dir </> "demo.db"))
                  {Rocks.optionsCreateIfMissing = True})
             snapshot <- Rocks.createSnapshot dbh
             Rocks.close dbh
             Rocks.releaseSnapshot snapshot))
       (== Rocks.DatabaseIsClosed "releaseSnapshot"))
  it
    "Used snapshot after release"
    (shouldThrow
       (withTempDirCleanedUp
          (\dir -> do
             dbh <-
               Rocks.open
                 ((Rocks.defaultOptions (dir </> "demo.db"))
                  {Rocks.optionsCreateIfMissing = True})
             finally
               (do snapshot <- Rocks.createSnapshot dbh
                   Rocks.releaseSnapshot snapshot
                   void
                     (Rocks.get
                        dbh
                        Rocks.defaultReadOptions
                        {Rocks.readOptionsSnapshot = Just snapshot}
                        "key"))
               (Rocks.close dbh)))
       (== Rocks.SnapshotIsClosed "withReadOptions"))
  it
    "Valid snapshot release"
    (do r <-
          withTempDirCleanedUp
            (\dir -> do
               dbh <-
                 Rocks.open
                   ((Rocks.defaultOptions (dir </> "demo.db"))
                    {Rocks.optionsCreateIfMissing = True})
               snapshot <- Rocks.createSnapshot dbh
               Rocks.releaseSnapshot snapshot
               Rocks.close dbh)
        shouldBe r ())
  it
    "No snapshot release"
    (do r <-
          withTempDirCleanedUp
            (\dir -> do
               dbh <-
                 Rocks.open
                   ((Rocks.defaultOptions (dir </> "demo.db"))
                    {Rocks.optionsCreateIfMissing = True})
               _ <- Rocks.createSnapshot dbh
               Rocks.close dbh)
        shouldBe r ())
  it
    "Read operations only see data from the snapshot"
    (do r <-
          withTempDirCleanedUp
            (\dir -> do
               dbh <-
                 Rocks.open
                   ((Rocks.defaultOptions (dir </> "demo.db"))
                    {Rocks.optionsCreateIfMissing = True})
               let key = "key"
               Rocks.put dbh Rocks.defaultWriteOptions key "Hello"
               snapshot <- Rocks.createSnapshot dbh
               Rocks.put dbh Rocks.defaultWriteOptions key "World!"
               snapshotVal <-
                 Rocks.get
                   dbh
                   Rocks.defaultReadOptions
                   {Rocks.readOptionsSnapshot = Just snapshot}
                   key
               regularVal <- Rocks.get dbh Rocks.defaultReadOptions key
               Rocks.delete dbh Rocks.defaultWriteOptions key
               snapshotVal2 <-
                 Rocks.get
                   dbh
                   Rocks.defaultReadOptions
                   {Rocks.readOptionsSnapshot = Just snapshot}
                   key
               regularVal2 <- Rocks.get dbh Rocks.defaultReadOptions key
               Rocks.releaseSnapshot snapshot
               Rocks.close dbh
               pure (snapshotVal, regularVal, snapshotVal2, regularVal2))
        shouldBe r (Just "Hello",Just "World!",Just "Hello", Nothing))

compression :: Spec
compression =
  sequence_
    [ itPendingWindows
      ("Get/put with " ++ show c)
      (do let key = "some key"
              val = "Hello, World!"
          result <-
            withTempDirCleanedUp
              (\dir -> do
                 dbh <-
                   Rocks.open
                     ((Rocks.defaultOptions (dir </> "demo.db"))
                      { Rocks.optionsCreateIfMissing = True
                      , Rocks.optionsCompression = c
                      })
                 Rocks.put dbh Rocks.defaultWriteOptions key val
                 v <- Rocks.get dbh Rocks.defaultReadOptions key
                 Rocks.close dbh
                 pure v)
          shouldBe result (Just val))
    | c <- [minBound .. maxBound]
    ]

#if defined(mingw32_HOST_OS)
itPendingWindows :: Example a => String -> a -> SpecWith ()
itPendingWindows l _ = it l (pendingWith "No Windows support yet.")
#else
itPendingWindows :: Example a => String -> a -> SpecWith (Arg a)
itPendingWindows l m = it l m
#endif

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
