{-# LANGUAGE BangPatterns #-}
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

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "Open/create/close" open

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
                            (Rocks.OpenConfig
                             { Rocks.openConfigFilePath = dir </> "demo.db"
                             , Rocks.openConfigCreateIfMissing = False
                             })
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
                            (Rocks.OpenConfig
                             { Rocks.openConfigFilePath = dir </> "demo.db"
                             , Rocks.openConfigCreateIfMissing = False
                             })
                        dbh' <-
                          Rocks.open
                            (Rocks.OpenConfig
                             { Rocks.openConfigFilePath = dir </> "demo.db"
                             , Rocks.openConfigCreateIfMissing = False
                             })
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
                            (Rocks.OpenConfig
                             { Rocks.openConfigFilePath = dir </> "demo.db"
                             , Rocks.openConfigCreateIfMissing = True
                             })
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
                            (Rocks.OpenConfig
                             { Rocks.openConfigFilePath = dir </> "demo.db"
                             , Rocks.openConfigCreateIfMissing = True
                             })
                        Rocks.close dbh
                        dbh' <-
                          Rocks.open
                            (Rocks.OpenConfig
                             { Rocks.openConfigFilePath = dir </> "demo.db"
                             , Rocks.openConfigCreateIfMissing = True
                             })
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
                            (Rocks.OpenConfig
                             { Rocks.openConfigFilePath = dir </> "demo.db"
                             , Rocks.openConfigCreateIfMissing = True
                             })
                        Rocks.close dbh
                        Rocks.close dbh))))
        shouldBe result (Right () :: Either String ()))

-- | Run the action with a temporary directory, force the result, remove the directory.
withTempDirCleanedUp :: NFData a => (FilePath -> IO a) -> IO a
withTempDirCleanedUp f =
  withSystemTempDirectory
    "rocks-test"
    (\dir -> do
       !v <- fmap force (f dir)
       removeDirectoryRecursive dir
       pure v)
