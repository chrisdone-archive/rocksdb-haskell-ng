{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ForeignFunctionInterface #-}

-- | A safe binding to RocksDB.

module Database.RocksDB
    -- * Functions
  ( open
  , OpenConfig(..)
  , close
    -- * Types
  , DBH
  , RocksDBException(..)
  ) where

import Control.Concurrent
import Control.Exception
import Data.Coerce
import Data.Typeable
import Foreign
import Foreign.C

--------------------------------------------------------------------------------
-- Publicly-exposed types


-- | Option options.
data OpenConfig = OpenConfig
  { openConfigCreateIfMissing :: !Bool
  , openConfigFilePath :: !FilePath
  }

-- | A handle to a RocksDB database. When handle becomes out of reach,
-- the database is closed.
data DBH = DBH
  { dbhRef :: !(MVar (Maybe (ForeignPtr DB)))
  }

-- | An exception thrown by this module.
data RocksDBException
  = UnsuccessfulOperation !String !String
  | AllocationReturnedNull !String
  deriving (Typeable, Show)
instance Exception RocksDBException

--------------------------------------------------------------------------------
-- Publicly-exposed functions

-- | Open a database at the given path.
--
-- Throws 'RocksDBException' on failure.
--
-- The database will be closed when the 'DBH' is garbage collected or
-- when 'close' is called on it.
open :: OpenConfig -> IO DBH
open config = do
  withCString
    (openConfigFilePath config)
    (\pathPtr ->
       withOptions
         (\optionsPtr -> do
            c_rocksdb_options_set_create_if_missing
              optionsPtr
              (openConfigCreateIfMissing config)
            dbhPtr <-
              assertNotError
                "c_rocksdb_open"
                (c_rocksdb_open optionsPtr pathPtr)
            dbhFptr <- newForeignPtr c_rocksdb_close_funptr dbhPtr
            ptrRef <- newMVar (Just dbhFptr)
            pure (DBH {dbhRef = ptrRef})))

-- | Close a database.
--
-- * Calling this function twice has no ill-effects.
--
-- * You don't have to call this; if you no longer hold a reference to
--   the @DBH@ then it will be closed upon garbage collection. But you
--   can call this function to do it early.
--
-- * Further operations (get/put) will throw an exception on a closed
-- * @DBH@.
close :: DBH -> IO ()
close dbh =
  modifyMVar_
    (dbhRef dbh)
    (\mfptr -> do
       maybe (return ()) finalizeForeignPtr mfptr
       -- Previous line: The fptr would be finalized _eventually_, so
       -- no memory leaks. But calling @close@ indicates you want to
       -- release the resources right now.
       --
       -- Also, a foreign pointer's finalizer is ran and then deleted,
       -- so you can't double-free.
       pure Nothing)

--------------------------------------------------------------------------------
-- Internal functions

withOptions :: (Ptr Options -> IO a) -> IO a
withOptions =
  bracket
    (assertNotNull "c_rocksdb_options_create" c_rocksdb_options_create)
    c_rocksdb_options_destroy

--------------------------------------------------------------------------------
-- Correctness checks for foreign functions

-- | Check that the RETCODE is successful.
assertNotNull :: (Coercible a (Ptr ())) => String -> IO a -> IO a
assertNotNull label m = do
  val <- m
  if coerce val == nullPtr
    then throwIO (AllocationReturnedNull label)
    else pure val

-- | Check that the RETCODE is successful.
assertNotError :: (Coercible a (Ptr ())) => String -> (Ptr CString -> IO a) -> IO a
assertNotError label f =
  alloca
    (\errorPtr -> do
       poke errorPtr nullPtr
       result <- f errorPtr
       value <- peek errorPtr
       if value == nullPtr
         then return result
         else do
           err <- peekCString value
           throwIO (UnsuccessfulOperation label err))

--------------------------------------------------------------------------------
-- Foreign (unsafe) bindings

data Options
data DB

foreign import ccall safe "rocksdb/c.h rocksdb_options_create"
  c_rocksdb_options_create :: IO (Ptr Options)

foreign import ccall safe "rocksdb/c.h rocksdb_options_destroy"
  c_rocksdb_options_destroy :: Ptr Options -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_options_set_create_if_missing"
  c_rocksdb_options_set_create_if_missing :: Ptr Options -> Bool -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_open"
  c_rocksdb_open :: Ptr Options -> CString -> Ptr CString -> IO (Ptr DB)

foreign import ccall safe "rocksdb\\c.h &rocksdb_close"
  c_rocksdb_close_funptr :: FunPtr (Ptr DB -> IO ())
