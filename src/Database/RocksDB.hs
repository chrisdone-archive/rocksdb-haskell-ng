{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ForeignFunctionInterface #-}

-- | A safe binding to RocksDB.

module Database.RocksDB
    -- * Functions
  ( open
  , OpenConfig(..)
  , close
  , put
  , get
    -- * Types
  , DBH
  , RocksDBException(..)
  ) where

import           Control.Concurrent
import           Control.Exception
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Unsafe as S
import           Data.Coerce
import           Data.Typeable
import           Foreign
import           Foreign.C

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
  { dbhVar :: !(MVar (Maybe (ForeignPtr DB)))
  }

-- | An exception thrown by this module.
data RocksDBException
  = UnsuccessfulOperation !String !String
  | AllocationReturnedNull !String
  | DatabaseIsClosed !String
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
         (\optsPtr -> do
            c_rocksdb_options_set_create_if_missing
              optsPtr
              (openConfigCreateIfMissing config)
            dbhPtr <-
              assertNotError "c_rocksdb_open" (c_rocksdb_open optsPtr pathPtr)
            dbhFptr <- newForeignPtr c_rocksdb_close_funptr dbhPtr
            dbRef <- newMVar (Just dbhFptr)
            pure (DBH {dbhVar = dbRef})))

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
    (dbhVar dbh)
    (\mfptr -> do
       maybe
         (return ())
         finalizeForeignPtr
         mfptr
       -- Previous line: The fptr would be finalized _eventually_, so
       -- no memory leaks. But calling @close@ indicates you want to
       -- release the resources right now.
       --
       -- Also, a foreign pointer's finalizer is ran and then deleted,
       -- so you can't double-free.
       pure Nothing)

-- | Put a @value@ at @key@.
put :: DBH -> ByteString -> ByteString -> IO ()
put dbh key value =
  withDBPtr
    dbh
    "put"
    (\dbPtr ->
       S.unsafeUseAsCStringLen
         key
         (\(key_ptr, klen) ->
            S.unsafeUseAsCStringLen
              value
              (\(val_ptr, vlen) ->
                 withWriteOptions
                   (\optsPtr ->
                      assertNotError
                        "put"
                        (c_rocksdb_put
                           dbPtr
                           optsPtr
                           key_ptr
                           (fromIntegral klen)
                           val_ptr
                           (fromIntegral vlen))))))

-- | Get a value at @key@.
get :: DBH -> ByteString -> IO (Maybe ByteString)
get dbh key =
  withDBPtr
    dbh
    "get"
    (\dbPtr ->
       S.unsafeUseAsCStringLen
         key
         (\(key_ptr, klen) ->
            alloca
              (\vlen_ptr ->
                 withReadOptions
                   (\optsPtr -> do
                      val_ptr <-
                        assertNotError
                          "get"
                          (c_rocksdb_get
                             dbPtr
                             optsPtr
                             key_ptr
                             (fromIntegral klen)
                             vlen_ptr)
                      vlen <- peek vlen_ptr
                      if val_ptr == nullPtr
                        then return Nothing
                    -- Below: we as callers of the rocksDB C library
                    -- own the malloc'd string and we are supposed to
                    -- free it ourselves. S.unsafeUseAsCStringLen
                    -- re-uses with no copying the C array and adds a
                    -- free() finalizer.
                        else fmap
                               Just
                               (S.unsafePackMallocCStringLen
                                  (val_ptr, fromIntegral vlen))))))

--------------------------------------------------------------------------------
-- Internal functions

-- | Do something with the pointer inside. This is thread-safe.
withDBPtr :: DBH -> String -> (Ptr DB -> IO a) -> IO a
withDBPtr dbh label f =
  withMVar
    (dbhVar dbh)
    (\mfptr ->
       case mfptr of
         Nothing -> throwIO (DatabaseIsClosed label)
         Just db -> withForeignPtr db f)

withOptions :: (Ptr Options -> IO a) -> IO a
withOptions =
  bracket
    (assertNotNull "c_rocksdb_options_create" c_rocksdb_options_create)
    c_rocksdb_options_destroy

withWriteOptions :: (Ptr WriteOptions -> IO a) -> IO a
withWriteOptions =
  bracket
    (assertNotNull "c_rocksdb_writeoptions_create" c_rocksdb_writeoptions_create)
    c_rocksdb_writeoptions_destroy

withReadOptions :: (Ptr ReadOptions -> IO a) -> IO a
withReadOptions =
  bracket
    (assertNotNull "c_rocksdb_readoptions_create" c_rocksdb_readoptions_create)
    c_rocksdb_readoptions_destroy

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
assertNotError :: String -> (Ptr CString -> IO a) -> IO a
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
data WriteOptions
data ReadOptions
data DB

foreign import ccall safe "rocksdb/c.h rocksdb_options_create"
  c_rocksdb_options_create :: IO (Ptr Options)

foreign import ccall safe "rocksdb/c.h rocksdb_options_destroy"
  c_rocksdb_options_destroy :: Ptr Options -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_options_set_create_if_missing"
  c_rocksdb_options_set_create_if_missing :: Ptr Options -> Bool -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_open"
  c_rocksdb_open :: Ptr Options -> CString -> Ptr CString -> IO (Ptr DB)

foreign import ccall safe "rocksdb/c.h &rocksdb_close"
  c_rocksdb_close_funptr :: FunPtr (Ptr DB -> IO ())

foreign import ccall safe "rocksdb/c.h rocksdb_put"
  c_rocksdb_put :: Ptr DB
                -> Ptr WriteOptions
                -> CString -> CSize
                -> CString -> CSize
                -> Ptr CString
                -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_get"
  c_rocksdb_get :: Ptr DB
                -> Ptr ReadOptions
                -> CString -> CSize
                -> Ptr CSize -- ^ Output length.
                -> Ptr CString
                -> IO CString

foreign import ccall safe "rocksdb/c.h rocksdb_writeoptions_create"
  c_rocksdb_writeoptions_create :: IO (Ptr WriteOptions)

foreign import ccall safe "rocksdb/c.h rocksdb_writeoptions_destroy"
  c_rocksdb_writeoptions_destroy :: Ptr WriteOptions -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_readoptions_create"
  c_rocksdb_readoptions_create :: IO (Ptr ReadOptions)

foreign import ccall safe "rocksdb/c.h rocksdb_readoptions_destroy"
  c_rocksdb_readoptions_destroy :: Ptr ReadOptions -> IO ()
