{-# LANGUAGE CPP #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ForeignFunctionInterface #-}

-- | A safe binding to RocksDB.

module Database.RocksDB
    -- * Basic Functions
  ( open
  , defaultOptions
  , Options(..)
  , DB
  , Compression(..)
  , close
  , put
  , defaultWriteOptions
  , WriteOptions(..)
  , get
  , defaultReadOptions
  , ReadOptions(..)
  , write
  , BatchOp(..)
  , delete
  -- * Iterators
  , createIter
  , releaseIter
  , iterSeek
  , iterEntry
  , iterNext
  , Iterator
  -- * Misc types
  , RocksDBException(..)
  ) where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import           Control.Monad.IO.Class
import           Data.ByteString (ByteString)
import           Data.ByteString.Internal
import qualified Data.ByteString.Unsafe as S
import           Data.Coerce
import           Data.Typeable
import           Foreign
import           Foreign.C
import qualified GHC.Foreign as GHC
import qualified GHC.IO.Encoding as GHC
import           System.Directory

--------------------------------------------------------------------------------
-- Publicly-exposed types

-- | Option options.
data Options = Options
  { optionsCreateIfMissing :: !Bool
  , optionsFilePath :: !FilePath
  , optionsCompression :: !Compression
  }

data Compression
  = NoCompression
  | SnappyCompression
  | ZlibCompression
  deriving (Enum, Bounded, Show)

data WriteOptions = WriteOptions {}

data ReadOptions = ReadOptions {}

-- | Batch operation
data BatchOp
  = Put !ByteString !ByteString
  | Del !ByteString

-- | A handle to a RocksDB database. When handle becomes out of reach,
-- the database is closed.
data DB = DB
  { dbVar :: !(MVar (Maybe (ForeignPtr CDB)))
  }

-- | A handle to a RocksDB iterator. When handle becomes out of reach,
-- the iterator is destroyed.
data Iterator = Iterator
  { iteratorDB :: !DB
  , iteratorVar :: !(MVar (Maybe (ForeignPtr CIterator)))
  }

-- | An exception thrown by this module.
data RocksDBException
  = UnsuccessfulOperation !String !String
  | AllocationReturnedNull !String
  | DatabaseIsClosed !String
  | IteratorIsClosed !String
  | IteratorIsInvalid !String
  deriving (Typeable, Show)
instance Exception RocksDBException

--------------------------------------------------------------------------------
-- Defaults

defaultOptions :: FilePath -> Options
defaultOptions fp =
  Options
  { optionsCreateIfMissing = False
  , optionsFilePath = fp
  , optionsCompression = SnappyCompression
  }

defaultWriteOptions :: WriteOptions
defaultWriteOptions = WriteOptions {}

defaultReadOptions :: ReadOptions
defaultReadOptions = ReadOptions {}

--------------------------------------------------------------------------------
-- Publicly-exposed functions

-- | Open a database at the given path.
--
-- Throws 'RocksDBException' on failure.
--
-- The database will be closed when the 'DB' is garbage collected or
-- when 'close' is called on it.
--
-- With LC_ALL=C, two things happen:
--   * rocksdb can't open a database with unicode in path;
--   * rocksdb can't create a folder properly.
--
-- So, we create the folder by ourselves, and for that we
-- need to set the encoding we're going to use. On Linux
-- it's almost always UTC-8.
open :: MonadIO m => Options -> m DB
open config =
  liftIO
    (do withFilePath
          (optionsFilePath config)
          (\pathPtr ->
             withOptions
               (\optsPtr -> do
                  c_rocksdb_options_set_create_if_missing
                    optsPtr
                    (optionsCreateIfMissing config)
                  c_rocksdb_options_set_compression
                    optsPtr
                    (fromIntegral (fromEnum (optionsCompression config)))
                  dbhPtr <-
                    bracket
                      (replaceEncoding config)
                      restoreEncoding
                      (const
                         (do when
                               (optionsCreateIfMissing config)
                               (createDirectoryIfMissing
                                  True
                                  (optionsFilePath config))
                             v <-
                               assertNotError
                                 "c_rocksdb_open"
                                 (c_rocksdb_open optsPtr pathPtr)
                             pure v))
                  dbhFptr <- newForeignPtr c_rocksdb_close_funptr dbhPtr
                  dbRef <- newMVar (Just dbhFptr)
                  pure (DB {dbVar = dbRef}))))

-- | Close a database.
--
-- * Calling this function twice has no ill-effects.
--
-- * You don't have to call this; if you no longer hold a reference to
--   the @DB@ then it will be closed upon garbage collection. But you
--   can call this function to do it early.
--
-- * Further operations (get/put) will throw an exception on a closed
-- * @DB@.
close :: MonadIO m => DB -> m ()
close dbh =
  liftIO
    (modifyMVar_
       (dbVar dbh)
       (\mfptr -> do
          maybe (evaluate ()) finalizeForeignPtr mfptr
          -- Previous line: The fptr would be finalized _eventually_, so
          -- no memory leaks. But calling @close@ indicates you want to
          -- release the resources right now.
          --
          -- Also, a foreign pointer's finalizer is ran and then deleted,
          -- so you can't double-free.
          pure Nothing))

-- | Delete value at @key@.
delete :: MonadIO m => DB -> WriteOptions -> ByteString -> m ()
delete dbh writeOpts key =
  liftIO
    (withDBPtr
       dbh
       "delete"
       (\dbPtr ->
          withWriteOptions
            writeOpts
            (\opts_ptr ->
               S.unsafeUseAsCStringLen
                 key
                 (\(key_ptr, klen) ->
                    assertNotError
                      "c_rocksdb_delete"
                      (c_rocksdb_delete
                         dbPtr
                         opts_ptr
                         key_ptr
                         (fromIntegral klen))))))

-- | Put a @value@ at @key@.
put :: MonadIO m => DB -> WriteOptions -> ByteString -> ByteString -> m ()
put dbh writeOpts key value =
  liftIO
    (withDBPtr
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
                      writeOpts
                      (\optsPtr ->
                         assertNotError
                           "put"
                           (c_rocksdb_put
                              dbPtr
                              optsPtr
                              key_ptr
                              (fromIntegral klen)
                              val_ptr
                              (fromIntegral vlen)))))))

-- | Get a value at @key@.
get :: MonadIO m => DB -> ReadOptions -> ByteString -> m (Maybe ByteString)
get dbh readOpts key =
  liftIO
    (withDBPtr
       dbh
       "get"
       (\dbPtr ->
          S.unsafeUseAsCStringLen
            key
            (\(key_ptr, klen) ->
               alloca
                 (\vlen_ptr ->
                    withReadOptions
                      readOpts
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
                         adoptByteStringMaybe val_ptr vlen)))))

-- | Write a batch of operations atomically.
write :: MonadIO m => DB -> WriteOptions -> [BatchOp] -> m ()
write dbh opts batch =
  liftIO
    (withDBPtr
       dbh
       "write"
       (\dbPtr ->
          withWriteOptions
            opts
            (\writeOpts ->
               withWriteBatch
                 (\writerPtr -> do
                    mapM_ (batchAdd writerPtr) batch
                    assertNotError
                      "c_rocksdb_write"
                      (c_rocksdb_write dbPtr writeOpts writerPtr)
                    -- Ensure @ByteString@s (and respective shared @CStringLen@s) aren't GC'ed
                    -- until here.
                    mapM_ touch batch))))
  where
    batchAdd batch_ptr (Put key val) =
      S.unsafeUseAsCStringLen key $ \(key_ptr, klen) ->
        S.unsafeUseAsCStringLen val $ \(val_ptr, vlen) ->
          c_rocksdb_writebatch_put
            batch_ptr
            key_ptr
            (fromIntegral klen)
            val_ptr
            (fromIntegral vlen)
    batchAdd batch_ptr (Del key) =
      S.unsafeUseAsCStringLen key $ \(key_ptr, klen) ->
        c_rocksdb_writebatch_delete batch_ptr key_ptr (fromIntegral klen)
    touch (Put (PS p _ _) (PS p' _ _)) = do
      touchForeignPtr p
      touchForeignPtr p'
    touch (Del (PS p _ _)) = touchForeignPtr p

-- | Create an iterator on the DB. If the DB is closed the iterator's functions
--  will throw an exception.
createIter :: MonadIO m => DB -> ReadOptions -> m Iterator
createIter db opts =
  liftIO
    (withDBPtr
       db
       "createIter"
       (\dbPtr ->
          withReadOptions
            opts
            (\readOpts -> do
               iterPtr <-
                 assertNotNull
                   "c_rocksdb_create_iterator"
                   (c_rocksdb_create_iterator dbPtr readOpts)
               iterhFptr <- newForeignPtr c_rocksdb_iter_destroy_ptr iterPtr
               var <- newMVar (Just iterhFptr)
               pure (Iterator {iteratorDB = db, iteratorVar = var}))))

-- | Destroy an iterator.
--
-- * Calling this function twice has no ill-effects.
--
-- * You don't have to call this; if you no longer hold a reference to
--   the @Iterator@ then it will be closed upon garbage collection. But you
--   can call this function to do it early.
--
-- * Further operations will throw an exception on a closed @Iterator@.
releaseIter :: MonadIO m => Iterator -> m ()
releaseIter iterator =
  liftIO
    (modifyMVar_
       (iteratorVar iterator)
       (\mfptr -> do
          maybe (return ()) finalizeForeignPtr mfptr
          -- Previous line: The fptr would be finalized _eventually_, so
          -- no memory leaks. But calling @releaseIter@ indicates you want to
          -- release the resources right now.
          --
          -- Also, a foreign pointer's finalizer is ran and then deleted,
          -- so you can't double-free.
          pure Nothing))

iterSeek :: MonadIO m => Iterator -> ByteString -> m ()
iterSeek iter key =
  withIterPtr
    iter
    "iterSeek"
    (\iterPtr ->
       S.unsafeUseAsCStringLen
         key
         (\(key_ptr, klen) ->
            c_rocksdb_iter_seek iterPtr key_ptr (fromIntegral klen)))

iterEntry :: MonadIO m => Iterator -> m (Maybe (ByteString, ByteString))
iterEntry iter =
  withIterPtr
    iter
    "iterEntry"
    (\iterPtr -> do
       mkey <-
         alloca
           (\klenp -> do
              key <- c_rocksdb_iter_key iterPtr klenp
              klen <- peek klenp
              adoptByteStringMaybe key klen)
       mval <-
         alloca
           (\vlenp -> do
              val <- c_rocksdb_iter_value iterPtr vlenp
              vlen <- peek vlenp
              adoptByteStringMaybe val vlen)
       pure ((,) <$> mkey <*> mval))

iterNext :: MonadIO m => Iterator -> m ()
iterNext iter =
  withIterPtr iter "iterNext" (\iterPtr -> c_rocksdb_iter_next iterPtr)

--------------------------------------------------------------------------------
-- Internal functions

withIterPtr :: MonadIO m => Iterator -> String -> (Ptr CIterator -> IO a) -> m a
withIterPtr iter label f =
  liftIO
    (withDBPtr
       (iteratorDB iter)
       label
       (const
          (withMVar
             (iteratorVar iter)
             (\mfptr ->
                case mfptr of
                  Nothing -> throwIO (IteratorIsClosed label)
                  Just db ->
                    withForeignPtr
                      db
                      (\ptr -> do
                         valid <- c_rocksdb_iter_valid ptr
                         if valid
                           then f ptr
                           else throwIO (IteratorIsInvalid label))))))

-- | Do something with the pointer inside. This is thread-safe.
withDBPtr :: DB -> String -> (Ptr CDB -> IO a) -> IO a
withDBPtr dbh label f =
  withMVar
    (dbVar dbh)
    (\mfptr ->
       case mfptr of
         Nothing -> throwIO (DatabaseIsClosed label)
         Just db -> withForeignPtr db f)

withWriteBatch :: (Ptr CWriteBatch -> IO a) -> IO a
withWriteBatch =
  bracket
    (assertNotNull "c_rocksdb_writebatch_create" c_rocksdb_writebatch_create)
    c_rocksdb_writebatch_destroy

withOptions :: (Ptr COptions -> IO a) -> IO a
withOptions =
  bracket
    (assertNotNull "c_rocksdb_options_create" c_rocksdb_options_create)
    c_rocksdb_options_destroy

withWriteOptions :: WriteOptions -> (Ptr CWriteOptions -> IO a) -> IO a
withWriteOptions WriteOptions =
  bracket
    (assertNotNull "c_rocksdb_writeoptions_create" c_rocksdb_writeoptions_create)
    c_rocksdb_writeoptions_destroy

withReadOptions :: ReadOptions -> (Ptr CReadOptions -> IO a) -> IO a
withReadOptions ReadOptions =
  bracket
    (assertNotNull "c_rocksdb_readoptions_create" c_rocksdb_readoptions_create)
    c_rocksdb_readoptions_destroy

replaceEncoding :: Options -> IO GHC.TextEncoding
#ifdef mingw32_HOST_OS
replaceEncoding _ = GHC.getFileSystemEncoding
#else
replaceEncoding opts = do
  oldenc <- GHC.getFileSystemEncoding
  when (optionsCreateIfMissing opts) (GHC.setFileSystemEncoding GHC.utf8)
  pure oldenc
#endif

restoreEncoding :: GHC.TextEncoding -> IO ()
#ifdef mingw32_HOST_OS
restoreEncoding _ = pure ()
#else
restoreEncoding = GHC.setFileSystemEncoding
#endif

withFilePath :: FilePath -> (CString -> IO a) -> IO a
# ifdef mingw32_HOST_OS
withFilePath = withCString
# else
withFilePath = GHC.withCString GHC.utf8
# endif

-- | Adopt ownership of the string given to us by the C library.
--
-- Below: we as callers of the rocksDB C library
-- own the malloc'd string and we are supposed to
-- free it ourselves.
--
-- See facebook's example https://github.com/facebook/rocksdb/blob/master/examples/c_simple_example.c#L53
--
-- S.unsafeUseAsCStringLen re-uses with no copying
-- the C array and adds a free() finalizer.
--
-- If the string is NULL, just return Nothing.
adoptByteStringMaybe :: CString -> CSize -> IO (Maybe ByteString)
adoptByteStringMaybe val_ptr vlen =
  if val_ptr == nullPtr
    then return Nothing
    else fmap Just (S.unsafePackMallocCStringLen (val_ptr, fromIntegral vlen))

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

data COptions
data CWriteOptions
data CWriteBatch
data CReadOptions
data CDB
data CIterator

foreign import ccall safe "rocksdb/c.h rocksdb_options_create"
  c_rocksdb_options_create :: IO (Ptr COptions)

foreign import ccall safe "rocksdb/c.h rocksdb_options_destroy"
  c_rocksdb_options_destroy :: Ptr COptions -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_options_set_create_if_missing"
  c_rocksdb_options_set_create_if_missing :: Ptr COptions -> Bool -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_open"
  c_rocksdb_open :: Ptr COptions -> CString -> Ptr CString -> IO (Ptr CDB)

foreign import ccall safe "rocksdb/c.h &rocksdb_close"
  c_rocksdb_close_funptr :: FunPtr (Ptr CDB -> IO ())

foreign import ccall safe "rocksdb/c.h rocksdb_put"
  c_rocksdb_put :: Ptr CDB
                -> Ptr CWriteOptions
                -> CString -> CSize
                -> CString -> CSize
                -> Ptr CString
                -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_get"
  c_rocksdb_get :: Ptr CDB
                -> Ptr CReadOptions
                -> CString -> CSize
                -> Ptr CSize -- ^ Output length.
                -> Ptr CString
                -> IO CString

foreign import ccall safe "rocksdb/c.h rocksdb_writeoptions_create"
  c_rocksdb_writeoptions_create :: IO (Ptr CWriteOptions)

foreign import ccall safe "rocksdb/c.h rocksdb_writeoptions_destroy"
  c_rocksdb_writeoptions_destroy :: Ptr CWriteOptions -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_readoptions_create"
  c_rocksdb_readoptions_create :: IO (Ptr CReadOptions)

foreign import ccall safe "rocksdb/c.h rocksdb_readoptions_destroy"
  c_rocksdb_readoptions_destroy :: Ptr CReadOptions -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_writebatch_create"
  c_rocksdb_writebatch_create :: IO (Ptr CWriteBatch)

foreign import ccall safe "rocksdb/c.h rocksdb_writebatch_destroy"
  c_rocksdb_writebatch_destroy :: Ptr CWriteBatch -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_writebatch_put"
  c_rocksdb_writebatch_put :: Ptr CWriteBatch
                           -> CString -> CSize
                           -> CString -> CSize
                           -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_writebatch_delete"
  c_rocksdb_writebatch_delete :: Ptr CWriteBatch -> CString -> CSize -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_write"
  c_rocksdb_write :: Ptr CDB
                  -> Ptr CWriteOptions
                  -> Ptr CWriteBatch
                  -> Ptr CString
                  -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_delete"
  c_rocksdb_delete :: Ptr CDB
                   -> Ptr CWriteOptions
                   -> CString -> CSize
                   -> Ptr CString
                   -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_create_iterator"
               c_rocksdb_create_iterator ::
               Ptr CDB -> Ptr CReadOptions -> IO (Ptr CIterator)

foreign import ccall safe "rocksdb/c.h &rocksdb_iter_destroy"
  c_rocksdb_iter_destroy_ptr :: FunPtr (Ptr CIterator -> IO ())

foreign import ccall safe "rocksdb/c.h rocksdb_iter_valid"
  c_rocksdb_iter_valid :: Ptr CIterator -> IO Bool

foreign import ccall safe "rocksdb/c.h rocksdb_iter_seek"
  c_rocksdb_iter_seek :: Ptr CIterator -> CString -> CSize -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_iter_next"
  c_rocksdb_iter_next :: Ptr CIterator -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_iter_key"
  c_rocksdb_iter_key :: Ptr CIterator -> Ptr CSize -> IO CString

foreign import ccall safe "rocksdb/c.h rocksdb_iter_value"
  c_rocksdb_iter_value :: Ptr CIterator -> Ptr CSize -> IO CString

foreign import ccall safe "rocksdb/c.h rocksdb_options_set_compression"
  c_rocksdb_options_set_compression :: Ptr COptions -> CInt -> IO ()
