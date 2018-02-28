{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ForeignFunctionInterface #-}

-- | A safe binding to RocksDB.

module Database.RocksDB
  ( -- * Basic functions
    open
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
  -- * Snapshots
  , createSnapshot
  , releaseSnapshot
  -- * Misc types
  , RocksDBException(..)
  ) where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import           Control.Monad.IO.Class
import           Data.ByteString (ByteString)
import qualified Data.ByteString as S
import           Data.ByteString.Internal
import qualified Data.ByteString.Unsafe as S
import           Data.Coerce
import           Data.IORef
import           Data.Typeable
import           Foreign
import           Foreign.C
#ifndef mingw32_HOST_OS
import qualified GHC.Foreign as GHC
#endif
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

-- | Data compression mode.
data Compression
  = NoCompression
  | SnappyCompression
  | ZlibCompression
  deriving (Enum, Bounded, Show)

-- | Options for writing data to the database.
data WriteOptions = WriteOptions {}

-- | Options for reading data to the database.
data ReadOptions = ReadOptions
  { readOptionsSnapshot :: Maybe Snapshot
  }

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
  , iteratorRef :: !(IORef (Maybe (Ptr CIterator)))
    -- ^ This field is not protected; operations in an Iterator get a
    -- lock on the @DB@ with @withDBPtr@, making access to this
    -- safe. Without a database pointer, an iterator pointer is freed
    -- and no longer useful.
    --
    -- We use an IORef (Maybe a) so that we can do a
    -- `releaseIter`. But closing the connection is also another way
    -- to "release" an iterator, anyway.
  }

-- | A handle to a RocksDB snapshot. When handle becomes out of reach,
-- the snapshot is destroyed.
data Snapshot = Snapshot
  { snapshotDB :: !DB
    -- ^ We keep a reference to the DB for liveness purposes.
  , snapshotRef :: !(IORef (Maybe (Ptr CSnapshot)))
    -- ^ This field is not protected by an MVar. The snapshot is accessed
  }

-- | An exception thrown by this module.
data RocksDBException
  = UnsuccessfulOperation !String !String
  | AllocationReturnedNull !String
  | DatabaseIsClosed !String
  | IteratorIsClosed !String
  | SnapshotIsClosed !String
  | IteratorIsInvalid !String
  deriving (Typeable, Show, Eq)
instance Exception RocksDBException

--------------------------------------------------------------------------------
-- Defaults

-- | Default connection options.
defaultOptions :: FilePath -> Options
defaultOptions fp =
  Options
  { optionsCreateIfMissing = False
  , optionsFilePath = fp
  , optionsCompression = NoCompression
  }

-- | Default writing options.
defaultWriteOptions :: WriteOptions
defaultWriteOptions = WriteOptions {}

-- | Default reading options.
defaultReadOptions :: ReadOptions
defaultReadOptions = ReadOptions {readOptionsSnapshot = Nothing}

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
    (do mfptr <- modifyMVar (dbVar dbh) (pure . (Nothing, ))
        -- If an async exception comes after here, that's a pity
        -- because we wanted to free the file now. But with regards to
        -- safety, the finalizers will take care of closing
        -- eventually.
        maybe (evaluate ()) finalizeForeignPtr mfptr)

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
                      dbPtr
                      readOpts
                      (\optsPtr ->
                         uninterruptibleMask_
                           -- These operations are wrapped in
                           -- uninterruptible mask to avoid memory
                           -- leaks in the case of an async
                           -- exception. This is fine because:
                           --
                           -- 1. Foreign function calls are already not interruptible.
                           -- 2. Everything from Foreign.* is not interruptible.
                           -- 3. By re-using val_ptr, we avoid copying, which is a nice speed-up.
                           (do val_ptr <-
                                 assertNotError
                                   "get"
                                   (c_rocksdb_get
                                      dbPtr
                                      optsPtr
                                      key_ptr
                                      (fromIntegral klen)
                                      vlen_ptr)
                               if val_ptr == nullPtr
                                 then return Nothing
                                 else do
                                   fptr <-
                                     newForeignPtr c_rocksdb_free_ptr val_ptr
                                   len <- peek vlen_ptr
                                   let !bs = PS fptr 0 (fromIntegral len)
                                   pure (Just bs)))))))

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

-- | Create an snapshot on the DB. If the DB is later closed the
--  snapshot's functions will throw an exception.
createSnapshot :: MonadIO m => DB -> m Snapshot
createSnapshot db =
  liftIO
    (withDBPtr
       db
       "createSnapshot"
       (\dbPtr -> do
          snapshotPtr <-
            assertNotNull
              "c_rocksdb_create_snapshot"
              (c_rocksdb_create_snapshot dbPtr)
          var <- newIORef (Just snapshotPtr)
          pure (Snapshot {snapshotDB = db, snapshotRef = var})))

-- | Destroy an snapshot.
--
-- * Calling this function twice has no ill-effects.
--
-- * You don't have to call this; if you no longer hold a reference to
--   the @Snapshot@ then it will be closed upon garbage collection. But you
--   can call this function to do it early.
--
-- * Further operations will throw an exception on a closed @Snapshot@.
releaseSnapshot :: MonadIO m => Snapshot -> m ()
releaseSnapshot snapshot =
  liftIO
    (withDBPtr
       (snapshotDB snapshot)
       "releaseSnapshot"
       (\dbPtr ->
          liftIO
            (uninterruptibleMask_
               (do mptr <- atomicModifyIORef' (snapshotRef snapshot) (Nothing, )
                   maybe (pure ()) (c_rocksdb_release_snapshot dbPtr) mptr))))

-- | Create an iterator on the DB. If the DB is later closed the
--  iterator's functions will throw an exception.
createIter :: MonadIO m => DB -> ReadOptions -> m Iterator
createIter db opts =
  liftIO
    (withDBPtr
       db
       "createIter"
       (\dbPtr ->
          withReadOptions
            dbPtr
            opts
            (\readOpts -> do
               iterPtr <-
                 assertNotNull
                   "c_rocksdb_create_iterator"
                   (c_rocksdb_create_iterator dbPtr readOpts)
               var <- newIORef (Just iterPtr)
               pure (Iterator {iteratorDB = db, iteratorRef = var}))))

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
    (withDBPtr
       (iteratorDB iterator)
       "releaseIter"
       (const
          (liftIO
             (uninterruptibleMask_
                (do mptr <-
                      atomicModifyIORef' (iteratorRef iterator) (Nothing, )
                    maybe (pure ()) c_rocksdb_iter_destroy mptr)))))

-- | Seek to the given key in the iterator.
iterSeek ::
     MonadIO m
  => Iterator
  -> ByteString -- ^ Key.
  -> m ()
iterSeek iter key =
  withIterPtr
    iter
    "iterSeek"
    (\iterPtr ->
       S.unsafeUseAsCStringLen
         key
         (\(key_ptr, klen) ->
            c_rocksdb_iter_seek iterPtr key_ptr (fromIntegral klen)))

-- | Get the next entry from the iterator.
--
-- We can't take control of the allocated key/value, so the returned
-- @ByteString@ values are copied.
iterEntry :: MonadIO m => Iterator -> m (Maybe (ByteString, ByteString))
iterEntry iter =
  withIterPtr
    iter
    "iterEntry"
    (\iterPtr -> do
       valid <- c_rocksdb_iter_valid iterPtr
       if valid
         then do
           !mkey <-
             alloca
               (\klenp -> do
                  key <- c_rocksdb_iter_key iterPtr klenp
                  klen <- peek klenp
                  copyByteStringMaybe key klen)
           !mval <-
             alloca
               (\vlenp -> do
                  val <- c_rocksdb_iter_value iterPtr vlenp
                  vlen <- peek vlenp
                  copyByteStringMaybe val vlen)
           pure ((,) <$> mkey <*> mval)
         else pure Nothing)

-- | Go to the next item in the iterator.
iterNext :: MonadIO m => Iterator -> m ()
iterNext iter =
  withIterPtr iter "iterNext" (\iterPtr -> c_rocksdb_iter_next iterPtr)

--------------------------------------------------------------------------------
-- Internal functions

-- | Do something with the iterator. This only succeeds if the db is
-- open and the iterator is not released.
withIterPtr :: MonadIO m => Iterator -> String -> (Ptr CIterator -> IO a) -> m a
withIterPtr iter label f =
  liftIO
    (withDBPtr
       (iteratorDB iter)
       label
       (const
          (do mfptr <- readIORef (iteratorRef iter)
              case mfptr of
                Nothing -> throwIO (IteratorIsClosed label)
                Just it -> f it)))

-- | Do something with the snapshot. This only succeeds if the db
-- snapshot is not released. This function expects a locked database
-- handle, via @withDBPtr@.
withSnapshotPtr :: MonadIO m => Ptr CDB -> Snapshot -> String -> (Ptr CSnapshot -> IO a) -> m a
withSnapshotPtr _ snapshot label f =
  liftIO
    (do mfptr <- readIORef (snapshotRef snapshot)
        case mfptr of
          Nothing -> throwIO (SnapshotIsClosed label)
          Just it -> f it)

-- | Do something with the pointer inside. This is thread-safe.
withDBPtr :: DB -> String -> (Ptr CDB -> IO a) -> IO a
withDBPtr dbh label f =
  withMVar
    (dbVar dbh)
    (\mfptr ->
       case mfptr of
         Nothing -> throwIO (DatabaseIsClosed label)
         Just db -> do
           v <- withForeignPtr db f
           touchForeignPtr db
           pure v)

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

withReadOptions :: Ptr CDB -> ReadOptions -> (Ptr CReadOptions -> IO a) -> IO a
withReadOptions dbPtr ReadOptions {readOptionsSnapshot = msnapshot} f =
  bracket
    (assertNotNull "c_rocksdb_readoptions_create" c_rocksdb_readoptions_create)
    c_rocksdb_readoptions_destroy
    (\opts -> do
       maybe
         (pure ())
         (\snapshot ->
            withSnapshotPtr
              dbPtr
              snapshot
              "withReadOptions"
              (c_rocksdb_readoptions_set_snapshot opts))
         msnapshot
       f opts)

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

-- | Copy the given CString, because we can't adopt it.
--
-- If the string is NULL, just return Nothing.
copyByteStringMaybe :: CString -> CSize -> IO (Maybe ByteString)
copyByteStringMaybe val_ptr vlen =
  if val_ptr == nullPtr
    then return Nothing
    else fmap Just (S.packCStringLen (val_ptr, fromIntegral vlen))

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
data CSnapshot

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
                -> IO (Ptr Word8)

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

foreign import ccall safe "rocksdb/c.h rocksdb_create_snapshot"
  c_rocksdb_create_snapshot ::
  Ptr CDB -> IO (Ptr CSnapshot)

foreign import ccall safe "rocksdb/c.h rocksdb_release_snapshot"
  c_rocksdb_release_snapshot ::
  Ptr CDB -> Ptr CSnapshot -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_readoptions_set_snapshot"
  c_rocksdb_readoptions_set_snapshot :: Ptr CReadOptions -> Ptr CSnapshot -> IO ()

foreign import ccall safe "rocksdb/c.h rocksdb_iter_destroy"
  c_rocksdb_iter_destroy :: Ptr CIterator -> IO ()

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

foreign import ccall safe "rocksdb/c.h &rocksdb_free"
  c_rocksdb_free_ptr :: FunPtr (Ptr a -> IO ())
