{-# LANGUAGE DeriveDataTypeable #-}
module Ciel (CielWF, Ref,

             cielInit,

             spawn, readRef, readRefBlocking, resolveBlocking,
             
             doWF, doIO, doCiel,

             registerType,

             remotable, mkClosure, mkClosureRec) where

{-

This version requires a hacked up RefSerialize; TCache
version 0.6.5 (Workflow won't with with later versions); and
the HaskellExecutor on CIEL.

To do: general clean-up, better error handling
       spawning without explicit closures: this is probably
          possible by "forking" the current workflow
       similarly, the resolve function should be able to
          halt the program until its deps are met, but
          this requires the ability to distinguish the
          "first run" from "restart." I think it is possible
          to query Workflow for the current step number,
          and include that in the args of the tailspawn
-}

import Control.Exception (Exception,throw,catch,bracket,NonTermination(..),finally,SomeException)
import Prelude hiding (catch)
import Control.Monad (when,liftM)
import Control.Monad.Trans (liftIO,MonadIO)
import Data.Typeable (Typeable,typeOf)
import System.IO.Temp (withTempDirectory)
import Control.Workflow (registerType,Workflow,step,startWF,plift,defPath,getWFHistory,unsafeIOtoWF)
import Data.TCache.IResource (IResource(..))
import Data.RefSerialize (Serialize(..))
import System.Random
import Data.RefSerialize.Parser (choice,symbol)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BL8
import Data.Maybe (fromJust)
import Data.Binary.Get
import Data.Binary.Put
import Data.Binary (Binary)
import qualified Data.Binary as Binary
import qualified Codec.Archive.Zip as Zip
import Text.JSON.String
import Text.JSON
import Control.Concurrent.MVar
import Text.JSON.Types
import System.Directory
import System.Environment (getArgs,getProgName)
import System.IO (hIsOpen,withBinaryFile,IOMode(..),hGetContents,Handle,hPutStrLn,BufferMode(..),hFlush,hSetBuffering,openBinaryFile,hClose)
import Ciel.Call
import Ciel.Types

import System.Posix.IO (handleToFd,fdRead)
import Control.Concurrent (forkIO,threadDelay,ThreadId,threadWaitRead)

import Debug.Trace

data ReferenceUnavailableException = ReferenceUnavailableException JSValue deriving (Typeable,Show)
instance Exception ReferenceUnavailableException

data DummyException = DummyException deriving (Typeable,Show)
instance Exception DummyException

step1 :: (Monad m, MonadIO m, IResource a, Typeable a) => m a -> Workflow m a
step1 = step
{-step1 proc= do
    r <- step proc 
    trace ("@@@" ++ show (typeOf r)) (return())
    unsafeIOtoWF $ do
        (cache,_) <- readIORef refcache
        H.delete cache $ keyResource Stat{wfName=  "CielHaskell#()"}
        getWFHistory "CielHaskell" ()
    return r-}

getCielState :: Ciel CielState
getCielState = Ciel $ \x -> return (x,x)

putCielState :: CielState -> Ciel ()
putCielState newval = Ciel $ \_ -> return (newval,())

setWFName :: String -> Ciel ()
setWFName newname = 
   do ts <- getCielState
      putCielState ts {csWFName=Just newname}

setClosure :: JSValue -> Ciel ()
setClosure newname = 
   do ts <- getCielState
      putCielState ts {csClosure=Just newname}

setBinaryName :: Maybe JSValue -> Ciel ()
setBinaryName bin =
   do ts <- getCielState
      putCielState ts {csBinaryName = newname}
   where newname = case bin of
                      Just (JSString s) -> Just $ fromJSString s
                      _ -> Nothing

getBinaryName :: Ciel String
getBinaryName =
  do ts <- getCielState
     case csBinaryName ts of
       Just s -> return s
       _ -> error "Binary name not set"

data Ref a = RFuture String
           | RValue String String
           | RConcrete JSValue
           | RNull
           deriving Typeable
{-
  |Stream
  |Sweetheart
  |Value of id * string 
  |Completed
-}

instance Serialize (Ref a) where
    showp RNull = return "RNull"
    showp (RFuture x) = showp x >>= \sx -> return $ "RFuture " ++ sx
    showp (RConcrete v) = showp (show v) >>= \sv -> return $ "RConcrete " ++ sv
    showp (RValue k v) =
         do ks <- showp k
            vs <- showp v
            return ("RValue " ++ ks ++ vs)

    readp =  choice [rNull,rFuture,rValue,rConcrete] where
      rConcrete = symbol "RConcrete" >> readp >>= \v -> return $ RConcrete (read v)
      rNull = symbol "RNull" >> return RNull
      rValue = symbol "RValue" >> readp >>= \k -> readp >>= \v -> return $ RValue k v
      rFuture = symbol "RFuture" >> readp >>= \x -> return $ RFuture x

instance JSON (Ref a) where
   showJSON (RFuture s) = refWrap $ JSArray [JSString $ toJSString "f2",JSString $ toJSString s]
   showJSON (RValue k v) = refWrap $ JSArray [JSString $ toJSString "val",JSString $ toJSString k,JSString $ toJSString v]
   showJSON (RConcrete v) = refWrap $ v
   showJSON _ = error "Unsupported ref type"

   readJSON (JSObject a) = case get_field a "__ref__" of
                  Nothing -> error "No kind of ref"
                  Just v -> helper v
       where
         helper q@(JSArray ((JSString flag):_))
          | fromJSString flag == "c2" = Ok $ RConcrete q
         helper (JSArray [JSString flag,JSString fid])
          | fromJSString flag == "f2" = Ok $ RFuture (fromJSString fid)
         helper (JSArray [JSString flag,JSString k,JSString v])
          | fromJSString flag == "val" = Ok $ RValue (fromJSString k) (fromJSString v)
         helper _ = error "Unsupported ref read type"
   readJSON _ = error "Not an object even"   

refWrap a = makeObj [("__ref__",a)]

instance Show (Ref a) where
   show (RFuture s) = s
   show (RValue k v) = k
   show (RConcrete v) = show v
   show _ = "<null reference>"

refFromJS :: JSON a => JSValue -> Ref a
refFromJS obj = 
  case readJSON obj of
     Ok n -> n
     Error err -> error err
 
pcatch :: Exception e => Ciel a -> (e -> Ciel a) -> Ciel a
pcatch body handler = 
   Ciel $ \ts -> let mbody = runCiel body ts
                     mhandler = \e -> runCiel (handler e) ts
                  in catch mbody mhandler

pforkIO :: Ciel () -> Ciel ThreadId
pforkIO body = Ciel $ \ts -> let mbody = runCiel body ts >> return ()
                              in do a <- forkIO mbody 
                                    return (ts,a)

pfinally :: Ciel a -> Ciel b -> Ciel a
pfinally body handler = 
   Ciel $ \ts -> let mbody = runCiel body ts
                     mhandler = runCiel handler ts
                  in finally mbody mhandler


doWF :: WF a -> CielWF a
doWF body = CielWF $  body

doIO :: (JSON a,IResource a, Typeable a) => IO a -> CielWF a
doIO body = doWF $ step1 $ liftIO body

doCiel :: (JSON a, IResource a,Typeable a) => Ciel a -> CielWF a
doCiel body = doWF $ step1 $ body

unWF (CielWF a) = a


openRef :: Ref a -> Bool -> Ciel FilePath
openRef ref sweetheart =
  do putMessage outgoing
     msg <- getMessage
     case msg of
        JSArray [JSString cmd,JSObject obj] 
            | fromJSString cmd == "open_ref" ->
                 case get_field obj "filename"  of
                   Just (JSString fn) -> return (fromJSString fn)
                   _ -> throw $ ReferenceUnavailableException $ showJSON ref
        _ -> throw $ ReferenceUnavailableException $ showJSON ref
  where outgoing = JSArray [JSString $ toJSString "open_ref",
                     JSObject $ toJSObject [("ref",showJSON ref),
                       ("make_sweetheart",JSBool sweetheart)]]

readRefBlocking :: (JSON a,IResource a,Typeable a) => Ref a -> CielWF a
readRefBlocking = doCiel . readRefBlocking'

readRefBlocking' :: (JSON a,Typeable a) => Ref a -> Ciel a
readRefBlocking' ref = resolveBlocking' [ref] >> readRef' ref

readRef :: (JSON a,Typeable a, IResource a) => Ref a -> CielWF a
readRef = doCiel . readRef'

readRef' :: (JSON a,Typeable a) => Ref a -> Ciel a
readRef' ref =
  do fp <- openRef ref False
     res <- liftM decode $ liftIO $ readFile fp
     case res of
       Ok a -> return a
       Error err -> error err

putMessageByValue :: JSON a => a -> Ciel ()
putMessageByValue jv =
  putMessage (showJSON jv)

putMessage :: JSValue -> Ciel ()
putMessage inp =
  do cs <- getCielState
     val `seq` liftIO $ BL.hPut (csOut cs) val
--     liftIO $ BL.putStrLn val
     liftIO $ hFlush (csOut cs)
  where
        encoding = showJSValue inp []
        val = runPut (putWord32be (fromIntegral $! length encoding) >> 
                      putLazyByteString (BL8.pack encoding))


getMessage :: Ciel JSValue
getMessage =
  do cs <- getCielState
     liftIO $ threadWaitRead (csIn cs)

     lenbs <- liftM fst $ liftIO $ fdRead (csIn cs) 4
     let len = runGet getWord32be (BL8.pack lenbs)

     liftIO $ threadWaitRead (csIn cs)

     buf <- len `seq` liftM fst $ liftIO $ fdRead (csIn cs) (fromIntegral len)
--     liftIO $ putStrLn buf
     case runGetJSON readJSValue (buf) of
       Right a -> return a
       Left err -> error err

cmdExit :: Bool -> Ciel ()
cmdExit fixed = putMessage val
  where val = JSArray [JSString $ toJSString "exit",task]
        task = makeObj [("keep_process",JSString $ toJSString isfixed)]
        isfixed = if fixed then "must_keep" else "no"

requireField :: JSValue -> String -> JSValue
requireField (JSObject obj) field =
     case get_field obj field of
        Nothing -> error $ "Require " ++ field ++ " not found"
        Just a -> a
requireField _ _ = error "Expected JSObject, got elsewhat"

requirePayload :: JSValue -> String -> JSValue
requirePayload jsv cmd =
  case jsv of
    JSArray [JSString incmd,obj]
       | fromJSString incmd == cmd -> obj
    _ -> error "Unexpected command result"

outputValue :: JSON a => a -> Ciel (Ref a)
outputValue val =
  do fp <- cmdOpenOutput 0 False False False
     liftIO $ writeFile fp (encode val) -- binary?
     cmdCloseOutput 0 Nothing

outputValueDirect :: String -> Ciel ()
outputValueDirect val =
  do fp <- cmdOpenOutput 0 False False False
     liftIO $ writeFile fp val
     cmdCloseOutput 0 Nothing :: Ciel (Ref ())
     return ()

outputNewValue :: JSON a => a -> Ciel (Ref a)
outputNewValue val =
  do idx <- cmdAllocateOutput 
     fp <- cmdOpenOutput idx False False False
     liftIO $ writeFile fp (encode val)
     cmdCloseOutput idx Nothing


setDummy :: Ciel a -> Ciel a
setDummy body = 
           do a <- getCielState
              (st,res) <- liftIO $! runCiel body a {csDummy=True}
              return res

explodeDummy :: Ciel ()
explodeDummy = do ts <- getCielState
                  when (csDummy ts) (throw DummyException)

getRandomWFName :: IO String
getRandomWFName =
  do val <- getStdRandom (random) :: IO Integer
     return $ "cielhs-"++show val

spawn :: (Typeable a,IResource a,JSON a) => Closure (CielWF a) -> CielWF (Ref a)
spawn = doCiel . spawn'

spawn' :: (Typeable a,IResource a,JSON a) => Closure (CielWF a) -> Ciel (Ref a)
spawn' fn =
   do ts <- getCielState
      res <- cmdSpawn False False [] [JSString $ toJSString "___",
                 showJSON fn,
                 JSString $ toJSString $ fromJust $ csWFName ts] Nothing (fromJust $ csWFName ts)
      return $ refFromJS res

resolveBlocking :: [Ref a] -> CielWF ()
resolveBlocking = doCiel . resolveBlocking'

resolveBlocking' :: [Ref a] -> Ciel ()
resolveBlocking' refs = 
    do ts <- getCielState
       cmdSpawn True True (map showJSON refs) [] Nothing (fromJust $ csWFName ts)
       cmdExit True
       msg <- getMessage
       case requirePayload msg "start_task" of
             _ -> return ()

-- This is currently a no-op. What is SHOULD do is issue the below tailSpawn and cmdExit calls
-- and then exit the workflow with an exception. The problem is how to resume the workflow
-- at the following step, and thus avoid re-executing the exception throw every time
resolve :: [Ref a] -> Ciel ()
resolve refs = return () -- tailSpawn (map showJSON refs) >> cmdExit False

tailSpawn :: [JSValue] -> Ciel ()
tailSpawn deps =
  do state <- liftIO archiveState
     ts <- getCielState
     cmdSpawn True False deps 
           [JSString $ toJSString "___",fromJust $ csClosure ts,
             JSString $ toJSString $ fromJust $ csWFName ts] 
           (Just state) (fromJust $ csWFName ts)
     return ()

cmdSpawn :: Bool -> Bool -> [JSValue] -> [JSValue] -> Maybe BL.ByteString -> String -> Ciel JSValue
cmdSpawn istail isfixed deps args fnref wfname =
  do ref <- case fnref of
              Nothing -> return []
              Just st -> liftM (\x -> [("fn_ref",showJSON x)]) $ outputNewValue st
     arg0 <- getBinaryName
     putMessage (val arg0 ref)
     case istail of
         False -> 
           do msg <- getMessage
              case requirePayload msg cmdname of
                 JSArray [v] -> return v
                 _ -> error "Filename a whatsitnow?"
         True -> return JSNull
  where cmdname = case istail of
                     True -> "tail_spawn"
                     False -> "spawn"
        fixed = [("is_fixed",JSBool isfixed)]
        val arg0 ref = JSArray [JSString $ toJSString cmdname,task arg0 ref]
        task arg0 ref = makeObj $ [("n_outputs",JSRational False 1),
                        ("args",JSArray args),
                        ("binary",JSString $ toJSString arg0),
                        ("executor_name",JSString $ toJSString "haskell"),
                        ("extra_dependencies",JSArray deps)]++
                        ref ++ fixed

cmdAllocateOutput :: Ciel Int
cmdAllocateOutput =
   do putMessage val
      msg <- getMessage
      case requireField (requirePayload msg "allocate_output") "index" of
         JSRational _ v -> return $ round v
         _ -> error "Index a whoosiwhat?"
   where val = JSArray [JSString $ toJSString "allocate_output",makeObj [("prefix",JSString $ toJSString "obj")]]

cmdOpenOutput :: Int -> Bool -> Bool -> Bool -> Ciel FilePath
cmdOpenOutput idx isStream isPipe isSweetheart =
  do putMessage val
     msg <- getMessage
     case requireField (requirePayload msg "open_output") "filename" of
        JSString str -> return $ fromJSString str
        _ -> error "Filename a whatsitnow?"
  where val = JSArray [JSString $ toJSString "open_output",task]
        task = makeObj [("index",JSRational False (fromIntegral idx)),
                        ("may_stream",JSBool isStream),
                        ("may_pipe",JSBool isPipe),
                        ("make_local_sweetheart",JSBool isSweetheart)]

cmdCloseOutput :: JSON a => Int -> Maybe Int -> Ciel (Ref a)
cmdCloseOutput idx size =
   do putMessage val
      msg <- getMessage
      return $ refFromJS $ requireField (requirePayload msg "close_output") "ref"
   where val = JSArray [JSString $ toJSString "close_output",task]
         task = makeObj ([("index",JSRational False (fromIntegral idx))] ++ (maybeSize size))
         maybeSize Nothing = []
         maybeSize (Just sz) = [("size",JSRational False (fromIntegral sz))]

archiveState :: IO BL.ByteString
archiveState =
   do arch <- Zip.addFilesToArchive [Zip.OptRecursive] Zip.emptyArchive [path]
      return $! Zip.fromArchive arch
   where path = "Workflows/" -- hardcoded into Control.Workflow

dearchiveState :: BL.ByteString -> IO ()
dearchiveState bs =
   Zip.extractFilesFromArchive [Zip.OptRecursive] archive
   where archive = Zip.toArchive bs

workflowName = "CielHaskell"

invokeWF :: String -> Closure (CielWF a) -> Ciel ()
invokeWF wfname (Closure cloname clopl) = 
  do ts <- getCielState
     let gofun = getEntryByIdent (csLookup ts) cloname
     case gofun of
        Just userfun ->
            do mres <- liftIO newEmptyMVar
               pforkIO $! startWF wfname () [(wfname,\_ -> 
                                  do res <- unWF (userfun clopl)
                                     res `seq` unsafeIOtoWF $! putMVar mres (Right res)
                                     return ())] `pcatch`  (\(ReferenceUnavailableException r) -> liftIO $ putMVar mres (Left r)) `pcatch` (\NonTermination -> liftIO( putMVar mres (Left JSNull))) `pcatch` otherException
               rres <- liftIO $! readMVar mres
               case rres of
                   Left JSNull -> return ()
                   Left r -> throw $ ReferenceUnavailableException r
                   Right rq -> outputValueDirect rq
        Nothing -> error $ "Couldn't find function " ++ cloname
     where otherException n = 
                 do d <- liftIO getCurrentDirectory
                    liftIO $ putStrLn $ "Exception in "++d
                    throw (n::SomeException)

cielInit :: (Typeable a,IResource a,JSON a) => [RemoteCallMetaData] -> (JSValue -> Closure (CielWF a)) -> IO ()
cielInit rcmd userFun = 
  do diInit
     args <- getArgs
     case args of
       ["--write-fifo",wf,"--read-fifo",rf] -> inTemp $ gofifo rf wf
       ["--read-fifo",rf,"--write-fifo",wf] -> inTemp $ gofifo rf wf
       _ -> error "Ciel wrapper can't parse arguments"
   where
    inTemp body = withTempDirectory "." "cielhs-tmp-" $
                       (\fp -> do ocwd <- getCurrentDirectory
                                  setCurrentDirectory fp
                                  body `finally` setCurrentDirectory ocwd)
    table = registerCalls rcmd
    diInit =
       do registerType :: IO () -- TODO register other types
          registerType :: IO String
          registerType :: IO Int
          registerType :: IO (Ref String)
          registerType :: IO (Ref Int)
    getCielArgs obj =
       case get_field obj "args" of
         Nothing -> makeObj []
         Just args -> args         
    referenceWrapper body =
      do -- removeDirectoryRecursive $ defPath ()
         pcatch body referenceHandler
    referenceHandler (ReferenceUnavailableException theref) =
      do tailSpawn [theref]
         cmdExit False
    catchTermination body = pcatch body (\NonTermination -> return ()) >> return ()
    executeTask (JSObject jsv) = do
           setBinaryName (get_field jsv "binary")
           case getCielArgs jsv of
              JSArray [JSString a,clo,JSString wfname] | fromJSString a == "___" -> 
                   do setWFName (fromJSString wfname)
                      let theclo = case readJSON clo of
                                     Ok ref -> ref
                                     Error err -> error err
                      setClosure clo
                      referenceWrapper $ catchTermination $ 
                         do case get_field jsv "fn_ref" of
                               Just q -> case readJSON q of
                                           Ok v -> do state <- readRef' v
                                                      liftIO $ dearchiveState state

                                                      qq <- liftIO $ getCurrentDirectory
                                                      trace ("Restored and about to restart " ++ show theclo++" in "++qq) (return())
                                           Error err -> error err
                               Nothing -> return ()
                            invokeWF (fromJSString wfname) theclo
                            cmdExit False
              _ -> do setWFName workflowName
                      let invoking = userFun (getCielArgs jsv)
                      setClosure $ showJSON invoking
                      referenceWrapper $  catchTermination $
                           do res <- invokeWF workflowName invoking
                              cmdExit False
                              return res
    executeTask _ = error "Unexpected task type"   
    openBinaryFileCond f m =
         openBinaryFile f m `catch` \n -> do putStrLn $ "Got " ++ show (n::IOError) ++ ", retrying"
                                             threadDelay 1000000
                                             openBinaryFileCond f m
    gofifo rf wf =
        do hin <- openBinaryFileCond rf ReadMode
           hout <- openBinaryFileCond wf WriteMode
           hSetBuffering hin NoBuffering
           fdin <- handleToFd hin
           runCiel messageLoop CielState {csIn=fdin,csOut=hout,
                                          csDummy=False,csWFName=Nothing,
                                          csBinaryName=Nothing,
                                          csLookup=table,
                                          csClosure=Nothing}
           return ()
    messageLoop =
      do msg <- getMessage
         again <- msg `seq` handleMessage msg
         case again of
             Nothing -> return ()
             Just jsv ->
                do executeTask jsv
                   -- actually, there is no loop
    handleMessage jv =
      case jv of
         JSArray [JSString cmd,task] 
           | fromJSString cmd == "die" -> 
                 return Nothing
           | fromJSString cmd == "start_task" ->
                 return $ Just task
         _  -> error "Unknown command"


