{-# LANGUAGE DeriveDataTypeable,GeneralizedNewtypeDeriving #-}
module Ciel.Types (CielWF(..), WF, CielState(..), Ciel(..), Payload(..), Closure(..), RemoteCallMetaData, Lookup(..),Entry(..), Identifier) where

import System.Posix.Types (Fd)
import Text.JSON
import Data.Dynamic (Dynamic)
import qualified Data.Map as Map
import Control.Workflow (Workflow)
import Data.Typeable (Typeable)
import Data.Typeable (Typeable,typeOf)
import System.IO (Handle)
import Control.Monad.Trans (liftIO, MonadIO)

newtype CielWF a = CielWF (WF a) deriving (Typeable,Monad)

type WF a = (Workflow Ciel a)



data CielState = CielState { csIn :: Fd
                           , csOut :: Handle 
                           , csDummy :: Bool
                           , csWFName :: Maybe String
                           , csBinaryName :: Maybe String
                           , csLookup :: Lookup
                           , csClosure :: JSValue }

data Ciel a = Ciel { runCiel :: CielState -> IO (CielState, a) }


instance Monad Ciel where
   m >>= k = Ciel $! \ts -> do
                (ts',a) <- runCiel m ts
                (ts'',a') <- runCiel (k a) (ts')
                return (ts'',a')              
   return x = Ciel $! \ts -> return $! (ts,x)

instance MonadIO Ciel where
    liftIO arg = Ciel $! \ts -> do
                    v <- liftIO $! arg
                    return (ts,v)

data Payload = Payload
                { 
                  payloadType :: !String,
                  payloadContent :: !String
                } deriving (Typeable)

instance JSON Payload where
  showJSON pl = JSArray [showJSON $ payloadType pl,showJSON $ payloadContent pl]
  readJSON (JSArray [pt,pv]) = Ok $ Payload {payloadType=fromOk $ readJSON pt,payloadContent=fromOk $ readJSON pv}
  readJSON _ = error "blah blah"


data Closure a = Closure String Payload
     deriving (Typeable)

instance Show (Closure a) where
     show a = case a of
                (Closure fn pl) -> show fn

instance JSON (Closure a) where
   showJSON (Closure s p) = JSArray [showJSON s,showJSON p]
   readJSON (JSArray [s,p]) = Ok $ Closure (fromOk $ readJSON s) (fromOk $ readJSON p)
   readJSON _ = error "Not an object even"   


fromOk (Ok n) = n
fromOk (Error n) = error n



type RemoteCallMetaData = Lookup -> Lookup

type Identifier = String

data Entry = Entry {
               entryName :: Identifier,
               entryFunRef :: Dynamic
             }

type IdentMap = Map.Map Identifier Entry
data Lookup = Lookup { identMap :: IdentMap }


