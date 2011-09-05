 {-# LANGUAGE TemplateHaskell,DeriveDataTypeable,GeneralizedNewtypeDeriving #-}

module Ciel.Call where

import Language.Haskell.TH
import Control.Monad (liftM)
import Control.Monad.Trans (liftIO,MonadIO)
import Data.Dynamic (Dynamic,toDyn,fromDynamic)
import Data.Typeable (Typeable,Typeable1,typeOf,typeOf1,mkAppTy,mkTyCon)
import qualified Data.Map as Map (insert,lookup,Map,empty)
import Control.Workflow (Workflow)
import System.IO (Handle)
import Data.ByteString.Lazy (ByteString)
import Text.JSON 
import System.Posix.Types (Fd)

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

-----------------------

type RemoteCallMetaData = Lookup -> Lookup

type Identifier = String

data Entry = Entry {
               entryName :: Identifier,
               entryFunRef :: Dynamic
             }

registerCalls :: [RemoteCallMetaData] -> Lookup
registerCalls [] = empty
registerCalls (h:rest) = let registered = registerCalls rest
                          in h registered

makeEntry :: (Typeable a) => Identifier -> a -> Entry
makeEntry ident funref = Entry {entryName=ident, entryFunRef=toDyn funref}

type IdentMap = Map.Map Identifier Entry
data Lookup = Lookup { identMap :: IdentMap }

putReg :: (Typeable a) => a -> Identifier -> Lookup -> Lookup
putReg a i l = putEntry l a i

putEntry :: (Typeable a) => Lookup -> a -> Identifier -> Lookup
putEntry amap value name = 
                             Lookup {
                                identMap = Map.insert name entry (identMap amap)
                             }
  where
       entry = makeEntry name value


getEntryByIdent :: (Typeable a) => Lookup -> Identifier -> Maybe a
getEntryByIdent amap ident = (Map.lookup ident (identMap amap)) >>= (\x -> fromDynamic (entryFunRef x))

empty :: Lookup
empty = Lookup {identMap = Map.empty}

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

plencode :: (JSON a,Typeable a) => a -> Payload
plencode a = let encoding = encode a
                      in encoding `seq` Payload {payloadType = show $ typeOf a,
                                                 payloadContent = encoding}

pldecode :: (JSON a,Typeable a) => Payload -> Maybe a
pldecode a = (\id -> 
                      let pc = payloadContent a
                      in
                        pc `seq`
                        if (payloadType a) == 
                              show (typeOf $ id undefined)
                           then case decode pc of
                                   Ok a -> Just (id a)
                                   _ -> Nothing 
                           else Nothing) id

mkClosure :: Name -> Q Exp
mkClosure n = do info <- reify n
                 case info of
                    VarI iname _ _ _ -> 
                        do let newn = mkName $ show iname ++ "__closure"
                           newinfo <- reify newn
                           case newinfo of
                              VarI newiname _ _ _ -> varE newiname
                              _ -> error $ "Unexpected type of closure symbol for "++show n
                    _ -> error $ "No closure corresponding to "++show n


mkClosureRec :: Name -> Q Exp
mkClosureRec name =
  do loc <- location
     info <- reify name
     case info of
       VarI aname atype _ _ -> 
        let modname = case nameModule aname of
                        Just a -> a
                        _ -> error "Non-module name at mkClosureRec "++show name
         in
           case modname == loc_module loc of
             True -> let implFqn = loc_module loc ++"." ++ nameBase aname ++ "__impl" 
                         closurecall = [e| Closure |]
                         encodecall = [e| plencode |]
                         paramnames = map (\x -> 'a' : show x) [1..(length (init arglist))]
                         arglist = getParams atype
                         paramnamesP = (map (varP . mkName) paramnames)
                         paramnamesE = (map (varE . mkName) paramnames)
                         paramnamesT = map (\(v,t) -> sigE v (return t)) (zip paramnamesE arglist)
                      in lamE paramnamesP (appE (appE closurecall (litE (stringL implFqn))) (appE encodecall (tupE paramnamesT)))
             False -> error $ show aname++": mkClosureRec can only make closures of functions in the same module; use mkClosure instead"
       _ -> error $ "Unexpected type of symbol for "++show name
     
remotable :: [Name] -> Q [Dec]
remotable names =
    do info <- liftM concat $ mapM getType names 
       loc <- location
       declGen <- mapM (makeDec loc) info
       decs <- sequence $ concat (map fst declGen)
       let outnames = concat $ map snd declGen
       regs <- sequence $ makeReg loc outnames
       return $ decs ++ regs
    where makeReg loc names = 
              let
                      mkentry = [e| putReg |]
                      regtype = [t| RemoteCallMetaData |]
                      registryName = (mkName "__remoteCallMetaData")
                      reasonableNameModule name = maybe (loc_module loc++".") ((flip (++))".") (nameModule name)
                      app2E op l r = appE (appE op l) r
                      param = mkName "x"
                      applies [] = varE param
                      applies [h] = appE (app2E mkentry (varE h) (litE $ stringL (reasonableNameModule h++nameBase h))) (varE param)
                      applies (h:t) = appE (app2E mkentry (varE h) (litE $ stringL (reasonableNameModule h++nameBase h))) (applies t)
                      bodyq = normalB (applies names)
                      sig = sigD registryName regtype
                      dec = funD registryName [clause [varP param] bodyq []]
               in [sig,dec]
          makeDec loc (aname,atype) =
              do payload <- [t| Payload |]
                 ttwfm <- return $ ConT $ mkName "WF"-- [t| WF |]
                 ttclosure <- [t| Closure |]
                 return $ let
                    implName = mkName (nameBase aname ++ "__impl") 
                    implPlName = mkName (nameBase aname ++ "__implPl")
                    implFqn = loc_module loc ++"." ++ nameBase aname ++ "__impl"
                    closureName = mkName (nameBase aname ++ "__closure")
                    paramnames = map (\x -> 'a' : show x) [1..(length (init arglist))]
                    paramnamesP = (map (varP . mkName) paramnames)
                    paramnamesE = (map (varE . mkName) paramnames)
                    closurearglist = init arglist ++ [processmtoclosure (last arglist)]
                    implarglist = payload : [toPayload (last arglist)]
                    toProcessM a = (AppT ttwfm a)
                    toPayload x = case funtype of
                                     0 -> case x of
                                           (AppT (ConT n) _) -> (AppT (ConT n) payload)
                                           _ -> toProcessM payload
                                     _ -> toProcessM payload
                    processmtoclosure (AppT mc x) | mc == ttwfm && isarrow x = AppT ttclosure x
                    processmtoclosure (x) =  (AppT ttclosure x)
                    isarrowful = isarrow $ last arglist
                    isarrow (AppT (AppT ArrowT _) _) = True
                    isarrow (AppT (process) v) 
                        | isarrow v && (process == ttwfm) = True
                    isarrow _ = False
                    applyargs f [] = f
                    applyargs f (l:r) = applyargs (appE f l) r
                    funtype = 0
                    just a = conP (mkName "Prelude.Just") [a]
                    errorcall = [e| Prelude.error |]
                    returnf = [e| Prelude.return |]
                    liftm = [e| liftM |]
                    asProcessM x = case funtype of
                                     0 -> x
                    decodecall = [e| pldecode |]
                    encodecall = [e| plencode |]
                    closurecall = [e| Closure |]
                    jsonencode = [e|encode|]
                    stringt = [t|CielWF String|]
                    closuredec = sigD closureName (return $ putParams closurearglist)
                    closuredef = funD closureName [clause paramnamesP
                                           (normalB (appE (appE closurecall (litE (stringL implFqn))) (appE encodecall (tupE paramnamesE))))
                                           []
                                          ]
                    impldec = sigD implName (appT (appT arrowT (return payload)) (stringt))
                    impldef = funD implName [clause [varP (mkName "a")]
                                                          (normalB (caseE (appE decodecall (varE (mkName "a")))
                                                                [match (just (tupP paramnamesP)) (normalB  (appE (appE liftm jsonencode) (applyargs (varE aname) paramnamesE) ) ) [],
                                                                 match wildP (normalB (appE (errorcall) (litE (stringL ("Bad decoding in closure splice of "++nameBase aname))))) []]))
                                            []]
                    arglist = getParams atype
                  in ([closuredec,closuredef,impldec,impldef],
                              [aname,implName])
 
getType name = 
  do info <- reify name
     case info of 
       VarI iname itype _ _ -> return [(iname,itype)]
       _ -> return []

putParams (afst:lst:[]) = AppT (AppT ArrowT afst) lst
putParams (afst:[]) = afst
putParams (afst:lst) = AppT (AppT ArrowT afst) (putParams lst)
putParams [] = error "Unexpected parameter type in remotable processing"
getParams typ = case typ of
                            AppT (AppT ArrowT b) c -> b : getParams c
                            b -> [b]
                                

