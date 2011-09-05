{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
import Ciel
import Control.Concurrent
import Control.Monad.Trans
import Text.JSON (JSValue)

import TestAux


cielTest2 :: CielWF String
cielTest2 = 
   do doIO $ putStrLn "Starting test"
      r1 <- mapM (\x -> doCiel $ spawn (add5__closure x)) [1..50]
      doCiel $ resolve r1
      v1 <- mapM (doCiel . readRef) r1
      r2 <- mapM (\x -> doCiel $ spawn (add1__closure x)) v1
      doCiel $ resolve r2
      v2 <- mapM (doCiel . readRef) r2
      rs <- doCiel $ spawn (sumup__closure v2)
      vs <- doCiel $ readRef rs
      return $ "Final result is: " ++ show vs


cielTest1 :: CielWF Int
cielTest1 =                do doIO $ putStrLn "hi"
                              doIO $ threadDelay 5000000
                              doIO $ putStrLn "bye"
                              q <- doCiel $ spawn $(mkClosureRec 'cielChild) :: CielWF (Ref Int)
                              v <- doCiel $ readRef q
                              doIO $ putStrLn $ show v
                              return (3::Int)

cielChild :: CielWF Int
cielChild = do doIO $ putStrLn "yoyoyoyo"
               return (9::Int)

$(remotable ['cielTest1, 'cielChild,'cielTest2])

main = 
    do registerType :: IO [Int]
       registerType :: IO (Ref [Int])
       registerType :: IO [Ref Int]
       cielInit [Main.__remoteCallMetaData,
                 TestAux.__remoteCallMetaData] cielTest2__closure
