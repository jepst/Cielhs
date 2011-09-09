{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
import Ciel
import Control.Concurrent
import Control.Monad.Trans
import Text.JSON (JSValue)

import TestAux


-- Uses Workflow-based continuations. When an unresolved
-- reference is encountered, the job will suspend until
-- it is restarted by the master.
cielTest3 :: JSValue -> CielWF String
cielTest3 arg = 
   do doIO $ putStrLn $ "Starting test "
      r1 <- mapM (\x -> spawn (add5__closure x)) [1..50]
      v1 <- mapM readRef r1
      r2 <- mapM (\x -> spawn (add1__closure x)) v1
      v2 <- mapM readRef r2
      rs <- spawn (sumup__closure v2)
      vs <- readRef rs
      return $ "Final result is: " ++ show vs

-- "Fixed" version blocks until references are resolved.
-- resolveBlock will block (and not tail recurse) until
-- desired refs are available.
-- Alternatively, use readRefBlock for same effect.
cielTest2 :: JSValue -> CielWF String
cielTest2 arg = 
   do doIO $ putStrLn $ "Starting test "
      r1 <- mapM (\x -> spawn (add5__closure x)) [1..50]
      resolveBlocking r1
      v1 <- mapM readRef r1
      r2 <- mapM (\x -> spawn (add1__closure x)) v1
      resolveBlocking r2
      v2 <- mapM readRef r2
      rs <- spawn (sumup__closure v2)
      resolveBlocking [rs]
      vs <- readRef rs
      return $ "Final result is: " ++ show vs

-- Automagically generate closures for remotely-invokable functions
$(remotable ['cielTest2, 'cielTest3])

main = 
    do registerTypes
       cielInit [Main.__remoteCallMetaData,
                 TestAux.__remoteCallMetaData] cielTest3__closure
    where registerTypes =
          -- All monadic types used in a CielWF must be explicitly registered here
             do registerType :: IO [Int] 
                registerType :: IO (Ref [Int])
                registerType :: IO [Ref Int]
