{-# LANGUAGE TemplateHaskell #-}
module TestAux where

import Ciel

-- import Debug.Trace (trace)

sumup :: [Int] -> CielWF Int
sumup is = return $ sum is

add5 :: Int -> CielWF Int
add5 i = return (i+5)

add1 :: Int -> CielWF Int
add1 i = return (i+1)

$(remotable ['sumup,'add5,'add1])
