-- | Comprehensive aggressive test-suite.

module Main where

import Test.Hspec

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "Open/create" open

open :: Spec
open = do
  it "Open/create a database" pending
