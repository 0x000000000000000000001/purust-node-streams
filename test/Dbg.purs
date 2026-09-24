module Test.Dbg where

import Prelude

import Control.Parallel (parSequence)
import Effect (Effect)
import Effect.Aff (error, launchAff_, throwError)
import Effect.Class (liftEffect)
import Effect.Class.Console as Console
import Node.Stream (destroy, newPassThrough)
import Node.Stream.Aff (readAll, toStringUTF8)

main :: Effect Unit
main = launchAff_ do
  let loop i = when (i <= 100) do
        s <- liftEffect newPassThrough
        liftEffect $ Console.error ("start " <> show i)
        result <- parSequence
          [ toStringUTF8 =<< readAll s
          , liftEffect (destroy s) *> pure "destroyed"
          ]
        unless (result == [ "", "destroyed" ]) $ throwError $ error (show result)
        liftEffect $ Console.error ("done " <> show i)
        loop (i + 1)
  loop 1
