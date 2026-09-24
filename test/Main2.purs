-- | How to test:
-- |
-- | ```
-- | spago test --main Test.Main2
-- | ```
-- |
-- | Writes many chunks through a `PassThrough` and reads them back. The
-- | original suite compared `expected == expected`, so its check could never
-- | fail; the port counts the actual lines and exits with a non-zero status so
-- | the runner can rely on the process status.
module Test.Main2 where

import Prelude

import Data.Array as Array
import Data.Either (Either(..))
import Data.String (Pattern(..))
import Data.String as String
import Effect (Effect)
import Effect.Aff (Error, error, forkAff, joinFiber, runAff_, throwError)
import Effect.Class (liftEffect)
import Effect.Class.Console as Console
import Node.Buffer as Buffer
import Node.Encoding (Encoding(..))
import Node.Process (exit')
import Node.Stream (newPassThrough)
import Node.Stream.Aff (end, readAll, write)

completion :: Either Error Unit -> Effect Unit
completion = case _ of
  Left e -> do
    Console.error (show e)
    exit' 1
  Right _ -> do
    Console.log "Tests passed"
    exit' 0

main :: Effect Unit
main = do
  duplex <- newPassThrough
  runAff_ completion do
    let expected = 100_000
    -- One newline per chunk, so the number of lines is observable.
    b <- liftEffect $ Buffer.fromString "aaaaaaaaaa\n" UTF8
    -- Read in paused mode *while* writing: the writer reports backpressure
    -- and waits for `drain`, which these reads produce. (A flowing reader
    -- never advances the buffered bytes, so it could not drain the writer.)
    writer <- forkAff do
      write duplex $ Array.replicate expected b
      end duplex
    bufs <- readAll duplex
    joinFiber writer
    all <- liftEffect $ Buffer.concat bufs
    str <- liftEffect $ Buffer.toString UTF8 all
    let actual = Array.length (String.split (Pattern "\n") str) - 1
    unless (actual == expected) do
      throwError $ error $ "Expected " <> show expected <> " lines, but got " <> show actual <> " lines."
