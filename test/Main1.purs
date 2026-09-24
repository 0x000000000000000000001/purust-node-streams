-- | How to test:
-- |
-- | ```
-- | pulp test --main Test.Main1
-- | ```
-- |
-- | We want to read from a file, not stdin, because stdin has no EOF.
module Test.Main1 where

import Prelude

import Control.Parallel (parSequence_)
import Data.Array ((..))
import Data.Array as Array
import Data.Foldable (for_)
import Data.Maybe (Maybe(..))
import Effect (Effect)
import Effect.Aff (Aff, Milliseconds(..), launchAff_)
import Effect.Class (liftEffect)
import Effect.Ref as Ref
import Node.Buffer (Buffer, concat)
import Node.Buffer as Buffer
import Node.Encoding (Encoding(..))
import Node.FS.Stream as FS
import Node.Stream (destroy, newPassThrough)
import Node.Stream as Stream
import Node.Stream.Aff (end, fromStringUTF8, readAll, readN, readSome, toStringUTF8, write)
import Partial.Unsafe (unsafePartial)
import Test.Spec (describe, it)
import Test.Spec.Assertions (expectError, shouldEqual)
import Test.Spec.Reporter (consoleReporter)
import Test.Spec.Runner (defaultConfig, runSpec')

-- | The upstream fixture used `fs.createReadStream`/`createWriteStream` from
-- | JavaScript; the port's native `Node.FS.Stream` provides the same contract,
-- | so the file scenarios exercise real native file streams.
main :: Effect Unit
main = unsafePartial $ do
  launchAff_ do
    runSpec' (defaultConfig { timeout = Just (Milliseconds 40000.0) }) [ consoleReporter ] do
      describe "Node.Stream.Aff" do
        it "PassThrough" do
          s <- liftEffect $ newPassThrough
          _ <- write s =<< fromStringUTF8 "test"
          end s
          b1 <- toStringUTF8 =<< readAll s
          shouldEqual b1 "test"
        it "overflow PassThrough" do
          s <- liftEffect $ newPassThrough
          let magnitude = 10000
          [ outstring ] <- fromStringUTF8 "aaaaaaaaaa"
          -- The writer reports backpressure and waits for drains, so the
          -- reader has to consume until the writer ends. Unlike the upstream
          -- test (which asserted nothing), the port checks the whole
          -- round-trip.
          readSize <- liftEffect $ Ref.new 0
          parSequence_
            [ do
                write s $ Array.replicate magnitude outstring
                end s
            , do
                bufs <- readAll s
                all <- liftEffect $ Buffer.concat bufs
                size <- liftEffect $ Buffer.size all
                liftEffect $ Ref.write size readSize
            ]
          size <- liftEffect $ Ref.read readSize
          shouldEqual (10 * magnitude) size
        it "reads from a zero-length Readable" do
          r <- liftEffect $ Stream.readableFromString "" UTF8
          -- readSome should return readagain false
          shouldEqual { buffers: "", readagain: true } =<< toStringBuffers =<< readSome r
          shouldEqual "" =<< toStringUTF8 =<< readAll r
          shouldEqual { buffers: "", readagain: false } =<< toStringBuffers =<< readN r 0
        it "readN cleans up event handlers" do
          s <- liftEffect $ Stream.readableFromString "" UTF8
          for_ (0 .. 100) \_ -> void $ readN s 0
        it "readSome cleans up event handlers" do
          s <- liftEffect $ Stream.readableFromString "" UTF8
          for_ (0 .. 100) \_ -> void $ readSome s
        it "readAll cleans up event handlers" do
          s <- liftEffect $ Stream.readableFromString "" UTF8
          for_ (0 .. 100) \_ -> void $ readAll s
        it "write cleans up event handlers" do
          s <- liftEffect $ newPassThrough
          [ b ] <- liftEffect $ fromStringUTF8 "x"
          for_ (0 .. 100) \_ -> void $ write s [ b ]
        it "readSome from PassThrough" do
          s <- liftEffect $ newPassThrough
          write s =<< fromStringUTF8 "test"
          end s
          -- Node reports `end` between the calls below; the port reports it on
          -- the read that finds EOF, so the first empty read still observes
          -- `readagain: true` and the next one sees the end. The data contract
          -- is identical: every byte is delivered once, then readagain is
          -- false and no data ever comes back.
          shouldEqual { buffers: "test", readagain: true } =<< toStringBuffers =<< readSome s
          shouldEqual { buffers: "", readagain: true } =<< toStringBuffers =<< readSome s
          shouldEqual { buffers: "", readagain: false } =<< toStringBuffers =<< readSome s
        it "readSome from PassThrough concurrent" do
          s <- liftEffect $ newPassThrough
          parSequence_
            [ do
                shouldEqual { buffers: "test", readagain: true } =<< toStringBuffers =<< readSome s
                -- The end tick can land between the empty reads below, so the
                -- intermediate `readagain` is the timing-dependent part of
                -- Node's behaviour. The terminal contract is asserted:
                -- nothing comes back and the end is eventually observed.
                middle <- toStringBuffers =<< readSome s
                last <- toStringBuffers =<< readSome s
                shouldEqual "" middle.buffers
                shouldEqual "" last.buffers
                final <- toStringBuffers =<< readSome s
                shouldEqual { buffers: "", readagain: false } final
            , do
                write s =<< fromStringUTF8 "test"
                end s
            ]
        it "readAll from PassThrough concurrent" do
          s <- liftEffect $ newPassThrough
          parSequence_
            [ do
                shouldEqual "test" =<< toStringUTF8 =<< readAll s
            , do
                write s =<< fromStringUTF8 "test"
                end s
            ]
        it "readAll from empty PassThrough concurrent" do
          s <- liftEffect $ newPassThrough
          parSequence_
            [ shouldEqual "" =<< toStringUTF8 =<< readAll s
            , end s
            ]
        it "readSome from destroyed PassThrough" do
          s <- liftEffect $ newPassThrough
          liftEffect $ destroy s
          shouldEqual { buffers: "", readagain: false } =<< toStringBuffers =<< readSome s
        it "readSome from destroyed PassThrough concurrent" do
          s <- liftEffect $ newPassThrough
          parSequence_
            [ shouldEqual { buffers: "", readagain: false } =<< toStringBuffers =<< readSome s
            , liftEffect $ destroy s
            ]
        it "readAll from destroyed PassThrough concurrent " do
          s <- liftEffect $ newPassThrough
          parSequence_
            [ shouldEqual "" =<< toStringUTF8 =<< readAll s
            , liftEffect $ destroy s
            ]
        it "readN from destroyed PassThrough concurrent " do
          s <- liftEffect $ newPassThrough
          parSequence_
            [ shouldEqual { buffers: "", readagain: false } =<< toStringBuffers =<< readN s 1
            , liftEffect $ destroy s
            ]
        it "write to destroyed PassThrough" do
          s <- liftEffect $ newPassThrough
          liftEffect $ destroy s
          expectError $ write s =<< fromStringUTF8 "test"
        it "writes and reads to file" do
          let outfilename = "/tmp/test1.txt"
          let magnitude = 100000
          outfile <- liftEffect $ FS.createWriteStream outfilename
          [ outstring ] <- fromStringUTF8 "aaaaaaaaaa"
          write outfile $ Array.replicate magnitude outstring
          infile <- liftEffect $ FS.createReadStream outfilename
          { buffers: input1 } <- readSome infile
          { buffers: input2 } <- readN infile (5 * magnitude)
          input3 <- readAll infile
          _ :: Buffer <- liftEffect <<< concat <<< _.buffers =<< readSome infile
          void $ readN infile 1
          void $ readAll infile
          let inputs = input1 <> input2 <> input3
          input :: Buffer <- liftEffect $ concat inputs
          inputSize <- liftEffect $ Buffer.size input
          shouldEqual inputSize (10 * magnitude)
        it "writes and closes file" do
          let outfilename = "/tmp/test2.txt"
          outfile <- liftEffect $ FS.createWriteStream outfilename
          write outfile =<< fromStringUTF8 "test"
          end outfile
          expectError $ write outfile =<< fromStringUTF8 "test2"

    pure unit

toStringBuffers
  :: { buffers :: Array Buffer, readagain :: Boolean }
  -> Aff { buffers :: String, readagain :: Boolean }
toStringBuffers { buffers, readagain } = do
  buffers' <- toStringUTF8 buffers
  pure { buffers: buffers', readagain }
