module Test.Main where

import Prelude

import Data.Array as Array
import Data.Either (Either(..))
import Data.Maybe (Maybe(..), fromJust, isJust, isNothing)
import Data.String (joinWith)
import Effect (Effect)
import Effect.Console (log)
import Effect.Exception (error)
import Effect.Ref as Ref
import Node.Buffer as Buffer
import Node.Encoding (Encoding(..))
import Node.EventEmitter (on_)
import Node.Stream (Duplex, dataH, dataHStr, destroy', drainH, end, end', errorH, newPassThrough, pipe, read, read', readEither, readEither', readString, readableH, setDefaultEncoding, setEncoding, unpipe, writeString, writeString')
import Partial.Unsafe (unsafePartial)
import Test.Assert (assert, assert')

assertEqual :: forall a. Show a => Eq a => a -> a -> Effect Unit
assertEqual x y =
  assert' (show x <> " did not equal " <> show y) (x == y)

main :: Effect Unit
main = do
  log "setDefaultEncoding should not affect writing"
  testSetDefaultEncoding

  log "setEncoding should not affect reading"
  testSetEncoding

  log "test pipe"
  testPipe

  log "test write"
  testWrite

  log "test end"
  testEnd

  log "test manual reads"
  testReads

  log "test partial reads and encodings"
  testPartialReads

  log "test backpressure"
  testBackpressure

  log "test unpipe"
  testUnpipe

  log "Tests passed"

testString :: String
testString = "üöß💡"

-- | Every handler that carries an assertion must run; each check counts the
-- | callbacks it observed and asserts the count afterwards.
testSetDefaultEncoding :: Effect Unit
testSetDefaultEncoding = do
  w1 <- newPassThrough
  check w1

  w2 <- newPassThrough
  setDefaultEncoding w2 UCS2
  check w2

  where
  check w = do
    seen <- Ref.new 0
    w # on_ dataH \buf -> do
      str <- Buffer.toString UTF8 buf
      assertEqual testString str
      Ref.modify_ (_ + 1) seen
    void $ writeString w UTF8 testString
    count <- Ref.read seen
    assertEqual 1 count

-- | The upstream version wrote twice into `r1`, never into `r2`, and nested
-- | the second assertion in a handler that could not fire. Both readers are
-- | exercised here, with their handlers attached before the writes.
testSetEncoding :: Effect Unit
testSetEncoding = do
  check UTF8
  check UTF16LE
  check UCS2
  where
  check enc = do
    r1 <- newPassThrough
    r1Seen <- Ref.new 0
    r1 # on_ dataH \buf -> do
      text <- Buffer.toString enc buf
      assert' ("r1 buffer (" <> show enc <> "): " <> show text) (text == testString)
      Ref.modify_ (_ + 1) r1Seen
    void $ writeString r1 enc testString

    r2 <- newPassThrough
    setEncoding r2 enc
    r2Seen <- Ref.new 0
    r2 # on_ dataHStr \str -> do
      assert' ("r2 string (" <> show enc <> "): " <> show str) (str == testString)
      Ref.modify_ (_ + 1) r2Seen
    void $ writeString r2 enc testString

    count1 <- Ref.read r1Seen
    count2 <- Ref.read r2Seen
    assert' ("r1 callback count (" <> show enc <> ")") (count1 == 1)
    assert' ("r2 callback count (" <> show enc <> ")") (count2 == 1)

-- | Round trip through gzip and gunzip. The assertions are attached before
-- | the writes, the write and end callbacks are mandatory, and the compressed
-- | bytes are checked against the gzip magic independently of the round trip.
testPipe :: Effect Unit
testPipe = do
  sIn <- newPassThrough
  sOut <- newPassThrough
  zip <- createGzip
  unzip <- createGunzip

  log "pipe 1"
  _ <- sIn `pipe` zip
  log "pipe 2"
  _ <- zip `pipe` unzip
  log "pipe 3"
  _ <- unzip `pipe` sOut

  received <- Ref.new ""
  sOut # on_ dataH \buf -> do
    str <- Buffer.toString UTF8 buf
    Ref.modify_ (_ <> str) received

  compressed <- Ref.new []
  zip # on_ dataH \buf -> do
    bytes <- Buffer.toArray buf
    Ref.modify_ (_ <> bytes) compressed

  writeDone <- Ref.new false
  endDone <- Ref.new false
  void $ writeString' sIn UTF8 testString \_ -> do
    Ref.write true writeDone
    end' sIn \_ -> do
      Ref.write true endDone

  wrote <- Ref.read writeDone
  ended <- Ref.read endDone
  assert' "write callback must run" wrote
  assert' "end callback must run" ended

  got <- Ref.read received
  assertEqual testString got

  gzipBytes <- Ref.read compressed
  assertEqual [ 0x1F, 0x8B ] (Array.take 2 gzipBytes)
  assert' "gzip output must not be empty" (Array.length gzipBytes > 2)

testWrite :: Effect Unit
testWrite = do
  hasError
  noError
  where
  hasError = do
    w1 <- newPassThrough
    w1 # on_ errorH (const $ pure unit)
    end w1
    called <- Ref.new false
    void $ writeString' w1 UTF8 "msg" \err -> do
      assert' "writeString - should have error" $ isJust err
      Ref.write true called
    ran <- Ref.read called
    assert' "writeString - callback must run" ran

  noError = do
    w1 <- newPassThrough
    called <- Ref.new false
    void $ writeString' w1 UTF8 "msg1" \err -> do
      assert' "writeString - should have no error" $ isNothing err
      Ref.write true called
    ran <- Ref.read called
    assert' "writeString - callback must run" ran
    end w1

testEnd :: Effect Unit
testEnd = do
  hasError
  noError
  where
  hasError = do
    w1 <- newPassThrough
    w1 # on_ errorH (const $ pure unit)
    called <- Ref.new false
    void $ writeString' w1 UTF8 "msg" \_ -> do
      _ <- destroy' w1 $ error "Problem"
      end' w1 \err -> do
        assert' "end - should have error" $ isJust err
        Ref.write true called
    ran <- Ref.read called
    assert' "end - callback must run after destroy" ran

  noError = do
    w1 <- newPassThrough
    called <- Ref.new false
    end' w1 \err -> do
      assert' "end - should have no error" $ isNothing err
      Ref.write true called
    ran <- Ref.read called
    assert' "end - callback must run" ran

testReads :: Effect Unit
testReads = do
  testReadString
  testReadBuf

  where
  testReadString = do
    sIn <- newPassThrough
    v <- readString sIn UTF8
    assert (isNothing v)

    seen <- Ref.new false
    sIn # on_ readableH do
      str <- readString sIn UTF8
      assert (isJust str)
      assertEqual (unsafePartial (fromJust str)) testString
      Ref.write true seen

    void $ writeString sIn UTF8 testString
    ran <- Ref.read seen
    assert' "readable handler must run for the string reader" ran

  testReadBuf = do
    sIn <- newPassThrough
    v <- read sIn
    assert (isNothing v)

    seen <- Ref.new false
    sIn # on_ readableH do
      buf <- read sIn
      assert (isJust buf)
      _ <- assertEqual <$> (Buffer.toString UTF8 (unsafePartial (fromJust buf))) <*> pure testString
      Ref.write true seen

    void $ writeString sIn UTF8 testString
    ran <- Ref.read seen
    assert' "readable handler must run for the buffer reader" ran

-- | Partial reads, multiple chunks, and the `readEither` variants with and
-- | without a stream encoding.
testPartialReads :: Effect Unit
testPartialReads = do
  -- `read'` takes at most the requested size; `read` drains the rest.
  s <- newPassThrough
  void $ writeString s UTF8 testString
  first <- read' s 2
  assert (isJust first)
  second <- read s
  assert (isJust second)
  str <- Buffer.concat [ unsafePartial (fromJust first), unsafePartial (fromJust second) ] >>= Buffer.toString UTF8
  assertEqual testString str
  empty <- read s
  assert (isNothing empty)

  -- Multiple writes produce one chunk each.
  m <- newPassThrough
  chunks <- Ref.new []
  m # on_ dataH \buf -> do
    chunk <- Buffer.toString UTF8 buf
    Ref.modify_ (_ <> [ chunk ]) chunks
  void $ writeString m UTF8 "a"
  void $ writeString m UTF8 "b"
  got <- Ref.read chunks
  assertEqual [ "a", "b" ] got

  -- With an encoding, `readEither` returns the decoded String branch.
  r <- newPassThrough
  setEncoding r UTF8
  void $ writeString r UTF8 testString
  decoded <- readEither r
  case decoded of
    Just (Left text) -> assertEqual testString text
    Just (Right _) -> assert' "readEither - expected the String branch" false
    Nothing -> assert' "readEither - expected a chunk" false

  -- Without an encoding, the sized variant returns the Buffer branch.
  r2 <- newPassThrough
  void $ writeString r2 UTF8 testString
  partial <- readEither' r2 2
  case partial of
    Just (Right buffer) -> do
      size <- Buffer.size buffer
      assertEqual 2 size
    Just (Left _) -> assert' "readEither' - expected the Buffer branch" false
    Nothing -> assert' "readEither' - expected a chunk" false

-- | Writes past the high water mark report backpressure; a read that drops
-- | the buffer below it emits `drain`.
testBackpressure :: Effect Unit
testBackpressure = do
  w <- newPassThrough
  backpressure <- writeString w UTF8 (joinWith "" (Array.replicate 70_000 "x"))
  assert' "write over the high water mark reports backpressure" (backpressure == false)
  drained <- Ref.new false
  w # on_ drainH (Ref.write true drained)
  first <- read w
  assert (isJust first)
  got <- Ref.read drained
  assert' "drain fires when the buffer drops below the high water mark" got
  rest <- read w
  assert (isJust rest)

-- | `unpipe` stops forwarding data to the destination.
testUnpipe :: Effect Unit
testUnpipe = do
  source <- newPassThrough
  destination <- newPassThrough
  _ <- source `pipe` destination
  received <- Ref.new ""
  destination # on_ dataH \buf -> do
    str <- Buffer.toString UTF8 buf
    Ref.modify_ (_ <> str) received
  void $ writeString source UTF8 "before"
  unpipe source destination
  void $ writeString source UTF8 "after"
  got <- Ref.read received
  assertEqual "before" got

foreign import createGzip :: Effect Duplex

foreign import createGunzip :: Effect Duplex
