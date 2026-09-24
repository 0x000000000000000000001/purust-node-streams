// Native Node streams. A stream *is* an event emitter (like in Node); its
// readable/writable state lives in the emitter's user data. `pipe` copies data
// in a deferred microtask job so handlers registered right after the call run.
use std::rc::Rc;
use std::sync::Mutex;

use Purs_Node_Encoding::{purust_encoding_decode, purust_encoding_encode};
use Purs_Node_EventEmitter::{purust_emitter_emit, EventEmitter};

// Chunks are either native buffers or strings, exactly like Node.
pub type Chunk = crate::UnknownType;

// A stream *is* an event emitter; the state lives in its user data.
pub type Stream = Purs_Node_EventEmitter::EventEmitter;

struct StreamState {
    readable: bool,
    writable: bool,
    // Readable side
    source: Vec<u8>,
    cursor: usize,
    encoding: Option<String>,
    flowing: bool,
    /// Created from a complete in-memory buffer (fs.readFile-style readers).
    static_source: bool,
    /// Whether a producer pushed into this readable after it was created.
    pushed: bool,
    paused: bool,
    end_emitted: bool,
    /// `end` requested while the readable buffer still had unconsumed bytes.
    end_requested: bool,
    readable_high_water_mark: i64,
    // Writable side
    destination: Option<String>,
    write_fd: Option<i32>,
    sink: Vec<u8>,
    corked: bool,
    corked_bytes: Vec<u8>,
    default_encoding: Option<String>,
    finished: bool,
    finish_emitted: bool,
    destroyed: bool,
    closed: bool,
    allow_half_open: bool,
    error: Option<crate::UnknownType>,
    /// Native integrations (sockets, child processes) attach their own state.
    extension: Option<crate::UnknownType>,
    /// Called when the writable side ends, so integrations can shut down fds.
    end_hook: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
    /// When set, writes are handed to this hook instead of the fd/file sink
    /// (HTTP layers flush their head before the first body write).
    write_hook: Option<std::sync::Arc<dyn Fn(&[u8]) + Send + Sync>>,
    /// Registered `pipe` destinations, so `unpipe` can remove their listeners.
    pipes: Vec<PipeRegistration>,
}

struct PipeRegistration {
    destination: usize,
    data: crate::UnknownType,
    end: crate::UnknownType,
    error: crate::UnknownType,
}

fn state_new(readable: bool, writable: bool) -> Rc<Mutex<StreamState>> {
    Rc::new(Mutex::new(StreamState {
        readable,
        writable,
        source: Vec::new(),
        cursor: 0,
        encoding: None,
        flowing: false,
        static_source: false,
        pushed: false,
        paused: false,
        end_emitted: false,
        end_requested: false,
        readable_high_water_mark: 65536,
        destination: None,
        write_fd: None,
        sink: Vec::new(),
        corked: false,
        corked_bytes: Vec::new(),
        default_encoding: None,
        finished: false,
        finish_emitted: false,
        destroyed: false,
        closed: false,
        allow_half_open: false,
        error: None,
        extension: None,
        end_hook: None,
        write_hook: None,
        pipes: Vec::new(),
    }))
}

fn stream_new(readable: bool, writable: bool) -> Rc<EventEmitter> {
    let emitter = Rc::new(EventEmitter::new_native());
    emitter.set_user_data(crate::Value::Class(Rc::new(state_new(readable, writable))));
    emitter
}

fn state_of(stream: &Rc<EventEmitter>) -> Rc<Mutex<StreamState>> {
    stream
        .user_data()
        .expect("Node.Stream: stream without native state")
        .unwrap_class::<Rc<Mutex<StreamState>>>()
        .clone()
}

fn unbox_stream(value: &crate::UnknownType) -> Rc<EventEmitter> {
    value.unwrap_class::<Rc<EventEmitter>>().clone()
}

pub fn purust_stream_box(stream: Rc<EventEmitter>) -> crate::UnknownType {
    crate::Value::Class(Rc::new(stream))
}

/// A duplex stream: readable and writable.
pub fn purust_stream_duplex() -> Rc<EventEmitter> {
    stream_new(true, true)
}

/// A stream with the requested directions.
pub fn purust_stream_new_stream(readable: bool, writable: bool) -> Rc<EventEmitter> {
    stream_new(readable, writable)
}

/// Drops the bytes accumulated so far on a readable stream (used after they
/// have been flushed to a descriptor).
pub fn purust_stream_clear(stream: &Rc<EventEmitter>) {
    let state = state_of(stream);
    let mut state = state.lock().unwrap();
    state.source.clear();
    state.cursor = 0;
}

/// Atomically drains the unconsumed readable bytes. Native integrations whose
/// listeners attach after the first bytes arrived replay them through this,
/// like Node resumes a paused stream.
pub fn purust_stream_take(stream: &Rc<EventEmitter>) -> Vec<u8> {
    let state = state_of(stream);
    let mut state = state.lock().unwrap();
    let consumed = state.cursor.min(state.source.len());
    let bytes = state.source.split_off(consumed);
    state.source.clear();
    state.cursor = 0;
    bytes
}

pub fn purust_stream_set_extension(stream: &Rc<EventEmitter>, value: crate::UnknownType) {
    state_of(stream).lock().unwrap().extension = Some(value);
}

pub fn purust_stream_extension(stream: &Rc<EventEmitter>) -> Option<crate::UnknownType> {
    state_of(stream).lock().unwrap().extension.clone()
}

pub fn purust_stream_set_write_hook(
    stream: &Rc<EventEmitter>,
    hook: std::sync::Arc<dyn Fn(&[u8]) + Send + Sync>,
) {
    state_of(stream).lock().unwrap().write_hook = Some(hook);
}

pub fn purust_stream_set_end_hook(
    stream: &Rc<EventEmitter>,
    hook: std::sync::Arc<dyn Fn() + Send + Sync>,
) {
    state_of(stream).lock().unwrap().end_hook = Some(hook);
}

/// All bytes accumulated on a readable stream so far.
pub fn purust_stream_bytes(stream: &Rc<EventEmitter>) -> Vec<u8> {
    state_of(stream).lock().unwrap().source.clone()
}

/// Writes from this stream reach a raw file descriptor (child process stdin).
pub fn purust_stream_set_write_fd(stream: &Rc<EventEmitter>, fd: i32) {
    state_of(stream).lock().unwrap().write_fd = Some(fd);
}

/// Appends data to a readable stream and notifies `data` listeners. After
/// `setEncoding`, Node delivers decoded strings to `data` listeners, so the
/// emitted chunk follows the stream encoding.
pub fn purust_stream_push(stream: &Rc<EventEmitter>, bytes: Vec<u8>) {
    let has_data = Purs_Node_EventEmitter::purust_emitter_listener_count(stream, "data") > 0;
    let has_readable = Purs_Node_EventEmitter::purust_emitter_listener_count(stream, "readable") > 0;
    let chunk = if has_data {
        let state = state_of(stream);
        let state = state.lock().unwrap();
        match state.encoding.clone() {
            Some(encoding) => {
                let name = Purs_Node_Encoding::purust_encoding_from_name(&encoding);
                Some(crate::Value::String(purust_encoding_decode(name, &bytes)))
            }
            None => Some(crate::Value::Class(Rc::new(
                Purs_Node_Buffer_Immutable::purust_buffer_from_bytes(bytes.clone()),
            ))),
        }
    } else {
        None
    };
    {
        let state = state_of(stream);
        let mut state = state.lock().unwrap();
        state.source.extend_from_slice(&bytes);
        state.pushed = true;
    }
    if let Some(chunk) = chunk {
        purust_emitter_emit(stream, "data", vec![chunk]);
    }
    if has_readable {
        purust_emitter_emit(stream, "readable", Vec::new());
    }
}

/// Signals the end of a readable stream (child process stdout/stderr EOF).
/// Like Node, the `end` event waits until the buffered data has been consumed:
/// it fires immediately when nothing is pending, or when a flowing consumer
/// (`data` listeners) received everything as it was pushed.
pub fn purust_stream_end(stream: &Rc<EventEmitter>) {
    let has_data = Purs_Node_EventEmitter::purust_emitter_listener_count(stream, "data") > 0;
    let (immediate, deferred) = {
        let state = state_of(stream);
        let mut state = state.lock().unwrap();
        if state.end_emitted {
            (false, false)
        } else {
            state.end_requested = true;
            if has_data || state.static_source {
                // A flowing consumer already received every pushed chunk.
                state.end_emitted = true;
                (true, false)
            } else {
                // Node reports the end on the next tick: a paused reader can
                // still drain the buffer before the events arrive.
                (false, true)
            }
        }
    };
    if immediate {
        purust_emitter_emit(stream, "end", Vec::new());
    } else if deferred {
        schedule_end(stream);
    }
}

pub fn purust_readable_from_bytes(bytes: Vec<u8>) -> Rc<EventEmitter> {
    let stream = stream_new(true, false);
    {
        let state = state_of(&stream);
        let mut state = state.lock().unwrap();
        state.source = bytes;
        state.static_source = true;
        // A complete in-memory source has no producer: `end` is pending until
        // the buffered bytes have been consumed (a read at EOF then reports
        // `readable` and `end`, like Node).
        state.end_requested = true;
    }
    stream
}

pub fn purust_readable_from_file(path: &str) -> Rc<EventEmitter> {
    purust_readable_from_bytes(std::fs::read(path).unwrap_or_default())
}

pub fn purust_readable_from_string(text: String) -> Rc<EventEmitter> {
    purust_readable_from_bytes(purust_core::purust_string_to_utf8_lossy(&text).into_bytes())
}

pub fn purust_writable_to_file(path: &str) -> Rc<EventEmitter> {
    let stream = stream_new(false, true);
    {
        let state = state_of(&stream);
        let mut state = state.lock().unwrap();
        state.destination = Some(path.to_owned());
        // Truncate/create like `fs.createWriteStream` does.
        let _ = std::fs::write(path, []);
    }
    stream
}

pub fn purust_writable_new() -> Rc<EventEmitter> {
    stream_new(false, true)
}

pub fn purust_writable_bytes(stream: &Rc<EventEmitter>) -> Vec<u8> {
    state_of(stream).lock().unwrap().sink.clone()
}

/// A write hook that must run *outside* the stream-state lock: integrations
/// (HTTP outgoing messages) resolve their own state through the stream
/// extension, which re-locks the same mutex.
type DeferredFlush = Option<(std::sync::Arc<dyn Fn(&[u8]) + Send + Sync>, Vec<u8>)>;

fn run_deferred_flush(deferred: DeferredFlush) {
    if let Some((hook, bytes)) = deferred {
        hook(&bytes);
    }
}

fn flush(state: &mut StreamState, bytes: &[u8]) -> DeferredFlush {
    if let Some(hook) = state.write_hook.clone() {
        return Some((hook, bytes.to_vec()));
    }
    state.sink.extend_from_slice(bytes);
    if let Some(fd) = state.write_fd {
        let mut written = 0usize;
        while written < bytes.len() {
            let count = unsafe {
                libc::write(
                    fd,
                    bytes[written..].as_ptr() as *const libc::c_void,
                    bytes.len() - written,
                )
            };
            if count <= 0 {
                break;
            }
            written += count as usize;
        }
    }
    if let Some(path) = state.destination.clone() {
        use std::io::Write;
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            let _ = file.write_all(bytes);
        }
    }
    None
}

fn chunk_bytes(chunk: &Chunk) -> Vec<u8> {
    match chunk.resolve() {
        crate::Value::String(text) => purust_core::purust_string_to_utf8_lossy(text).into_bytes(),
        _ => chunk
            .unwrap_class::<Rc<Purs_Node_Buffer_Immutable::ImmutableBuffer>>()
            .bytes(),
    }
}

/// Emits `readable` then `end` at the next checkpoint, once: Node reports the
/// end of a readable on the next tick, which leaves room for a reader to drain
/// what is left between `end` and the events.
fn schedule_end(stream: &Rc<EventEmitter>) {
    let stream = stream.clone();
    defer(move || {
        purust_emitter_emit(&stream, "readable", Vec::new());
        let ended = {
            let state = state_of(&stream);
            let mut state = state.lock().unwrap();
            if state.end_emitted {
                false
            } else {
                state.end_emitted = true;
                true
            }
        };
        if ended {
            purust_emitter_emit(&stream, "end", Vec::new());
        }
    });
}

fn read_chunk(stream: &Rc<EventEmitter>, limit: Option<usize>) -> Option<Chunk> {
    let state = state_of(stream);
    let mut eof_pending = false;
    let (chunk, drained, end_pending) = {
        let mut state = state.lock().unwrap();
        if state.cursor >= state.source.len() {
            // A read at the end of a complete stream reports the end; Node
            // emits `readable` at EOF, before `end`.
            eof_pending = state.end_requested && !state.end_emitted;
            (None, false, false)
        } else {
            let before = state.source.len() - state.cursor;
            let remaining = state.source.len() - state.cursor;
            let size = limit
                .unwrap_or(state.readable_high_water_mark.max(1) as usize)
                .min(remaining)
                .max(1);
            let bytes = state.source[state.cursor..state.cursor + size].to_vec();
            state.cursor += size;
            let after = state.source.len() - state.cursor;
            let high_water_mark = state.readable_high_water_mark.max(1) as usize;
            let chunk = match state.encoding.clone() {
                Some(encoding) => {
                    let name = Purs_Node_Encoding::purust_encoding_from_name(&encoding);
                    crate::Value::String(purust_encoding_decode(name, &bytes))
                }
                None => crate::Value::Class(Rc::new(
                    Purs_Node_Buffer_Immutable::purust_buffer_from_bytes(bytes),
                )),
            };
            // Reading below the high water mark makes the writable side drain.
            let drained = before > high_water_mark && after <= high_water_mark;
            // `end` is reported by the read that finds EOF (Node emits `end`
            // when a read returns null), not by the read that consumes the
            // last chunk.
            (Some(chunk), drained, false)
        }
    };
    if drained {
        purust_emitter_emit(stream, "drain", Vec::new());
    }
    if end_pending || eof_pending {
        schedule_end(stream);
    }
    chunk
}

fn finish_writable(stream: &Rc<EventEmitter>) {
    let (should_emit, deferred) = {
        let state = state_of(stream);
        let mut state = state.lock().unwrap();
        if state.finished {
            (false, None)
        } else {
            let pending = std::mem::take(&mut state.corked_bytes);
            let deferred = flush(&mut state, &pending);
            state.finished = true;
            (!state.finish_emitted, deferred)
        }
    };
    run_deferred_flush(deferred);
    if should_emit {
        let hook = state_of(stream).lock().unwrap().end_hook.clone();
        state_of(stream).lock().unwrap().finish_emitted = true;
        purust_emitter_emit(stream, "finish", Vec::new());
        if let Some(hook) = hook {
            hook();
        }
    }
}

fn defer(job: impl FnOnce() + 'static) {
    purust_core::microtasks::current().enqueue(job);
}

/// Copies the remaining readable bytes into the destination, then ends it and
/// emits `end` on the source, like `Readable.pipe`.
fn stream_end_emitted(stream: &Rc<EventEmitter>) -> bool {
    state_of(stream).lock().unwrap().end_emitted
}

/// A complete buffered source (`readableFromBuf`) has no producer that could
/// end it later, so `pipe` replays its `end` once the buffer is drained.
fn stream_is_complete_buffer(stream: &Rc<EventEmitter>) -> bool {
    let state = state_of(stream);
    let state = state.lock().unwrap();
    state.static_source && !state.pushed
}

pub fn purust_stream_pipe(source: Rc<EventEmitter>, destination: Rc<EventEmitter>) {
    // Flowing pipe: every chunk written or pushed on the source is forwarded
    // to the destination, and the destination ends with the source.
    let destination_for_data = destination.clone();
    let data_callback = crate::Value::Func1(purust_core::Func1::Shared(Rc::new(move |chunk| {
        let _ = write_value(&destination_for_data, chunk_bytes(&chunk));
        purust_emitter_emit(&destination_for_data, "drain", Vec::new());
        crate::Value::Unit
    })));
    Purs_Node_EventEmitter::purust_emitter_on_native(&source, "data", data_callback.clone());
    let destination_for_end = destination.clone();
    let end_callback = crate::Value::Func1(purust_core::Func1::Shared(Rc::new(move |_| {
        finish_writable(&destination_for_end);
        crate::Value::Unit
    })));
    Purs_Node_EventEmitter::purust_emitter_on_native(&source, "end", end_callback.clone());
    let destination_for_error = destination.clone();
    let error_callback = crate::Value::Func1(purust_core::Func1::Shared(Rc::new(move |error| {
        purust_emitter_emit(&destination_for_error, "error", vec![error]);
        crate::Value::Unit
    })));
    Purs_Node_EventEmitter::purust_emitter_on_native(&source, "error", error_callback.clone());
    // `unpipe` removes exactly these listeners.
    {
        let state = state_of(&source);
        let mut state = state.lock().unwrap();
        state.pipes.push(PipeRegistration {
            destination: Rc::as_ptr(&destination) as usize,
            data: data_callback,
            end: end_callback,
            error: error_callback,
        });
    }

    // Anything the source buffered before the pipe started flows in a
    // deferred job, so listeners registered right after `pipe` still see it.
    defer(move || {
        while let Some(chunk) = read_chunk(&source, None) {
            purust_emitter_emit(&source, "data", vec![chunk]);
        }
        // A source that already ended - or a complete in-memory buffer that
        // was just drained - still notifies late listeners.
        if stream_end_emitted(&source) || stream_is_complete_buffer(&source) {
            finish_writable(&destination);
            purust_emitter_emit(&source, "end", Vec::new());
        }
    });
}

pub fn Node_Stream_setEncodingImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|readable, encoding| {
        let readable = unbox_stream(&readable);
        state_of(&readable).lock().unwrap().encoding = Some(encoding.unwrap_string());
        crate::Value::Unit
    })))
}

/// `readImpl`/`readSizeImpl` wrap the chunk in a `Class` for the generated
/// `Maybe Chunk` parser, while `dataH`/`dataHStr` hand the raw value over.
/// Unwrap the carrier before classifying so both paths agree.
fn unwrap_chunk_carrier(chunk: crate::UnknownType) -> crate::UnknownType {
    if let crate::Value::Class(payload) = chunk.resolve() {
        if let Some(inner) = payload.downcast_ref::<Rc<crate::UnknownType>>() {
            return inner.as_ref().clone();
        }
    }
    chunk
}

pub fn Node_Stream_readChunkImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(
        |use_buffer, use_string, chunk| {
            let chunk = unwrap_chunk_carrier(chunk);
            if let crate::Value::String(_) = chunk.resolve() {
                use_string.unwrap_func1()(chunk)
            } else {
                use_buffer.unwrap_func1()(chunk)
            }
        },
    )))
}

pub fn Node_Stream_readableImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        // Node: `readable` is false once `end` has been emitted or the stream
        // was destroyed, even though the buffered bytes remain readable.
        let (readable, ended, destroyed) = {
            let state = state_of(&stream);
            let state = state.lock().unwrap();
            (state.readable, state.end_emitted, state.destroyed)
        };
        crate::mk_bool(readable && !ended && !destroyed)
    })))
}

pub fn Node_Stream_readableEndedImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        // Node: `readableEnded` is true once the `end` event has been emitted.
        let (readable, ended) = {
            let state = state_of(&stream);
            let state = state.lock().unwrap();
            (state.readable, state.end_emitted)
        };
        crate::mk_bool(readable && ended)
    })))
}

pub fn Node_Stream_readableFlowingImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let flowing = state_of(&stream).lock().unwrap().flowing;
        crate::mk_bool(flowing)
    })))
}

pub fn Node_Stream_readableHighWaterMarkImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        // The PureScript declaration types this as Boolean.
        let has_room = state_of(&stream).lock().unwrap().readable_high_water_mark > 0;
        crate::mk_bool(has_room)
    })))
}

pub fn Node_Stream_readableLengthImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let remaining = {
            let state = state_of(&stream);
            let state = state.lock().unwrap();
            state.cursor < state.source.len()
        };
        crate::mk_bool(remaining)
    })))
}

pub fn Node_Stream_resumeImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        {
            let state = state_of(&stream);
            let mut state = state.lock().unwrap();
            state.flowing = true;
            state.paused = false;
        }
        let job_stream = stream.clone();
        defer(move || {
            while let Some(chunk) = read_chunk(&job_stream, None) {
                purust_emitter_emit(&job_stream, "data", vec![chunk]);
            }
            purust_emitter_emit(&job_stream, "end", Vec::new());
            state_of(&job_stream).lock().unwrap().end_emitted = true;
        });
        crate::Value::Unit
    })))
}

pub fn Node_Stream_pauseImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let state = state_of(&stream);
        let mut state = state.lock().unwrap();
        state.flowing = false;
        state.paused = true;
        crate::Value::Unit
    })))
}

pub fn Node_Stream_isPausedImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let paused = state_of(&stream).lock().unwrap().paused;
        crate::mk_bool(paused)
    })))
}

pub fn Node_Stream_pipeImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(
        |source, destination| {
            let source = unbox_stream(&source);
            let destination = unbox_stream(&destination);
            purust_stream_pipe(source, destination.clone());
            purust_stream_box(destination)
        },
    )))
}

pub fn Node_Stream_pipeCbImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(
        |source, destination, _options| {
            let source = unbox_stream(&source);
            let destination = unbox_stream(&destination);
            purust_stream_pipe(source, destination);
            crate::Value::Unit
        },
    )))
}

fn remove_pipe_listeners(source: &Rc<EventEmitter>, registration: &PipeRegistration) {
    let _ = Purs_Node_EventEmitter::purust_emitter_remove_native(source, "data", &registration.data);
    let _ = Purs_Node_EventEmitter::purust_emitter_remove_native(source, "end", &registration.end);
    let _ = Purs_Node_EventEmitter::purust_emitter_remove_native(source, "error", &registration.error);
}

pub fn Node_Stream_unpipeImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|source, destination| {
        let source = unbox_stream(&source);
        let destination = unbox_stream(&destination);
        let destination_ptr = Rc::as_ptr(&destination) as usize;
        let removed = {
            let state = state_of(&source);
            let mut state = state.lock().unwrap();
            let mut kept = Vec::new();
            let mut removed = Vec::new();
            for registration in state.pipes.drain(..) {
                if registration.destination == destination_ptr {
                    removed.push(registration);
                } else {
                    kept.push(registration);
                }
            }
            state.pipes = kept;
            removed
        };
        for registration in &removed {
            remove_pipe_listeners(&source, registration);
        }
        crate::Value::Unit
    })))
}

pub fn Node_Stream_unpipeAllImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|source| {
        let source = unbox_stream(&source);
        let removed = {
            let state = state_of(&source);
            let mut state = state.lock().unwrap();
            std::mem::take(&mut state.pipes)
        };
        for registration in &removed {
            remove_pipe_listeners(&source, registration);
        }
        crate::Value::Unit
    })))
}

pub fn Node_Stream_readImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        match read_chunk(&stream, None) {
            Some(chunk) => crate::Value::Class(Rc::new(
                // The generated `Maybe Chunk` parser unwraps the nullable
                // payload as `Rc<Chunk>`, so the chunk is class-wrapped.
                Purs_Data_Nullable::Data_Nullable_notNull(crate::Value::Class(Rc::new(Rc::new(
                    chunk,
                )))),
            )),
            None => crate::Value::Class(Rc::new(Purs_Data_Nullable::Data_Nullable_null())),
        }
    })))
}

/// `Nullable Buffer` readers: the payload is the native Buffer value, so no
/// `Chunk` classification is needed at the call site.
fn nullable_buffer(chunk: Option<crate::UnknownType>) -> crate::UnknownType {
    crate::Value::Class(Rc::new(match chunk {
        Some(chunk) => Purs_Data_Nullable::Data_Nullable_notNull(chunk),
        None => Purs_Data_Nullable::Data_Nullable_null(),
    }))
}

fn read_buffer(value: &crate::UnknownType, size: Option<usize>) -> crate::UnknownType {
    let stream = unbox_stream(value);
    match read_chunk(&stream, size) {
        Some(chunk) => match chunk.resolve() {
            crate::Value::String(_) => panic!("Stream encoding should not be set"),
            _ => nullable_buffer(Some(chunk)),
        },
        None => nullable_buffer(None),
    }
}

pub fn Node_Stream_readBufferImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        read_buffer(&value, None)
    })))
}

pub fn Node_Stream_readBufferSizeImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|value, size| {
        read_buffer(&value, Some(size.unwrap_int().max(1) as usize))
    })))
}

pub fn Node_Stream_readSizeImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|value, size| {
        let stream = unbox_stream(&value);
        let size = size.unwrap_int().max(1) as usize;
        match read_chunk(&stream, Some(size)) {
            Some(chunk) => crate::Value::Class(Rc::new(
                Purs_Data_Nullable::Data_Nullable_notNull(crate::Value::Class(Rc::new(Rc::new(
                    chunk,
                )))),
            )),
            None => crate::Value::Class(Rc::new(Purs_Data_Nullable::Data_Nullable_null())),
        }
    })))
}

/// Returns the error that rejected the write, like Node's write-after-end and
/// writes to a destroyed stream.
fn write_value(stream: &Rc<EventEmitter>, bytes: Vec<u8>) -> Option<crate::UnknownType> {
    {
        let state = state_of(stream);
        let state = state.lock().unwrap();
        if let Some(error) = &state.error {
            return Some(error.clone());
        }
        if state.destroyed {
            return Some(Purs_Effect_Exception::Effect_Exception_error(
                purust_core::purust_string_from_utf8("Cannot call write after a stream was destroyed"),
            ));
        }
        if state.finished {
            return Some(Purs_Effect_Exception::Effect_Exception_error(
                purust_core::purust_string_from_utf8("write after end"),
            ));
        }
    }
    let deferred = {
        let state = state_of(stream);
        let mut state = state.lock().unwrap();
        if state.corked {
            state.corked_bytes.extend_from_slice(&bytes);
            None
        } else {
            flush(&mut state, &bytes)
        }
    };
    run_deferred_flush(deferred);
    None
}

fn nullable_error(error: Option<crate::UnknownType>) -> crate::UnknownType {
    crate::Value::Class(Rc::new(match error {
        Some(error) => Purs_Data_Nullable::Data_Nullable_notNull(error),
        None => Purs_Data_Nullable::Data_Nullable_null(),
    }))
}

fn reject_write(stream: &Rc<EventEmitter>, error: crate::UnknownType) -> crate::UnknownType {
    purust_emitter_emit(stream, "error", vec![error.clone()]);
    error
}

/// Node reports backpressure when the buffered readable side exceeds the high
/// water mark; `drain` fires once a read brings it back under.
fn writable_has_room(stream: &Rc<EventEmitter>) -> bool {
    let state = state_of(stream);
    let state = state.lock().unwrap();
    let buffered = state.source.len().saturating_sub(state.cursor);
    buffered <= state.readable_high_water_mark.max(1) as usize
}

pub fn Node_Stream_writeImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|value, buffer| {
        let stream = unbox_stream(&value);
        let bytes = buffer
            .unwrap_class::<Rc<Purs_Node_Buffer_Immutable::ImmutableBuffer>>()
            .bytes();
        match write_value(&stream, bytes) {
            None => crate::mk_bool(writable_has_room(&stream)),
            Some(error) => {
                let _ = reject_write(&stream, error);
                crate::mk_bool(false)
            }
        }
    })))
}

pub fn Node_Stream_writeCbImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(
        |value, buffer, callback| {
            let stream = unbox_stream(&value);
            let bytes = buffer
                .unwrap_class::<Rc<Purs_Node_Buffer_Immutable::ImmutableBuffer>>()
                .bytes();
            match write_value(&stream, bytes) {
                None => {
                    callback.unwrap_func1()(nullable_error(None));
                    crate::mk_bool(writable_has_room(&stream))
                }
                Some(error) => {
                    let error = reject_write(&stream, error);
                    callback.unwrap_func1()(nullable_error(Some(error)));
                    crate::mk_bool(false)
                }
            }
        },
    )))
}

pub fn Node_Stream_writeStringImpl() -> crate::UnknownType {
    crate::Value::Func3(purust_core::Func3::Shared(Rc::new(
        |value, text, encoding| {
            let stream = unbox_stream(&value);
            match write_value(&stream, encode_string(&text.unwrap_string(), &encoding)) {
                None => crate::mk_bool(writable_has_room(&stream)),
                Some(error) => {
                    let _ = reject_write(&stream, error);
                    crate::mk_bool(false)
                }
            }
        },
    )))
}

pub fn Node_Stream_writeStringCbImpl() -> crate::UnknownType {
    crate::Value::Func4(purust_core::Func4::Shared(Rc::new(
        |value, text, encoding, callback| {
            let stream = unbox_stream(&value);
            match write_value(&stream, encode_string(&text.unwrap_string(), &encoding)) {
                None => {
                    callback.unwrap_func1()(nullable_error(None));
                    crate::mk_bool(writable_has_room(&stream))
                }
                Some(error) => {
                    let error = reject_write(&stream, error);
                    callback.unwrap_func1()(nullable_error(Some(error)));
                    crate::mk_bool(false)
                }
            }
        },
    )))
}

fn encode_string(text: &str, encoding: &crate::UnknownType) -> Vec<u8> {
    match encoding.resolve() {
        crate::Value::String(name) => {
            let name = Purs_Node_Encoding::purust_encoding_from_name(name);
            purust_encoding_encode(name, text)
        }
        _ => purust_core::purust_string_to_utf8_lossy(text).into_bytes(),
    }
}

pub fn Node_Stream_corkImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        state_of(&stream).lock().unwrap().corked = true;
        crate::Value::Unit
    })))
}

pub fn Node_Stream_uncorkImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let pending = {
            let state = state_of(&stream);
            let mut state = state.lock().unwrap();
            state.corked = false;
            std::mem::take(&mut state.corked_bytes)
        };
        let deferred = {
            let state = state_of(&stream);
            let mut state = state.lock().unwrap();
            flush(&mut state, &pending)
        };
        run_deferred_flush(deferred);
        crate::Value::Unit
    })))
}

pub fn Node_Stream_setDefaultEncodingImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|value, encoding| {
        let stream = unbox_stream(&value);
        state_of(&stream).lock().unwrap().default_encoding = Some(encoding.unwrap_string());
        crate::Value::Unit
    })))
}

pub fn Node_Stream_endCbImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|value, callback| {
        let stream = unbox_stream(&value);
        let error = state_of(&stream).lock().unwrap().error.clone();
        match error {
            Some(error) => {
                callback.unwrap_func1()(nullable_error(Some(error)));
            }
            None => {
                finish_writable(&stream);
                callback.unwrap_func1()(nullable_error(None));
            }
        }
        crate::Value::Unit
    })))
}

pub fn Node_Stream_endImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        finish_writable(&stream);
        crate::Value::Unit
    })))
}

pub fn Node_Stream_writeableImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let state = state_of(&stream);
        let state = state.lock().unwrap();
        let writable = state.writable && !state.destroyed;
        crate::mk_bool(writable)
    })))
}

pub fn Node_Stream_writeableEndedImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let finished = state_of(&stream).lock().unwrap().finished;
        crate::mk_bool(finished)
    })))
}

pub fn Node_Stream_writeableCorkedImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let corked = state_of(&stream).lock().unwrap().corked;
        crate::mk_bool(corked)
    })))
}

pub fn Node_Stream_erroredImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let has_error = state_of(&stream).lock().unwrap().error.is_some();
        crate::mk_bool(has_error)
    })))
}

pub fn Node_Stream_writeableFinishedImpl() -> crate::UnknownType {
    Node_Stream_writeableEndedImpl()
}

pub fn Node_Stream_writeableHighWaterMarkImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let high_water_mark = state_of(&stream).lock().unwrap().readable_high_water_mark;
        crate::mk_number(high_water_mark as f64)
    })))
}

pub fn Node_Stream_writeableLengthImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let pending = {
            let state = state_of(&stream);
            let state = state.lock().unwrap();
            state.corked_bytes.len()
        };
        crate::mk_number(pending as f64)
    })))
}

pub fn Node_Stream_writeableNeedDrainImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Static(|_| crate::mk_bool(false)))
}

pub fn Node_Stream_destroyImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        {
            let state = state_of(&stream);
            let mut state = state.lock().unwrap();
            state.destroyed = true;
            state.closed = true;
        }
        // Node emits `close` after a destroy, and pending readers complete.
        purust_emitter_emit(&stream, "close", Vec::new());
        crate::Value::Unit
    })))
}

pub fn Node_Stream_destroyErrorImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|value, error| {
        let stream = unbox_stream(&value);
        {
            let state = state_of(&stream);
            let mut state = state.lock().unwrap();
            state.destroyed = true;
            state.closed = true;
            state.error = Some(error.clone());
        }
        purust_emitter_emit(&stream, "error", vec![error]);
        purust_emitter_emit(&stream, "close", Vec::new());
        crate::Value::Unit
    })))
}

pub fn Node_Stream_closedImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let closed = state_of(&stream).lock().unwrap().closed;
        crate::mk_bool(closed)
    })))
}

pub fn Node_Stream_destroyedImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let destroyed = state_of(&stream).lock().unwrap().destroyed;
        crate::mk_bool(destroyed)
    })))
}

pub fn Node_Stream_allowHalfOpenImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|value| {
        let stream = unbox_stream(&value);
        let allow_half_open = state_of(&stream).lock().unwrap().allow_half_open;
        crate::mk_bool(allow_half_open)
    })))
}

pub fn Node_Stream_pipelineImpl() -> crate::UnknownType {
    crate::Value::Func4(purust_core::Func4::Shared(Rc::new(
        |source, transforms, destination, callback| {
            let source = unbox_stream(&source);
            let destination = unbox_stream(&destination);
            let transforms: Vec<Rc<EventEmitter>> = transforms
                .unwrap_array()
                .iter()
                .map(unbox_stream)
                .collect();
            defer(move || {
                let mut stages = vec![source.clone()];
                stages.extend(transforms);
                stages.push(destination.clone());
                for pair in stages.windows(2) {
                    let bytes = {
                        let state = state_of(&pair[0]);
                        let mut state = state.lock().unwrap();
                        let remaining = state.source[state.cursor..].to_vec();
                        state.cursor = state.source.len();
                        remaining
                    };
                    let deferred = {
                        let state = state_of(&pair[1]);
                        let mut state = state.lock().unwrap();
                        flush(&mut state, &bytes)
                    };
                    run_deferred_flush(deferred);
                    state_of(&pair[0]).lock().unwrap().end_emitted = true;
                    state_of(&pair[1]).lock().unwrap().end_emitted = true;
                }
                finish_writable(&destination);
                callback.unwrap_func1()(crate::Value::Class(Rc::new(
                    Purs_Data_Nullable::Data_Nullable_null(),
                )));
            });
            crate::Value::Unit
        },
    )))
}

pub fn Node_Stream_readableFromStrImpl() -> crate::UnknownType {
    crate::Value::Func2(purust_core::Func2::Shared(Rc::new(|text, encoding| {
        let bytes = encode_string(&text.unwrap_string(), &encoding);
        purust_stream_box(purust_readable_from_bytes(bytes))
    })))
}

pub fn Node_Stream_readableFromBufImpl() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Shared(Rc::new(|buffer| {
        let bytes = buffer
            .unwrap_class::<Rc<Purs_Node_Buffer_Immutable::ImmutableBuffer>>()
            .bytes();
        purust_stream_box(purust_readable_from_bytes(bytes))
    })))
}

pub fn Node_Stream_newPassThrough() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Static(|_| {
        purust_stream_box(new_pass_through())
    }))
}

/// A duplex that echoes writes to its readable side, like Node's PassThrough.
fn new_pass_through() -> Rc<EventEmitter> {
    let stream = stream_new(true, true);
    let weak_for_write = Rc::downgrade(&stream);
    purust_stream_set_write_hook(
        &stream,
        std::sync::Arc::new(move |bytes| {
            if let Some(target) = weak_for_write.upgrade() {
                purust_stream_push(&target, bytes.to_vec());
            }
        }),
    );
    let weak_for_end = Rc::downgrade(&stream);
    purust_stream_set_end_hook(
        &stream,
        std::sync::Arc::new(move || {
            if let Some(target) = weak_for_end.upgrade() {
                purust_stream_end(&target);
            }
        }),
    );
    stream
}
