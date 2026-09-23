// Test-only FFI: the upstream suite exercises `pipe` through a gzip round trip
// (`zlib.createGzip`/`createGunzip` in the JavaScript stack).
use std::io::Write;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use flate2::write::{GzDecoder, GzEncoder};
use flate2::Compression;

use Purs_Node_Stream::{
    purust_stream_box, purust_stream_end, purust_stream_new_stream, purust_stream_push,
    purust_stream_set_end_hook, purust_stream_set_write_hook,
};

type Encoder = GzEncoder<Vec<u8>>;
type Decoder = GzDecoder<Vec<u8>>;

fn drain(inner: &mut Vec<u8>) -> Vec<u8> {
    inner.drain(..).collect()
}

pub fn Test_Main_createGzip() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Static(|_| {
        let stream = purust_stream_new_stream(true, true);
        let encoder = Rc::new(Mutex::new(Some(GzEncoder::new(
            Vec::new(),
            Compression::default(),
        ))));
        let weak_for_write = Rc::downgrade(&stream);
        let encoder_for_write = encoder.clone();
        purust_stream_set_write_hook(
            &stream,
            Arc::new(move |bytes| {
                let mut encoder = encoder_for_write.lock().unwrap();
                let Some(encoder) = encoder.as_mut() else {
                    return;
                };
                encoder.write_all(bytes).expect("gzip write");
                let pending = drain(encoder.get_mut());
                drop(encoder);
                if let Some(target) = weak_for_write.upgrade() {
                    purust_stream_push(&target, pending);
                }
            }),
        );
        let weak_for_end = Rc::downgrade(&stream);
        purust_stream_set_end_hook(
            &stream,
            Arc::new(move || {
                let mut encoder = encoder.lock().unwrap();
                let Some(encoder) = encoder.take() else {
                    return;
                };
                let trailer = encoder.finish().expect("gzip finish");
                if let Some(target) = weak_for_end.upgrade() {
                    if !trailer.is_empty() {
                        purust_stream_push(&target, trailer);
                    }
                    purust_stream_end(&target);
                }
            }),
        );
        purust_stream_box(stream)
    }))
}

pub fn Test_Main_createGunzip() -> crate::UnknownType {
    crate::Value::Func1(purust_core::Func1::Static(|_| {
        let stream = purust_stream_new_stream(true, true);
        let decoder = Rc::new(Mutex::new(Some(GzDecoder::new(Vec::new()))));
        let weak_for_write = Rc::downgrade(&stream);
        let decoder_for_write = decoder.clone();
        purust_stream_set_write_hook(
            &stream,
            Arc::new(move |bytes| {
                let mut decoder = decoder_for_write.lock().unwrap();
                let Some(decoder) = decoder.as_mut() else {
                    return;
                };
                decoder.write_all(bytes).expect("gunzip write");
                let pending = drain(decoder.get_mut());
                drop(decoder);
                if let Some(target) = weak_for_write.upgrade() {
                    purust_stream_push(&target, pending);
                }
            }),
        );
        let weak_for_end = Rc::downgrade(&stream);
        purust_stream_set_end_hook(
            &stream,
            Arc::new(move || {
                let mut decoder = decoder.lock().unwrap();
                let Some(decoder) = decoder.take() else {
                    return;
                };
                let trailer = decoder.finish().expect("gunzip finish");
                if let Some(target) = weak_for_end.upgrade() {
                    if !trailer.is_empty() {
                        purust_stream_push(&target, trailer);
                    }
                    purust_stream_end(&target);
                }
            }),
        );
        purust_stream_box(stream)
    }))
}
