#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "common.rs"]
mod common;

fuzz_target!(|data: &[u8]| {
    common::runtime().block_on(async {
        let mut reader = data;
        let _ = hotrod_protocol::fuzz_internal::read_array(&mut reader).await;

        let mut reader = data;
        let _ = hotrod_protocol::fuzz_internal::read_string(&mut reader).await;

        let mut reader = data;
        let _ = hotrod_protocol::fuzz_internal::read_string_map(&mut reader).await;
    });
});
