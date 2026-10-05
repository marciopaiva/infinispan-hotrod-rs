#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "common.rs"]
mod common;

fuzz_target!(|data: &[u8]| {
    common::runtime().block_on(async {
        let _ = hotrod_protocol::fuzz_internal::read_response_header(data).await;
    });
});
