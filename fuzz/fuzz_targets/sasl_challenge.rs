#![no_main]

use libfuzzer_sys::fuzz_target;

// Fixed credentials: this target is about the robustness of parsing a
// *server-sent* challenge, the one part of either exchange with
// attacker-controlled bytes, not about fuzzing the client's own
// username/password handling.
const AUTHCID: &str = "fuzz-user";
const PASSWORD: &str = "fuzz-password";

fuzz_target!(|data: &[u8]| {
    // Split in two: the first half plays the server-first message,
    // the second the server-final one, so a single input can drive a
    // mechanism through both real server-sent challenges in one
    // exchange, reaching the signature-verification branch
    // (`scram_respond`/`digest_respond`'s doc comment explains why a
    // single challenge can never do that).
    let mid = data.len() / 2;
    let (server_first, server_final) = data.split_at(mid);

    let _ = hotrod_protocol::fuzz_internal::scram_respond(
        AUTHCID,
        PASSWORD,
        server_first,
        server_final,
    );
    let _ = hotrod_protocol::fuzz_internal::digest_respond(
        AUTHCID,
        PASSWORD,
        server_first,
        server_final,
    );
});
