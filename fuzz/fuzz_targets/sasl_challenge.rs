#![no_main]

use libfuzzer_sys::fuzz_target;

// Fixed credentials: this target is about the robustness of parsing a
// *server-sent* challenge, the one part of either exchange with
// attacker-controlled bytes, not about fuzzing the client's own
// username/password handling.
const AUTHCID: &str = "fuzz-user";
const PASSWORD: &str = "fuzz-password";

fuzz_target!(|data: &[u8]| {
    let _ = hotrod_protocol::fuzz_internal::scram_respond(AUTHCID, PASSWORD, data);
    let _ = hotrod_protocol::fuzz_internal::digest_respond(AUTHCID, PASSWORD, data);
});
