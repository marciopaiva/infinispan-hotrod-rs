//! Shared by every target in this crate: a lazily-built, reused
//! current-thread Tokio runtime. `&[u8]` already implements
//! `AsyncRead` by reading straight from the slice, so none of these
//! targets ever actually waits on real I/O; this only exists to drive
//! each parser's first (and only) `.await` to completion.

use std::sync::OnceLock;
use tokio::runtime::Runtime;

pub fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("building a current-thread runtime never fails")
    })
}
