//! Response status byte, mirroring `HotRodConstants`'s status codes and the
//! `isSuccess`/`isNotExecuted`/`isNotExist`/`hasPrevious` predicates.
//!
//! Object-storage status variants (0x06-0x08) are not modeled: phase 1 never
//! negotiates the `APPLICATION_OBJECT` media type that triggers them.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Status(pub u8);

impl Status {
    const SUCCESS: u8 = 0x00;
    const NOT_PUT_REMOVED_REPLACED: u8 = 0x01;
    const KEY_DOES_NOT_EXIST: u8 = 0x02;
    const SUCCESS_WITH_PREVIOUS: u8 = 0x03;
    const NOT_EXECUTED_WITH_PREVIOUS: u8 = 0x04;
    /// `IterationNext`'s `iterationId` is no longer known to the server
    /// (reaped after five minutes idle, or the server restarted), or
    /// `IterationEnd` was sent for one already gone. Carries no message
    /// body in either case, unlike every status in `is_error()`: confirmed
    /// against `DefaultIterationManager`/`Encoder2x` on the server side.
    const INVALID_ITERATION: u8 = 0x05;

    const INVALID_MAGIC_OR_MESSAGE_ID: u8 = 0x81;
    const UNKNOWN_COMMAND: u8 = 0x82;
    const UNKNOWN_VERSION: u8 = 0x83;
    const REQUEST_PARSING_ERROR: u8 = 0x84;
    const SERVER_ERROR: u8 = 0x85;
    const COMMAND_TIMED_OUT: u8 = 0x86;
    const NODE_SUSPECTED: u8 = 0x87;
    const ILLEGAL_LIFECYCLE_STATE: u8 = 0x88;

    pub(crate) fn is_success(self) -> bool {
        matches!(self.0, Self::SUCCESS | Self::SUCCESS_WITH_PREVIOUS)
    }

    pub(crate) fn is_not_executed(self) -> bool {
        matches!(
            self.0,
            Self::NOT_PUT_REMOVED_REPLACED | Self::NOT_EXECUTED_WITH_PREVIOUS
        )
    }

    pub(crate) fn is_not_exist(self) -> bool {
        self.0 == Self::KEY_DOES_NOT_EXIST
    }

    pub(crate) fn is_invalid_iteration(self) -> bool {
        self.0 == Self::INVALID_ITERATION
    }

    /// Every error status the server can send carries just one thing in the
    /// response body: a UTF-8 message (see Codec30.checkForErrorsInResponseStatus).
    pub(crate) fn is_error(self) -> bool {
        matches!(
            self.0,
            Self::INVALID_MAGIC_OR_MESSAGE_ID
                | Self::UNKNOWN_COMMAND
                | Self::UNKNOWN_VERSION
                | Self::REQUEST_PARSING_ERROR
                | Self::SERVER_ERROR
                | Self::COMMAND_TIMED_OUT
                | Self::NODE_SUSPECTED
                | Self::ILLEGAL_LIFECYCLE_STATE
        )
    }

    pub(crate) fn is_known(self) -> bool {
        self.is_success()
            || self.is_not_executed()
            || self.is_not_exist()
            || self.is_invalid_iteration()
            || self.is_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_success_is_success_and_nothing_else() {
        let s = Status(0x00);
        assert!(s.is_success());
        assert!(!s.is_not_executed());
        assert!(!s.is_not_exist());
        assert!(!s.is_error());
    }

    #[test]
    fn server_error_is_error() {
        assert!(Status(0x85).is_error());
    }

    #[test]
    fn unrecognized_byte_is_unknown() {
        assert!(!Status(0x99).is_known());
    }

    #[test]
    fn invalid_iteration_is_known_but_not_an_error() {
        let s = Status(0x05);
        assert!(s.is_known());
        assert!(s.is_invalid_iteration());
        assert!(!s.is_error());
    }
}
