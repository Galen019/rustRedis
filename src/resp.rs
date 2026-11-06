//! RESP2 framing: request parsing and reply encoding.
//!
//! - requests are arrays of bulk strings; inline commands are rejected
//! - `MAX_LINE`, `MAX_BULK`, and `MAX_ARGS` bound every length the peer controls
//! - replies are encoded into a caller-supplied buffer

use std::io::{self, BufRead, ErrorKind, Read};
use std::sync::Arc;

/// Longest header line accepted, excluding the trailing CRLF.
const MAX_LINE: usize = 64 * 1024;
/// Largest bulk string accepted, in bytes.
const MAX_BULK: usize = 64 * 1024 * 1024;
/// Largest argument count accepted.
const MAX_ARGS: usize = 1024;

const UNEXPECTED_EOF: &str = "unexpected end of input";
const EXPECTED_CRLF: &str = "expected '\\r\\n'";
const TOO_BIG_LINE: &str = "too big inline request";
const INVALID_BULK: &str = "invalid bulk length";
const INVALID_MULTIBULK: &str = "invalid multibulk length";

/// One frame sent to the client.
#[derive(Debug, PartialEq, Eq)]
pub enum Reply {
    /// `+<text>`
    Simple(&'static str),
    /// `$<len>` plus payload, or `$-1` for a miss.
    Bulk(Option<Arc<[u8]>>),
    /// `:<n>`
    Integer(i64),
    /// `-<text>`
    Error(String),
}

/// Appends the wire form of `reply` to `out`.
pub fn encode(reply: &Reply, out: &mut Vec<u8>) {
    match reply {
        Reply::Simple(text) => {
            out.push(b'+');
            out.extend_from_slice(text.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Reply::Bulk(None) => out.extend_from_slice(b"$-1\r\n"),
        Reply::Bulk(Some(bytes)) => {
            write_len(out, b'$', bytes.len());
            out.extend_from_slice(bytes);
            out.extend_from_slice(b"\r\n");
        }
        Reply::Integer(n) => {
            out.push(b':');
            out.extend_from_slice(n.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Reply::Error(text) => {
            out.push(b'-');
            out.extend_from_slice(text.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }
}

fn write_len(out: &mut Vec<u8>, marker: u8, len: usize) {
    out.push(marker);
    out.extend_from_slice(len.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// Escapes peer bytes so they cannot break a RESP line.
pub(crate) fn escaped(bytes: &[u8]) -> String {
    bytes
        .iter()
        .flat_map(|&byte| char::from(byte).escape_default())
        .collect()
}

/// A framing violation: the stream cannot be resynchronized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError(String);

impl ProtocolError {
    fn new(detail: impl Into<String>) -> Self {
        Self(detail.into())
    }

    /// Detail text that follows the `ERR Protocol error: ` prefix.
    pub fn detail(&self) -> &str {
        &self.0
    }
}

/// Why a request could not be read.
#[derive(Debug)]
pub enum RequestError {
    /// The connection failed; there is nothing to reply with.
    Io(io::Error),
    /// The bytes are not RESP2: reply with an error, then close.
    Protocol(ProtocolError),
}

impl From<io::Error> for RequestError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<ProtocolError> for RequestError {
    fn from(e: ProtocolError) -> Self {
        Self::Protocol(e)
    }
}

/// Reads one request.
///
/// - `Ok(None)` on a clean EOF at a frame boundary
/// - `Ok(Some(args))` for one request; empty `args` means the peer sent `*0`
/// - `Err` leaves the connection unusable
pub fn read_request<R: BufRead>(reader: &mut R) -> Result<Option<Vec<Vec<u8>>>, RequestError> {
    let Some(line) = read_line(reader)? else {
        return Ok(None);
    };

    let count = parse_args_len(&line)?;
    if count == 0 {
        return Ok(Some(Vec::new()));
    }

    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let line = read_line(reader)?.ok_or_else(|| ProtocolError::new(UNEXPECTED_EOF))?;
        let len = parse_bulk_len(&line)?;
        // Grow with received bytes: a header alone must not force `MAX_BULK`.
        let mut arg = Vec::new();
        let read = io::Read::take(&mut *reader, len as u64).read_to_end(&mut arg)?;
        if read != len {
            return Err(ProtocolError::new(UNEXPECTED_EOF).into());
        }
        read_terminator(reader)?;
        args.push(arg);
    }

    Ok(Some(args))
}

/// Reads one CRLF-terminated line without its terminator.
///
/// - `None` on EOF before any byte
/// - the read is capped because `read_until` grows its buffer without bound
fn read_line<R: BufRead>(reader: &mut R) -> Result<Option<Vec<u8>>, RequestError> {
    let mut line = Vec::new();
    // `Read::take` called through UFCS: `reader.take(..)` would autoderef to `R`
    // and try to move it. `read_until` alone grows its buffer without bound.
    let read = io::Read::take(reader, (MAX_LINE + 3) as u64).read_until(b'\n', &mut line)?;

    if read == 0 {
        return Ok(None);
    }
    if !line.ends_with(b"\n") {
        // Either the cap stopped the read or the peer died mid-line.
        let detail = if line.len() > MAX_LINE + 2 {
            TOO_BIG_LINE
        } else {
            UNEXPECTED_EOF
        };
        return Err(ProtocolError::new(detail).into());
    }
    if !line.ends_with(b"\r\n") {
        return Err(ProtocolError::new(EXPECTED_CRLF).into());
    }

    line.truncate(line.len() - 2);
    if line.len() > MAX_LINE {
        return Err(ProtocolError::new(TOO_BIG_LINE).into());
    }
    Ok(Some(line))
}

/// Consumes the CRLF that follows a bulk string's payload.
fn read_terminator<R: BufRead>(reader: &mut R) -> Result<(), RequestError> {
    let mut crlf = [0u8; 2];
    read_exact(reader, &mut crlf)?;
    if crlf != *b"\r\n" {
        return Err(ProtocolError::new(EXPECTED_CRLF).into());
    }
    Ok(())
}

/// Reads exactly `buf.len()` bytes, reporting a truncated frame as a protocol error.
fn read_exact<R: BufRead>(reader: &mut R, buf: &mut [u8]) -> Result<(), RequestError> {
    match reader.read_exact(buf) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
            Err(ProtocolError::new(UNEXPECTED_EOF).into())
        }
        Err(e) => Err(e.into()),
    }
}

fn parse_args_len(line: &[u8]) -> Result<usize, ProtocolError> {
    let digits = match line.split_first() {
        Some((b'*', digits)) => digits,
        Some((byte, _)) => {
            return Err(ProtocolError::new(format!(
                "expected '*', got '{}'",
                escaped(std::slice::from_ref(byte))
            )));
        }
        None => return Err(ProtocolError::new(INVALID_MULTIBULK)),
    };

    let count = parse_int(digits, INVALID_MULTIBULK)?;
    if count < 0 || count > MAX_ARGS as i64 {
        return Err(ProtocolError::new(INVALID_MULTIBULK));
    }
    Ok(count as usize)
}

fn parse_bulk_len(line: &[u8]) -> Result<usize, ProtocolError> {
    let digits = match line.split_first() {
        Some((b'$', digits)) => digits,
        Some((byte, _)) => {
            return Err(ProtocolError::new(format!(
                "expected '$', got '{}'",
                escaped(std::slice::from_ref(byte))
            )));
        }
        None => return Err(ProtocolError::new(INVALID_BULK)),
    };

    let len = parse_int(digits, INVALID_BULK)?;
    // `$-1` is a legal null bulk string in a reply, never in a request.
    if len < 0 || len > MAX_BULK as i64 {
        return Err(ProtocolError::new(INVALID_BULK));
    }
    Ok(len as usize)
}

/// Parses an ASCII integer, rejecting anything `i64::from_str` would tolerate loosely.
fn parse_int(digits: &[u8], invalid: &'static str) -> Result<i64, ProtocolError> {
    let text = std::str::from_utf8(digits).map_err(|_| ProtocolError::new(invalid))?;
    // Rejects empty input and the `+`/whitespace forms `from_str` would accept.
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit() || b == b'-') {
        return Err(ProtocolError::new(invalid));
    }
    text.parse::<i64>().map_err(|_| ProtocolError::new(invalid))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_reply(reply: Reply) -> Vec<u8> {
        let mut out = Vec::new();
        encode(&reply, &mut out);
        out
    }

    fn args(parts: &[&[u8]]) -> Vec<Vec<u8>> {
        parts.iter().map(|part| part.to_vec()).collect()
    }

    fn protocol_detail(bytes: &[u8]) -> String {
        match read_request(&mut &bytes[..]) {
            Err(RequestError::Protocol(e)) => e.detail().to_string(),
            other => panic!("expected a protocol error, got {other:?}"),
        }
    }

    #[test]
    fn encodes_every_reply_variant() {
        assert_eq!(encode_reply(Reply::Simple("PONG")), b"+PONG\r\n");
        assert_eq!(encode_reply(Reply::Bulk(None)), b"$-1\r\n");
        assert_eq!(
            encode_reply(Reply::Bulk(Some(Arc::from(&b""[..])))),
            b"$0\r\n\r\n"
        );
        assert_eq!(
            encode_reply(Reply::Bulk(Some(Arc::from(&b"v"[..])))),
            b"$1\r\nv\r\n"
        );
        assert_eq!(encode_reply(Reply::Integer(0)), b":0\r\n");
        assert_eq!(encode_reply(Reply::Integer(-7)), b":-7\r\n");
        assert_eq!(
            encode_reply(Reply::Error("ERR nope".into())),
            b"-ERR nope\r\n"
        );
    }

    #[test]
    fn parses_a_single_request() {
        let request = &b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"[..];
        assert_eq!(
            read_request(&mut &request[..]).expect("parse"),
            Some(args(&[b"GET", b"k"]))
        );
    }

    #[test]
    fn parses_pipelined_requests_in_order() {
        let mut input = &b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n"[..];
        assert_eq!(
            read_request(&mut input).expect("first"),
            Some(args(&[b"PING"]))
        );
        assert_eq!(
            read_request(&mut input).expect("second"),
            Some(args(&[b"DEL", b"k"]))
        );
        assert_eq!(read_request(&mut input).expect("eof"), None);
    }

    #[test]
    fn empty_array_is_an_empty_argument_list() {
        let request = &b"*0\r\n"[..];
        assert_eq!(
            read_request(&mut &request[..]).expect("parse"),
            Some(Vec::new())
        );
    }

    #[test]
    fn clean_eof_is_not_an_error() {
        let request = &b""[..];
        assert_eq!(read_request(&mut &request[..]).expect("parse"), None);
    }

    #[test]
    fn payload_may_contain_crlf() {
        let request = &b"*1\r\n$4\r\na\r\nb\r\n"[..];
        assert_eq!(
            read_request(&mut &request[..]).expect("parse"),
            Some(args(&[b"a\r\nb"]))
        );
    }

    #[test]
    fn rejects_inline_commands() {
        assert_eq!(protocol_detail(b"PING\r\n"), "expected '*', got 'P'");
    }

    #[test]
    fn escapes_peer_bytes_in_error_text() {
        assert_eq!(escaped(b"X\r\n+Fake"), "X\\r\\n+Fake");
    }

    #[test]
    fn rejects_control_bytes_without_emitting_them() {
        assert_eq!(protocol_detail(b"\r\r\n"), "expected '*', got '\\r'");
    }

    #[test]
    fn rejects_non_bulk_arguments() {
        assert_eq!(protocol_detail(b"*1\r\n:1\r\n"), "expected '$', got ':'");
    }

    #[test]
    fn rejects_negative_bulk_length() {
        assert_eq!(protocol_detail(b"*1\r\n$-1\r\n"), INVALID_BULK);
    }

    #[test]
    fn rejects_bulk_length_over_the_cap() {
        let request = format!("*1\r\n${}\r\n", MAX_BULK + 1);
        assert_eq!(protocol_detail(request.as_bytes()), INVALID_BULK);
    }

    #[test]
    fn rejects_argument_count_over_the_cap() {
        let request = format!("*{}\r\n", MAX_ARGS + 1);
        assert_eq!(protocol_detail(request.as_bytes()), INVALID_MULTIBULK);
    }

    #[test]
    fn rejects_non_numeric_lengths() {
        assert_eq!(protocol_detail(b"*x\r\n"), INVALID_MULTIBULK);
        assert_eq!(protocol_detail(b"*1\r\n$+1\r\n"), INVALID_BULK);
        assert_eq!(protocol_detail(b"*\r\n"), INVALID_MULTIBULK);
    }

    #[test]
    fn rejects_truncated_frames() {
        assert_eq!(
            protocol_detail(b"*2\r\n$3\r\nGET\r\n"),
            "unexpected end of input"
        );
        assert_eq!(
            protocol_detail(b"*2\r\n$3\r\nGE"),
            "unexpected end of input"
        );
    }

    #[test]
    fn rejects_missing_payload_terminator() {
        assert_eq!(protocol_detail(b"*1\r\n$1\r\nk\rX"), "expected '\\r\\n'");
    }

    #[test]
    fn rejects_bare_line_feed() {
        assert_eq!(protocol_detail(b"*0\n"), "expected '\\r\\n'");
    }

    #[test]
    fn rejects_over_long_lines() {
        let mut no_newline = vec![b'*'; MAX_LINE + 3];
        assert_eq!(protocol_detail(&no_newline), TOO_BIG_LINE);

        no_newline.truncate(MAX_LINE + 1);
        no_newline.extend_from_slice(b"\r\n");
        assert_eq!(protocol_detail(&no_newline), TOO_BIG_LINE);
    }
}
