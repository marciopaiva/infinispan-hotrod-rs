//! Remote administration: creating, removing and listing caches
//! (`docs/adr/0014-remote-administration.md`).
//!
//! There are no dedicated administration opcodes on the wire. Every
//! operation here goes through `HotRodConnection::execute_task`, the
//! same generic named-task mechanism (`EXEC_REQUEST`/`EXEC_RESPONSE`)
//! that invokes any server-side task, calling one of the `@@cache@...`
//! tasks the server ships. Confirmed against Infinispan's own
//! `CacheCreateTask.java` and its protocol documentation, not guessed.

use crate::client::HotRodClient;
use crate::error::{Error, Result};
use crate::remote_cache::RemoteCache;

/// A flag from `CacheContainerAdmin.AdminFlag`, comma-joined into the
/// `flags` task parameter when set. `Volatile` marks the cache as
/// non-persistent across a cluster restart; `Update` lets
/// `create_cache`/`get_or_create_cache` update an existing cache's
/// configuration instead of failing because it already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminFlag {
    Volatile,
    Update,
}

impl AdminFlag {
    fn as_str(self) -> &'static str {
        match self {
            AdminFlag::Volatile => "VOLATILE",
            AdminFlag::Update => "UPDATE",
        }
    }
}

/// A cache configuration for `create_cache`/`get_or_create_cache`:
/// either the name of an existing template, or a full configuration
/// document. The server auto-detects the document's format (XML,
/// a bare XML fragment, JSON and YAML all work, confirmed against
/// Infinispan's own tests; its protocol documentation mentions XML
/// only, which is stale).
#[derive(Debug, Clone)]
pub enum CacheConfig {
    Template(String),
    Definition(String),
}

/// A handle for remote cache administration, obtained from
/// `HotRodClient::administration`. Every operation here runs at the
/// cache-manager level, not against any specific cache: there is no
/// key to route by, so every call goes to the active seed, the same
/// as `RemoteCache::query`.
pub struct Administration<'a> {
    client: &'a HotRodClient,
    flags: Vec<AdminFlag>,
}

impl<'a> Administration<'a> {
    pub(crate) fn new(client: &'a HotRodClient) -> Self {
        Self {
            client,
            flags: Vec::new(),
        }
    }

    /// Returns a copy of this handle that passes `flags` on every
    /// `create_cache`/`get_or_create_cache`/`remove_cache` call it
    /// makes from here on.
    pub fn with_flags(mut self, flags: impl IntoIterator<Item = AdminFlag>) -> Self {
        self.flags = flags.into_iter().collect();
        self
    }

    fn flags_param(&self) -> Option<(String, Vec<u8>)> {
        if self.flags.is_empty() {
            return None;
        }
        let joined = self
            .flags
            .iter()
            .map(|flag| flag.as_str())
            .collect::<Vec<_>>()
            .join(",");
        Some(("flags".to_string(), joined.into_bytes()))
    }

    // Tasks run at the cache-manager level, not against any specific
    // cache, so which `RemoteCache` this routes through only matters
    // for connection pooling, never for the request itself:
    // `execute_task` always declares an empty cache name on the wire
    // regardless of what this handle carries. Reusing the default
    // cache's own pool, rather than inventing a separate reserved
    // name, costs nothing and avoids one more pool per client.
    fn exec_cache(&self) -> RemoteCache {
        self.client.cache("")
    }

    async fn create_cache_with_task(
        &self,
        task_name: &str,
        name: &str,
        config: CacheConfig,
    ) -> Result<()> {
        let mut params = vec![("name".to_string(), name.as_bytes().to_vec())];
        match config {
            CacheConfig::Template(template) => {
                params.push(("template".to_string(), template.into_bytes()));
            }
            CacheConfig::Definition(definition) => {
                params.push(("configuration".to_string(), definition.into_bytes()));
            }
        }
        if let Some(flags) = self.flags_param() {
            params.push(flags);
        }
        self.exec_cache()
            .run_exec(task_name.to_string(), params)
            .await?;
        Ok(())
    }

    /// Creates `name`, failing if a cache with that name already
    /// exists, unless `AdminFlag::Update` is set.
    pub async fn create_cache(&self, name: &str, config: CacheConfig) -> Result<()> {
        self.create_cache_with_task("@@cache@create", name, config)
            .await
    }

    /// Creates `name` if it does not exist yet; otherwise returns
    /// successfully without changing it.
    pub async fn get_or_create_cache(&self, name: &str, config: CacheConfig) -> Result<()> {
        self.create_cache_with_task("@@cache@getorcreate", name, config)
            .await
    }

    /// Removes `name`. Whether removing a cache that does not exist
    /// is an error could not be confirmed against a real server
    /// during this feature's research: this call is passed through
    /// unchanged, surfacing whatever the server returns.
    pub async fn remove_cache(&self, name: &str) -> Result<()> {
        let mut params = vec![("name".to_string(), name.as_bytes().to_vec())];
        if let Some(flags) = self.flags_param() {
            params.push(flags);
        }
        self.exec_cache()
            .run_exec("@@cache@remove".to_string(), params)
            .await?;
        Ok(())
    }

    /// Lists every cache name known to the cache manager.
    pub async fn cache_names(&self) -> Result<Vec<String>> {
        let response = self
            .exec_cache()
            .run_exec("@@cache@names".to_string(), Vec::new())
            .await?;
        parse_json_string_array(&response)
    }
}

impl HotRodClient {
    /// Returns a handle for remote cache administration
    /// (`docs/adr/0014-remote-administration.md`).
    pub fn administration(&self) -> Administration<'_> {
        Administration::new(self)
    }
}

/// Parses the one JSON shape remote administration ever returns: a
/// flat array of strings, the way `@@cache@names` replies. Hand-rolled
/// rather than pulling in a JSON dependency for this single shape, the
/// same call already made for the Protobuf envelope
/// (`docs/adr/0013-remote-query.md`).
fn parse_json_string_array(bytes: &[u8]) -> Result<Vec<String>> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| Error::MalformedAdminResponse(format!("response is not valid UTF-8: {e}")))?;
    let mut chars = text.trim().chars().peekable();
    if chars.next() != Some('[') {
        return Err(Error::MalformedAdminResponse(
            "expected a JSON array".to_string(),
        ));
    }
    skip_whitespace(&mut chars);
    let mut result = Vec::new();
    if chars.peek() == Some(&']') {
        chars.next();
        return Ok(result);
    }
    loop {
        skip_whitespace(&mut chars);
        result.push(parse_json_string(&mut chars)?);
        skip_whitespace(&mut chars);
        match chars.next() {
            Some(',') => continue,
            Some(']') => break,
            other => {
                return Err(Error::MalformedAdminResponse(format!(
                    "expected ',' or ']' in JSON array, got {other:?}"
                )))
            }
        }
    }
    Ok(result)
}

fn skip_whitespace(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    while matches!(chars.peek(), Some(c) if c.is_whitespace()) {
        chars.next();
    }
}

fn parse_json_string(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Result<String> {
    if chars.next() != Some('"') {
        return Err(Error::MalformedAdminResponse(
            "expected a JSON string".to_string(),
        ));
    }
    let mut value = String::new();
    loop {
        match chars.next() {
            Some('"') => return Ok(value),
            Some('\\') => match chars.next() {
                Some('"') => value.push('"'),
                Some('\\') => value.push('\\'),
                Some('/') => value.push('/'),
                Some('n') => value.push('\n'),
                Some('t') => value.push('\t'),
                Some('r') => value.push('\r'),
                Some('b') => value.push('\u{8}'),
                Some('f') => value.push('\u{c}'),
                Some('u') => {
                    let code = (0..4)
                        .map(|_| chars.next())
                        .collect::<Option<String>>()
                        .and_then(|hex| u32::from_str_radix(&hex, 16).ok())
                        .and_then(char::from_u32)
                        .ok_or_else(|| {
                            Error::MalformedAdminResponse(
                                "invalid \\u escape in JSON string".to_string(),
                            )
                        })?;
                    value.push(code);
                }
                other => {
                    return Err(Error::MalformedAdminResponse(format!(
                        "invalid escape sequence in JSON string: {other:?}"
                    )))
                }
            },
            Some(c) => value.push(c),
            None => {
                return Err(Error::MalformedAdminResponse(
                    "unterminated JSON string".to_string(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use crate::client::tests::{read_request_opcode, response_header};
    use crate::remote_cache::tests::client_with_seeds;
    use crate::varint::read_vint;
    use crate::wire::{read_array, write_array};

    #[test]
    fn parses_an_empty_array() {
        assert_eq!(
            parse_json_string_array(b"[]").unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn parses_names_with_whitespace_and_escapes() {
        let names = parse_json_string_array(br#" [ "one" , "two\"three" ] "#).unwrap();
        assert_eq!(names, vec!["one".to_string(), "two\"three".to_string()]);
    }

    #[test]
    fn rejects_a_non_array() {
        assert!(parse_json_string_array(b"\"oops\"").is_err());
    }

    #[test]
    fn rejects_a_truncated_array() {
        assert!(parse_json_string_array(br#"["one""#).is_err());
    }

    /// `create_cache` sends `name` and, since a template was given,
    /// `template` rather than `configuration`, plus the comma-joined
    /// `flags` set via `with_flags`. There is no key to route by, so
    /// this (like every other administration call) always goes to
    /// the seed, same as `RemoteCache::query`.
    #[tokio::test]
    async fn create_cache_sends_name_template_and_flags() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x2B, "expected an Exec request");
            let task_name = read_array(&mut stream).await.unwrap();
            assert_eq!(task_name, b"@@cache@create");
            let param_count = read_vint(&mut stream).await.unwrap();
            assert_eq!(param_count, 3);

            let name0 = read_array(&mut stream).await.unwrap();
            let value0 = read_array(&mut stream).await.unwrap();
            assert_eq!(name0, b"name");
            assert_eq!(value0, b"my-cache");

            let name1 = read_array(&mut stream).await.unwrap();
            let value1 = read_array(&mut stream).await.unwrap();
            assert_eq!(name1, b"template");
            assert_eq!(value1, b"org.infinispan.DIST_SYNC");

            let name2 = read_array(&mut stream).await.unwrap();
            let value2 = read_array(&mut stream).await.unwrap();
            assert_eq!(name2, b"flags");
            assert_eq!(value2, b"VOLATILE,UPDATE");

            let mut resp = response_header(id, 0x2C, 0x00);
            write_array(&mut resp, b"\"ok\"");
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![addr], addr);
        client
            .administration()
            .with_flags([AdminFlag::Volatile, AdminFlag::Update])
            .create_cache(
                "my-cache",
                CacheConfig::Template("org.infinispan.DIST_SYNC".to_string()),
            )
            .await
            .expect("create_cache should succeed");

        server.await.unwrap();
    }

    /// `remove_cache` with no flags set sends only `name`, not an
    /// empty `flags` parameter.
    #[tokio::test]
    async fn remove_cache_sends_only_the_name_when_no_flags_are_set() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x2B, "expected an Exec request");
            let task_name = read_array(&mut stream).await.unwrap();
            assert_eq!(task_name, b"@@cache@remove");
            let param_count = read_vint(&mut stream).await.unwrap();
            assert_eq!(param_count, 1);
            let name = read_array(&mut stream).await.unwrap();
            let value = read_array(&mut stream).await.unwrap();
            assert_eq!(name, b"name");
            assert_eq!(value, b"my-cache");

            let mut resp = response_header(id, 0x2C, 0x00);
            write_array(&mut resp, b"");
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![addr], addr);
        client
            .administration()
            .remove_cache("my-cache")
            .await
            .expect("remove_cache should succeed");

        server.await.unwrap();
    }

    /// `cache_names` parses `@@cache@names`'s JSON array response
    /// through `parse_json_string_array`, exercised here against the
    /// full `execute_task` round trip rather than just the parser
    /// unit tests above.
    #[tokio::test]
    async fn cache_names_parses_the_json_array_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x2B, "expected an Exec request");
            let task_name = read_array(&mut stream).await.unwrap();
            assert_eq!(task_name, b"@@cache@names");
            let param_count = read_vint(&mut stream).await.unwrap();
            assert_eq!(param_count, 0);

            let mut resp = response_header(id, 0x2C, 0x00);
            write_array(&mut resp, br#"["cache-one","cache-two"]"#);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![addr], addr);
        let names = client
            .administration()
            .cache_names()
            .await
            .expect("cache_names should succeed");
        assert_eq!(
            names,
            vec!["cache-one".to_string(), "cache-two".to_string()]
        );

        server.await.unwrap();
    }
}
