// The substrate light-client request-response protocol (`/<genesis>/light/2`).
//
// This is the "heavy" protocol from the load-testing point of view: every
// `RemoteCallRequest` forces the full node to execute a runtime method in its
// proving backend and return a Merkle/execution proof. See `smoldot-info.txt`.
//
// The wire framing mirrors substrate's `GenericCodec`
// (`polkadot-sdk/substrate/client/network/src/request_responses.rs`): each
// message is an `unsigned_varint` length prefix followed by the prost-encoded
// protobuf payload, after which the write half of the substream is closed.
//
// The protobuf messages are generated from the polkadot-sdk light proto by
// `build.rs` (package `api.v1.light`), so the bytes are wire-compatible.

use std::io;
use std::time::Duration;

use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::request_response::{self, Codec, ProtocolSupport};
use libp2p::StreamProtocol;
use prost::Message;

/// Generated `api.v1.light` protobuf types (`Request`, `Response`,
/// `RemoteCallRequest`, ...).
pub mod schema {
    include!(concat!(env!("OUT_DIR"), "/api.v1.light.rs"));
}

/// Default cap for an outbound request body. Light requests are tiny.
const DEFAULT_MAX_REQUEST_SIZE: u64 = 1024 * 1024;
/// Default cap for a response body. Call proofs can be large.
const DEFAULT_MAX_RESPONSE_SIZE: u64 = 16 * 1024 * 1024;

/// A `request_response::Codec` for the substrate light protocol.
///
/// Requests and responses are opaque byte blobs (already prost-encoded by the
/// caller / decoded by [`decode_response`]); this type only handles the
/// length-prefixed framing. A `Response` of `Err(())` means the peer closed the
/// substream without sending anything (mirrors substrate's `GenericCodec`).
#[derive(Debug, Clone)]
pub struct LightCodec {
    max_request_size: u64,
    max_response_size: u64,
}

impl Default for LightCodec {
    fn default() -> Self {
        Self {
            max_request_size: DEFAULT_MAX_REQUEST_SIZE,
            max_response_size: DEFAULT_MAX_RESPONSE_SIZE,
        }
    }
}

#[async_trait]
impl Codec for LightCodec {
    type Protocol = StreamProtocol;
    type Request = Vec<u8>;
    type Response = Result<Vec<u8>, ()>;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        mut io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let length = unsigned_varint::aio::read_usize(&mut io)
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        if length > usize::try_from(self.max_request_size).unwrap_or(usize::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Request size exceeds limit: {} > {}",
                    length, self.max_request_size
                ),
            ));
        }

        let mut buffer = vec![0; length];
        io.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        mut io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        // `Ok(Err(()))` signifies the peer closed the substream without a
        // response; `Err(_)` is a hard protocol error that closes the connection.
        let length = match unsigned_varint::aio::read_usize(&mut io).await {
            Ok(l) => l,
            Err(unsigned_varint::io::ReadError::Io(err))
                if matches!(err.kind(), io::ErrorKind::UnexpectedEof) =>
            {
                return Ok(Err(()))
            }
            Err(err) => return Err(io::Error::new(io::ErrorKind::InvalidInput, err)),
        };

        if length > usize::try_from(self.max_response_size).unwrap_or(usize::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Response size exceeds limit: {} > {}",
                    length, self.max_response_size
                ),
            ));
        }

        let mut buffer = vec![0; length];
        io.read_exact(&mut buffer).await?;
        Ok(Ok(buffer))
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        {
            let mut buffer = unsigned_varint::encode::usize_buffer();
            io.write_all(unsigned_varint::encode::usize(req.len(), &mut buffer))
                .await?;
        }
        io.write_all(&req).await?;
        io.close().await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        // We are an outbound-only client; this is here only to satisfy the trait.
        if let Ok(res) = res {
            {
                let mut buffer = unsigned_varint::encode::usize_buffer();
                io.write_all(unsigned_varint::encode::usize(res.len(), &mut buffer))
                    .await?;
            }
            io.write_all(&res).await?;
        }
        io.close().await?;
        Ok(())
    }
}

/// Build the protocol name for a chain's light protocol: `/<genesis>/light/2`.
///
/// `genesis` may be given with or without the `0x` prefix.
pub fn protocol_name(genesis: &str) -> String {
    format!("/{}/light/2", genesis.trim_start_matches("0x"))
}

/// Construct an outbound-only light request-response behaviour for the given
/// protocol name (e.g. `/<genesis>/light/2`).
///
/// `max_concurrent_streams` is the per-connection cap on in-flight outbound
/// requests. The libp2p default is only 100; set it from the desired spam
/// concurrency so the behaviour doesn't throttle itself with
/// `"max sub-streams reached"` (which would masquerade as server load-shedding).
pub fn behaviour(
    protocol: &str,
    request_timeout: Duration,
    max_concurrent_streams: usize,
) -> Result<request_response::Behaviour<LightCodec>, std::io::Error> {
    let protocol = StreamProtocol::try_from_owned(protocol.to_string())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let cfg = request_response::Config::default()
        .with_request_timeout(request_timeout)
        .with_max_concurrent_streams(max_concurrent_streams);
    Ok(request_response::Behaviour::with_codec(
        LightCodec::default(),
        std::iter::once((protocol, ProtocolSupport::Outbound)),
        cfg,
    ))
}

/// Encode a `RemoteCallRequest` (runtime-API execution proof) for `/light/2`.
///
/// `block_hash` is the block **hash** (raw 32 bytes) to execute against,
/// `method` is the runtime API name (e.g. `Core_version`,
/// `AccountNonceApi_account_nonce`), and `data` is the SCALE-encoded args.
pub fn remote_call(block_hash: Vec<u8>, method: String, data: Vec<u8>) -> Vec<u8> {
    let request = schema::Request {
        request: Some(schema::request::Request::RemoteCallRequest(
            schema::RemoteCallRequest {
                block: block_hash,
                method,
                data,
            },
        )),
    };
    request.encode_to_vec()
}

/// Encode a `RemoteReadRequest` (storage Merkle proof) for `/light/2`.
pub fn remote_read(block_hash: Vec<u8>, keys: Vec<Vec<u8>>) -> Vec<u8> {
    let request = schema::Request {
        request: Some(schema::request::Request::RemoteReadRequest(
            schema::RemoteReadRequest {
                block: block_hash,
                keys,
            },
        )),
    };
    request.encode_to_vec()
}

/// Prefix that turns a bare child trie name into the prefixed storage key the
/// node expects in `RemoteReadChildRequest::storage_key`.
///
/// `sp_storage::well_known_keys::DEFAULT_CHILD_STORAGE_KEY_PREFIX`
/// (`polkadot-sdk/substrate/primitives/storage/src/lib.rs:230`). The handler runs
/// `ChildType::from_prefixed_key` on the field and rejects a bare name with
/// `InvalidChildStorageKey`, so the prefix is mandatory on the wire.
pub const DEFAULT_CHILD_STORAGE_KEY_PREFIX: &[u8] = b":child_storage:default:";

/// Encode a `RemoteReadChildRequest` (child-trie storage Merkle proof) for
/// `/light/2`.
///
/// `child_trie` is the **bare** trie name — for a revive contract, the
/// `trie_id` out of its `ContractInfo`. The `:child_storage:default:` prefix is
/// prepended here, matching what smoldot does in
/// `lib/src/network/codec/storage_call_proof.rs`.
///
/// This is a *read*, not a runtime call: the node answers from
/// `Client::read_child_proof` with no Wasm execution
/// (`substrate/client/network/light/src/light_client_requests/handler.rs:252`).
/// The reply is an ordinary `RemoteReadResponse`, so [`decode_response`] needs no
/// special case for it.
pub fn remote_read_child(block_hash: Vec<u8>, child_trie: &[u8], keys: Vec<Vec<u8>>) -> Vec<u8> {
    let mut storage_key =
        Vec::with_capacity(DEFAULT_CHILD_STORAGE_KEY_PREFIX.len() + child_trie.len());
    storage_key.extend_from_slice(DEFAULT_CHILD_STORAGE_KEY_PREFIX);
    storage_key.extend_from_slice(child_trie);

    let request = schema::Request {
        request: Some(schema::request::Request::RemoteReadChildRequest(
            schema::RemoteReadChildRequest {
                block: block_hash,
                storage_key,
                keys,
            },
        )),
    };
    request.encode_to_vec()
}

/// A decoded `/light/2` response, summarised for logging/stats.
#[derive(Debug, Clone)]
pub enum LightResponse {
    /// `RemoteCallResponse`: execution proof (present unless the node couldn't
    /// answer, e.g. the block was pruned).
    Call { proof_len: Option<usize> },
    /// `RemoteReadResponse`: storage read proof.
    Read { proof_len: Option<usize> },
    /// A `Response` message with no `response` oneof set.
    Empty,
}

impl LightResponse {
    /// Length of the returned proof, if any. `None` means the node answered but
    /// declined to provide a proof (e.g. pruned block / unknown method).
    pub fn proof_len(&self) -> Option<usize> {
        match self {
            LightResponse::Call { proof_len } | LightResponse::Read { proof_len } => *proof_len,
            LightResponse::Empty => None,
        }
    }
}

/// Decode a raw `/light/2` response body into a [`LightResponse`].
pub fn decode_response(bytes: &[u8]) -> Result<LightResponse, prost::DecodeError> {
    let response = schema::Response::decode(bytes)?;
    Ok(match response.response {
        Some(schema::response::Response::RemoteCallResponse(r)) => LightResponse::Call {
            proof_len: r.proof.map(|p| p.len()),
        },
        Some(schema::response::Response::RemoteReadResponse(r)) => LightResponse::Read {
            proof_len: r.proof.map(|p| p.len()),
        },
        None => LightResponse::Empty,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three request builders must land in the right `oneof` slot: the node
    /// dispatches on it (`light_client_requests/handler.rs`), and a child read
    /// arriving as `RemoteReadRequest` would be answered off the main trie —
    /// a successful response to the wrong question.
    #[test]
    fn each_builder_encodes_its_own_request_variant() {
        let block = vec![9u8; 32];

        let call = schema::Request::decode(
            &remote_call(block.clone(), "Core_version".to_string(), Vec::new())[..],
        )
        .expect("valid protobuf");
        assert!(matches!(
            call.request,
            Some(schema::request::Request::RemoteCallRequest(_))
        ));

        let read =
            schema::Request::decode(&remote_read(block.clone(), vec![b":code".to_vec()])[..])
                .expect("valid protobuf");
        assert!(matches!(
            read.request,
            Some(schema::request::Request::RemoteReadRequest(_))
        ));

        let child = schema::Request::decode(
            &remote_read_child(block.clone(), &[1, 2, 3], vec![b"slot".to_vec()])[..],
        )
        .expect("valid protobuf");
        assert!(matches!(
            child.request,
            Some(schema::request::Request::RemoteReadChildRequest(_))
        ));
    }

    /// `storage_key` must carry the `:child_storage:default:` prefix. The handler
    /// runs `ChildType::from_prefixed_key` on it and fails the request with
    /// `InvalidChildStorageKey` for a bare trie name, so an unprefixed key would
    /// measure the error path.
    #[test]
    fn child_reads_prefix_the_bare_trie_id() {
        let block = vec![9u8; 32];
        let trie_id = [0xaa, 0xbb, 0xcc];

        let bytes = remote_read_child(
            block.clone(),
            &trie_id,
            vec![b"one".to_vec(), b"two".to_vec()],
        );
        let request = schema::Request::decode(&bytes[..]).expect("valid protobuf");

        let Some(schema::request::Request::RemoteReadChildRequest(r)) = request.request else {
            panic!("expected a child read request");
        };
        assert_eq!(r.block, block);
        assert_eq!(
            r.storage_key,
            [b":child_storage:default:".as_slice(), &trie_id].concat(),
            "the prefix is prepended exactly once, ahead of the bare trie id"
        );
        assert_eq!(
            r.keys,
            vec![b"one".to_vec(), b"two".to_vec()],
            "every key rides in one request"
        );
    }

    /// A child read is answered with a plain `RemoteReadResponse` — there is no
    /// child-specific response variant — so the stats path must classify it as a
    /// read rather than falling through to `Empty`.
    #[test]
    fn child_read_responses_decode_as_reads() {
        let response = schema::Response {
            response: Some(schema::response::Response::RemoteReadResponse(
                schema::RemoteReadResponse {
                    proof: Some(vec![0u8; 128]),
                },
            )),
        };
        let decoded = decode_response(&response.encode_to_vec()).expect("valid protobuf");
        assert!(matches!(decoded, LightResponse::Read { .. }));
        assert_eq!(decoded.proof_len(), Some(128));
    }
}
