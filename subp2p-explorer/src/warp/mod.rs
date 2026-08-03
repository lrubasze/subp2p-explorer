// Copyright 2023 Alexandru Vasile
// This file is dual-licensed as Apache-2.0 or GPL-3.0.
// see LICENSE for license details.

//! The GRANDPA warp-sync request-response protocol (`/<genesis>/sync/warp`).
//!
//! This is what a warp-syncing client asks a full node for first: a chain of
//! GRANDPA authority-set handovers, each certified by the previous set, letting
//! the client skip every block in between.
//!
//! Wire format, verified against polkadot-sdk
//! `0e1812505425812c01e6ff6d4f28f6edf729678a`:
//!
//! - **Framing** is substrate's `GenericCodec`, the same one `/light/2` uses, so
//!   [`crate::light::LightCodec`] handles this protocol unchanged — a varint
//!   length prefix, the payload, then the write half closed.
//! - **The request** is `WarpProofRequest { begin: B::Hash }`, which SCALE-encodes
//!   to the bare 32 hash bytes. The node declares `max_request_size` for this
//!   protocol as exactly 32 (`sync/src/warp_request_handler.rs:56`), so there is
//!   no room for anything else.
//! - **The response** is a SCALE-encoded
//!   `WarpSyncProof { proofs: Vec<WarpSyncFragment>, is_finished: bool }`, capped
//!   at `MAX_WARP_SYNC_PROOF_SIZE` = 8 MiB (`grandpa/src/warp_proof.rs:61`).
//!
//! `begin` needs only to be a **finalized block hash on the canonical chain**
//! (`warp_proof.rs:95-115` checks exactly that, and nothing about authority-set
//! changes), so a caller can walk the chain using block hashes from an RPC
//! endpoint instead of decoding responses.
//!
//! We deliberately do not decode fragments. Each carries a header *and* a GRANDPA
//! justification, and SCALE `Vec`s have no per-item offsets, so reaching the last
//! fragment means decoding every justification before it. Two fields are readable
//! without any of that, and they are all a load test needs — see
//! [`summarize_response`].

use codec::{Compact, Decode};

/// The `begin` field of a `WarpProofRequest` is a 32-byte block hash, and the
/// whole request is nothing but that hash.
pub const REQUEST_LEN: usize = 32;

/// Build the protocol name for a chain's warp-sync protocol: `/<genesis>/sync/warp`.
///
/// `genesis` is hex, with or without the `0x` prefix.
pub fn protocol_name(genesis: &str) -> String {
    format!("/{}/sync/warp", genesis.trim_start_matches("0x"))
}

/// Build the protocol name for a chain that sets a fork id:
/// `/<genesis>/<fork_id>/sync/warp`.
pub fn protocol_name_with_fork(genesis: &str, fork_id: &str) -> String {
    format!(
        "/{}/{}/sync/warp",
        genesis.trim_start_matches("0x"),
        fork_id
    )
}

/// Encode a `WarpProofRequest`. `begin` must be a finalized, canonical block hash
/// as far as the serving node is concerned; otherwise it answers with
/// `InvalidRequest` and closes the substream without a body.
pub fn encode_request(begin: &[u8; 32]) -> Vec<u8> {
    begin.to_vec()
}

/// What we read out of a warp proof without decoding its fragments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WarpProofSummary {
    /// Number of fragments, from the leading compact-encoded `Vec` length.
    pub fragments: u64,
    /// The trailing `is_finished` flag. `false` means the node stopped at the
    /// 8 MiB cap and more proof remains beyond this response.
    pub is_finished: bool,
    /// Total body length in bytes.
    pub len: usize,
}

/// Summarise a warp proof response body.
///
/// Reads only the two fields that need no knowledge of fragment contents:
/// SCALE lays out a struct's fields in declaration order, so the body is
/// `Compact(fragment_count) ++ fragments… ++ is_finished`, which puts the count
/// at the front and the one-byte flag at the very end.
///
/// Returns `None` if the body is too short to hold either field, or if the
/// leading compact length is malformed.
pub fn summarize_response(bytes: &[u8]) -> Option<WarpProofSummary> {
    // Shortest valid body: compact 0 (one byte) + is_finished (one byte).
    if bytes.len() < 2 {
        return None;
    }

    let mut cursor = bytes;
    let Compact(fragments) = Compact::<u64>::decode(&mut cursor).ok()?;

    // `is_finished` is a `bool`, always the final byte.
    let is_finished = match bytes[bytes.len() - 1] {
        0 => false,
        1 => true,
        // Not a valid SCALE bool: treat the body as unparseable rather than
        // guessing, so a framing bug surfaces instead of being averaged in.
        _ => return None,
    };

    Some(WarpProofSummary {
        fragments,
        is_finished,
        len: bytes.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use codec::Encode;

    #[test]
    fn request_is_the_bare_hash() {
        let begin = [7u8; 32];
        let encoded = encode_request(&begin);
        assert_eq!(encoded, begin.to_vec());
        // The node's max_request_size for /sync/warp is exactly 32, so anything
        // longer than this would be rejected outright.
        assert_eq!(encoded.len(), REQUEST_LEN);
    }

    #[test]
    fn protocol_names_tolerate_the_0x_prefix() {
        let bare = "b0a8d493285c2df73290dfb7e61f870f17b41801197a149ca93654499ea3dafe";
        let expected = format!("/{bare}/sync/warp");
        assert_eq!(protocol_name(bare), expected);
        assert_eq!(protocol_name(&format!("0x{bare}")), expected);
        assert_eq!(
            protocol_name_with_fork(bare, "ksmcc3"),
            format!("/{bare}/ksmcc3/sync/warp")
        );
    }

    /// Build a body the same way SCALE would: compact length, opaque fragment
    /// bytes, then the flag. The summary must not care what the fragments hold.
    fn body(fragments: u64, fragment_bytes: &[u8], is_finished: bool) -> Vec<u8> {
        let mut out = Compact(fragments).encode();
        out.extend_from_slice(fragment_bytes);
        out.extend_from_slice(&is_finished.encode());
        out
    }

    #[test]
    fn summarizes_without_decoding_fragments() {
        // Opaque filler standing in for headers + justifications we never parse.
        let filler = vec![0xab; 5000];
        let b = body(3, &filler, false);
        let s = summarize_response(&b).expect("summary");
        assert_eq!(s.fragments, 3);
        assert!(!s.is_finished, "0x00 tail means more proof remains");
        assert_eq!(s.len, b.len());

        let b = body(1, &[0x01, 0x02], true);
        let s = summarize_response(&b).expect("summary");
        assert_eq!(s.fragments, 1);
        assert!(s.is_finished);
    }

    /// A `begin` at or past the last authority-set change yields an empty proof
    /// rather than an error, so this is a real response shape, not an edge case.
    #[test]
    fn empty_proof_is_two_bytes() {
        let b = body(0, &[], true);
        assert_eq!(b, vec![0x00, 0x01]);
        let s = summarize_response(&b).expect("summary");
        assert_eq!(s.fragments, 0);
        assert!(s.is_finished);
        assert_eq!(s.len, 2);
    }

    /// A compact length that crosses into two-byte mode must still be read
    /// correctly — 64 is the first value that does.
    #[test]
    fn reads_multi_byte_compact_lengths() {
        for count in [63u64, 64, 16_383, 16_384] {
            let b = body(count, &[0xff; 10], false);
            let s = summarize_response(&b).expect("summary");
            assert_eq!(s.fragments, count, "fragment count {count} round-trips");
        }
    }

    #[test]
    fn rejects_bodies_it_cannot_read() {
        assert_eq!(summarize_response(&[]), None, "empty");
        assert_eq!(summarize_response(&[0x00]), None, "no room for the flag");
        // 0x02 is not a valid SCALE bool.
        assert_eq!(summarize_response(&[0x00, 0x02]), None, "bad bool");
    }
}
