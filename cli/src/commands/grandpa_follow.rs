// Copyright 2023 Alexandru Vasile
// This file is dual-licensed as Apache-2.0 or GPL-3.0.
// see LICENSE for license details.

//! The smoldot side of the `/grandpa/1` notification protocol, for synthetic
//! peers.
//!
//! A light client never votes. On this substream it only (a) tells the node its
//! view with a *neighbor packet* — set id, round, finalized height — and (b)
//! receives *commit* messages, which are the only way it learns that a block is
//! final. [`GrandpaFollower`] does exactly that: it mirrors the node's set id in
//! our neighbor packet as soon as the node's first packet reveals it, advances
//! our finalized height when a commit arrives, and re-sends our packet after each
//! change, the way smoldot's `set_local_grandpa_state` does after it finalizes.
//!
//! Getting this right matters for load tests, not just for measuring finality:
//! the node's gossip validator only counts a peer as a light gossip peer once the
//! substream is open, and only ever sends commits to a peer whose view is in the
//! current set (`View::consider_global`). A holder that refuses the substream, or
//! never sends a packet, is invisible to `lucky_light_peers` and does not load
//! the mechanism that <https://github.com/paritytech/smoldot/issues/3375> is
//! about.
//!
//! Wire format, from `sc-consensus-grandpa/src/communication/gossip.rs`:
//! `GossipMessage` is a SCALE enum — `Vote` = 0, `Commit` = 1, `Neighbor` = 2 —
//! and `Neighbor` wraps `VersionedNeighborPacket::V1` (index 1).

use codec::{Decode, Encode};
use futures::channel::mpsc;
use primitive_types::H256;

const GOSSIP_VOTE: u8 = 0;
const GOSSIP_COMMIT: u8 = 1;
const GOSSIP_NEIGHBOR: u8 = 2;
const NEIGHBOR_V1: u8 = 1;

/// The node's `LUCKY_PEERS`: how many light peers are offered the best commit
/// per GRANDPA round.
pub(crate) const LUCKY_PEERS: u64 = 4;
/// The node's `REBROADCAST_AFTER`. Note it does *not* bound how long a non-lucky
/// peer goes without a commit: the periodic pass only re-sends to peers that
/// already hold the message.
pub(crate) const REBROADCAST_AFTER: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// A V1 neighbor packet, without the two variant bytes in front.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub(crate) struct NeighborPacket {
    pub round: u64,
    pub set_id: u64,
    pub commit_finalized_height: u32,
}

impl NeighborPacket {
    fn to_gossip(self) -> Vec<u8> {
        let mut out = vec![GOSSIP_NEIGHBOR, NEIGHBOR_V1];
        self.encode_to(&mut out);
        out
    }
}

/// The prefix of a `FullCommitMessage`. The compact commit's precommits and
/// signatures follow and are left undecoded.
#[derive(Debug, Decode)]
struct CommitHead {
    round: u64,
    set_id: u64,
    _target_hash: H256,
    target_number: u32,
}

/// What a grandpa notification turned out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GrandpaMessage {
    /// The node's view. Its `commit_finalized_height` is the node's finalized
    /// head, sent to every peer on each commit it imports.
    Neighbor(NeighborPacket),
    /// A commit finalizing `target`. Only lucky light peers get these.
    Commit {
        round: u64,
        set_id: u64,
        target: u32,
    },
    /// A vote. Light peers should never see one.
    Vote,
    /// Anything else, including a message we could not decode.
    Other,
}

/// One synthetic peer's grandpa state.
#[derive(Default)]
pub(crate) struct GrandpaFollower {
    sender: Option<mpsc::Sender<Vec<u8>>>,
    /// The view we last told the node about. `None` until the node's first
    /// neighbor packet reveals which set it is in.
    view: Option<NeighborPacket>,
}

impl GrandpaFollower {
    /// The substream opened. Returns false if it was already open.
    pub(crate) fn on_open(&mut self, sender: mpsc::Sender<Vec<u8>>) -> bool {
        if self.sender.is_some() {
            return false;
        }
        self.sender = Some(sender);
        // smoldot sends its state the moment the substream opens. We can only
        // do that once we know the set, but if we already do, do it now.
        if let Some(view) = self.view {
            self.send(view);
        }
        true
    }

    /// The substream closed. Returns false if it was not open.
    pub(crate) fn on_close(&mut self) -> bool {
        self.sender.take().is_some()
    }

    pub(crate) fn is_open(&self) -> bool {
        self.sender.is_some()
    }

    /// Our current finalized height, once we have one.
    #[cfg(test)]
    pub(crate) fn finalized(&self) -> Option<u32> {
        self.view.map(|v| v.commit_finalized_height)
    }

    /// Decode a notification, update our view like smoldot would, and say what
    /// it was.
    pub(crate) fn on_notification(&mut self, message: &[u8]) -> GrandpaMessage {
        match message.first().copied() {
            Some(GOSSIP_NEIGHBOR) if message.get(1) == Some(&NEIGHBOR_V1) => {
                let Ok(packet) = NeighborPacket::decode(&mut &message[2..]) else {
                    return GrandpaMessage::Other;
                };
                // Enter the node's set the first time we hear of it, and follow
                // it across authority-set changes: a peer whose view is in another
                // set gets no commits at all. We start "synced" at the node's
                // height, as smoldot is right after warp sync. Round 1 is what
                // smoldot sends; the round only matters for votes.
                if self.view.is_none_or(|v| v.set_id < packet.set_id) {
                    let ours = NeighborPacket {
                        round: 1,
                        set_id: packet.set_id,
                        commit_finalized_height: self
                            .view
                            .map(|v| v.commit_finalized_height)
                            .unwrap_or(packet.commit_finalized_height),
                    };
                    self.view = Some(ours);
                    self.send(ours);
                }
                GrandpaMessage::Neighbor(packet)
            }
            Some(GOSSIP_COMMIT) => {
                let Ok(head) = CommitHead::decode(&mut &message[1..]) else {
                    return GrandpaMessage::Other;
                };
                // Finalized: tell the node, as smoldot does after importing a
                // commit. Only ever forwards, so the node never sees a duplicate
                // or a regressing view (both are misbehaviour to it).
                let advanced = self.view.is_none_or(|v| {
                    head.target_number > v.commit_finalized_height || head.set_id > v.set_id
                });
                if advanced {
                    let ours = NeighborPacket {
                        round: 1,
                        set_id: head.set_id.max(self.view.map(|v| v.set_id).unwrap_or(0)),
                        commit_finalized_height: head.target_number,
                    };
                    self.view = Some(ours);
                    self.send(ours);
                }
                GrandpaMessage::Commit {
                    round: head.round,
                    set_id: head.set_id,
                    target: head.target_number,
                }
            }
            Some(GOSSIP_VOTE) => GrandpaMessage::Vote,
            _ => GrandpaMessage::Other,
        }
    }

    fn send(&mut self, view: NeighborPacket) {
        if let Some(sender) = self.sender.as_mut() {
            if let Err(e) = sender.try_send(view.to_gossip()) {
                log::debug!("grandpa: neighbor packet not sent: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_packet(set_id: u64, height: u32) -> Vec<u8> {
        NeighborPacket {
            round: 7,
            set_id,
            commit_finalized_height: height,
        }
        .to_gossip()
    }

    fn commit(set_id: u64, target: u32) -> Vec<u8> {
        let mut out = vec![GOSSIP_COMMIT];
        (3u64, set_id, H256::repeat_byte(1), target).encode_to(&mut out);
        // Empty precommits and auth_data vectors.
        out.extend_from_slice(&[0, 0]);
        out
    }

    #[test]
    fn neighbor_packet_wire_format() {
        // Variant bytes, then round, set_id (u64 LE) and height (u32 LE).
        let bytes = node_packet(0x0102, 0x0a0b0c0d);
        assert_eq!(bytes.len(), 2 + 8 + 8 + 4);
        assert_eq!(&bytes[..2], &[2, 1]);
        assert_eq!(&bytes[2..10], &7u64.to_le_bytes());
        assert_eq!(&bytes[10..18], &0x0102u64.to_le_bytes());
        assert_eq!(&bytes[18..], &0x0a0b0c0du32.to_le_bytes());
    }

    #[test]
    fn follows_the_node_set_and_advances_on_commits() {
        let (tx, mut rx) = mpsc::channel(8);
        let mut f = GrandpaFollower::default();
        assert!(f.on_open(tx));
        // Nothing to say until the node reveals its set.
        assert!(rx.try_next().is_err());

        assert_eq!(
            f.on_notification(&node_packet(40, 1000)),
            GrandpaMessage::Neighbor(NeighborPacket {
                round: 7,
                set_id: 40,
                commit_finalized_height: 1000
            })
        );
        // We answered with set 40, round 1, and the node's height as ours.
        let ours = rx.try_next().unwrap().unwrap();
        assert_eq!(&ours[..2], &[2, 1]);
        let ours = NeighborPacket::decode(&mut &ours[2..]).unwrap();
        assert_eq!(
            (ours.round, ours.set_id, ours.commit_finalized_height),
            (1, 40, 1000)
        );
        assert_eq!(f.finalized(), Some(1000));

        // A new round in the same set changes nothing on our side.
        f.on_notification(&node_packet(40, 1001));
        assert!(rx.try_next().is_err());

        // A commit moves us forward and we say so.
        assert_eq!(
            f.on_notification(&commit(40, 1001)),
            GrandpaMessage::Commit {
                round: 3,
                set_id: 40,
                target: 1001
            }
        );
        let ours = NeighborPacket::decode(&mut &rx.try_next().unwrap().unwrap()[2..]).unwrap();
        assert_eq!(ours.commit_finalized_height, 1001);

        // An older commit (the 5-minute rebroadcast can deliver one) is ignored.
        f.on_notification(&commit(40, 1001));
        assert!(rx.try_next().is_err());

        // The node moves to a new set: we follow, keeping our height.
        f.on_notification(&node_packet(41, 1200));
        let ours = NeighborPacket::decode(&mut &rx.try_next().unwrap().unwrap()[2..]).unwrap();
        assert_eq!((ours.set_id, ours.commit_finalized_height), (41, 1001));
    }

    #[test]
    fn classifies_other_messages() {
        let mut f = GrandpaFollower::default();
        assert_eq!(
            f.on_notification(&[GOSSIP_VOTE, 1, 2, 3]),
            GrandpaMessage::Vote
        );
        assert_eq!(
            f.on_notification(&[GOSSIP_COMMIT, 1]),
            GrandpaMessage::Other
        );
        assert_eq!(f.on_notification(&[]), GrandpaMessage::Other);
        assert_eq!(f.on_notification(&[9]), GrandpaMessage::Other);
    }
}
