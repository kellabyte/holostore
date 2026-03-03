//! Packed unary batch encode/decode utilities for transport v2 RPCs.
//!
//! Purpose:
//! - Provide a compact batch representation for unary RPCs that avoids repeated
//!   protobuf batch-wrapper overhead.
//!
//! Design:
//! - Encode each item protobuf payload back-to-back in one contiguous `frame`.
//! - Store each item's exclusive end offset in `end_offsets`.
//! - Validate offsets strictly on decode to reject malformed payloads early.
//!
//! Inputs:
//! - Slices of protobuf messages implementing `pilota::pb::Message`.
//!
//! Outputs:
//! - Packed `(frame, end_offsets)` tuples and decoded protobuf item vectors.

use std::fmt;

use bytes::Bytes;
use pilota::pb::encoding::EncodeLengthContext;
use pilota::pb::Message;
use pilota::LinkedBytes;

/// Error returned by packed-batch encode/decode helpers.
///
/// Purpose:
/// - Report malformed packed batches and protobuf decode failures with context.
///
/// Design:
/// - Keep the error payload small and deterministic for transport logging.
///
/// Inputs:
/// - Validation failures while encoding offsets or decoding item segments.
///
/// Outputs:
/// - Structured error details consumable by caller-specific status mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackedBatchError {
    OffsetOverflow {
        index: usize,
        end: usize,
    },
    OffsetNotStrictlyIncreasing {
        index: usize,
        previous: u32,
        current: u32,
    },
    OffsetOutOfBounds {
        index: usize,
        end: u32,
        frame_len: usize,
    },
    FrameLengthMismatch {
        consumed: usize,
        frame_len: usize,
    },
    DecodeItem {
        index: usize,
        message: String,
    },
}

impl fmt::Display for PackedBatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OffsetOverflow { index, end } => {
                write!(f, "packed batch item #{index} end offset overflows u32: {end}")
            }
            Self::OffsetNotStrictlyIncreasing {
                index,
                previous,
                current,
            } => write!(
                f,
                "packed batch item #{index} end offset not strictly increasing: prev={previous}, cur={current}"
            ),
            Self::OffsetOutOfBounds {
                index,
                end,
                frame_len,
            } => write!(
                f,
                "packed batch item #{index} end offset out of bounds: end={end}, frame_len={frame_len}"
            ),
            Self::FrameLengthMismatch { consumed, frame_len } => write!(
                f,
                "packed batch offsets do not consume full frame: consumed={consumed}, frame_len={frame_len}"
            ),
            Self::DecodeItem { index, message } => {
                write!(f, "packed batch decode failed for item #{index}: {message}")
            }
        }
    }
}

impl std::error::Error for PackedBatchError {}

/// Builder for packed unary batch payloads.
///
/// Purpose:
/// - Encode message items into one frame while tracking item boundaries.
///
/// Design:
/// - Uses `LinkedBytes` so item encoding appends without allocating one `Vec`
///   per item.
/// - Tracks running frame length and records each item's end offset.
///
/// Inputs:
/// - Protobuf messages implementing `pilota::pb::Message`.
///
/// Outputs:
/// - Packed bytes frame and end-offset index.
pub struct PackedBatchBuilder {
    frame: LinkedBytes,
    end_offsets: Vec<u32>,
    frame_len: usize,
}

impl PackedBatchBuilder {
    /// Create a new builder with capacity hints.
    ///
    /// Purpose:
    /// - Reduce reallocations for expected item and frame sizes.
    ///
    /// Design:
    /// - Hints are best-effort and never affect correctness.
    ///
    /// Inputs:
    /// - `items_hint`: expected number of items.
    /// - `frame_bytes_hint`: expected encoded payload bytes.
    ///
    /// Outputs:
    /// - Empty builder ready for `push`.
    pub fn with_capacity(items_hint: usize, frame_bytes_hint: usize) -> Self {
        Self {
            frame: LinkedBytes::with_capacity(frame_bytes_hint.max(1)),
            end_offsets: Vec::with_capacity(items_hint),
            frame_len: 0,
        }
    }

    /// Append one protobuf message to the packed frame.
    ///
    /// Purpose:
    /// - Encode one item and record its end boundary.
    ///
    /// Design:
    /// - Computes encoded length first to update offsets deterministically.
    /// - Encodes directly into the linked frame buffer.
    ///
    /// Inputs:
    /// - `item`: protobuf message to append.
    ///
    /// Outputs:
    /// - Updated internal frame and end-offset index.
    pub fn push<M: Message>(&mut self, item: &M) -> Result<(), PackedBatchError> {
        let mut ctx = EncodeLengthContext::default();
        let item_len = item.encoded_len(&mut ctx);
        let new_len = self.frame_len.saturating_add(item_len);
        let end_offset = u32::try_from(new_len).map_err(|_| PackedBatchError::OffsetOverflow {
            index: self.end_offsets.len(),
            end: new_len,
        })?;

        // Encode directly into the shared linked frame. This intentionally does
        // not build per-item temporary vectors.
        item.encode_raw(&mut self.frame);
        self.frame_len = new_len;
        self.end_offsets.push(end_offset);
        Ok(())
    }

    /// Finish building and return packed frame parts.
    ///
    /// Purpose:
    /// - Expose the final packed payload for RPC transport.
    ///
    /// Design:
    /// - Concatenates linked chunks into one immutable `Bytes` frame.
    ///
    /// Inputs:
    /// - Builder with zero or more encoded items.
    ///
    /// Outputs:
    /// - `(frame, end_offsets)` pair for packed unary RPC messages.
    pub fn finish(self) -> (Bytes, Vec<u32>) {
        (self.frame.concat().into(), self.end_offsets)
    }
}

/// Encode items into packed frame parts.
///
/// Purpose:
/// - Convenience wrapper for one-shot packed encoding.
///
/// Design:
/// - Uses `PackedBatchBuilder` with conservative frame-size hint.
///
/// Inputs:
/// - `items`: protobuf messages to encode in order.
///
/// Outputs:
/// - `(frame, end_offsets)` pair preserving item order.
pub fn encode_items_to_parts<M: Message>(
    items: &[M],
) -> Result<(Bytes, Vec<u32>), PackedBatchError> {
    let mut builder =
        PackedBatchBuilder::with_capacity(items.len(), items.len().saturating_mul(64));
    // Keep wire ordering identical to caller order; this does not sort or
    // deduplicate items, because caller order carries response fan-out semantics.
    for item in items {
        builder.push(item)?;
    }
    Ok(builder.finish())
}

/// Decode packed frame parts into protobuf messages.
///
/// Purpose:
/// - Reconstruct ordered message items from packed unary batch payload.
///
/// Design:
/// - Enforces strictly increasing offsets.
/// - Rejects out-of-bounds segments.
/// - Requires offsets to consume the full frame to detect trailing corruption.
///
/// Inputs:
/// - `frame`: concatenated item payload bytes.
/// - `end_offsets`: exclusive end offset of each item.
///
/// Outputs:
/// - Decoded messages in original order.
pub fn decode_items_from_parts<M: Message + Default>(
    frame: Bytes,
    end_offsets: &[u32],
) -> Result<Vec<M>, PackedBatchError> {
    if end_offsets.is_empty() {
        if frame.is_empty() {
            return Ok(Vec::new());
        }
        return Err(PackedBatchError::FrameLengthMismatch {
            consumed: 0,
            frame_len: frame.len(),
        });
    }

    let mut items = Vec::with_capacity(end_offsets.len());
    let mut start = 0usize;

    // Validate and decode each indexed segment exactly once. This loop
    // intentionally does not attempt recovery from malformed offsets.
    for (index, end) in end_offsets.iter().copied().enumerate() {
        let end_usize = end as usize;
        if end <= start as u32 {
            return Err(PackedBatchError::OffsetNotStrictlyIncreasing {
                index,
                previous: start as u32,
                current: end,
            });
        }
        if end_usize > frame.len() {
            return Err(PackedBatchError::OffsetOutOfBounds {
                index,
                end,
                frame_len: frame.len(),
            });
        }
        let item = M::decode(frame.slice(start..end_usize)).map_err(|err| {
            PackedBatchError::DecodeItem {
                index,
                message: err.to_string(),
            }
        })?;
        items.push(item);
        start = end_usize;
    }

    if start != frame.len() {
        return Err(PackedBatchError::FrameLengthMismatch {
            consumed: start,
            frame_len: frame.len(),
        });
    }

    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pilota::pb::encoding::EncodeLengthContext;

    use crate::volo_gen::holo_store::rpc;

    /// Validate normal-path packed round-trip for request messages.
    ///
    /// Purpose:
    /// - Ensure encode/decode preserves order and payload bytes.
    ///
    /// Design:
    /// - Encode two KV get requests and decode them back.
    ///
    /// Inputs:
    /// - Two `KvGetRequest` messages with distinct keys.
    ///
    /// Outputs:
    /// - Decoded requests match original vector exactly.
    #[test]
    fn packed_roundtrip_kv_get_requests() {
        let requests = vec![
            rpc::KvGetRequest {
                key: Bytes::from_static(b"a"),
            },
            rpc::KvGetRequest {
                key: Bytes::from_static(b"b"),
            },
        ];
        let (frame, end_offsets) = encode_items_to_parts(&requests).expect("encode packed batch");
        let decoded = decode_items_from_parts::<rpc::KvGetRequest>(frame, &end_offsets)
            .expect("decode packed batch");
        assert_eq!(decoded, requests);
    }

    /// Validate edge-path handling for empty packed batches.
    ///
    /// Purpose:
    /// - Ensure empty batches decode without allocating item payloads.
    ///
    /// Design:
    /// - Decode an empty frame with no offsets.
    ///
    /// Inputs:
    /// - Empty frame and empty offsets.
    ///
    /// Outputs:
    /// - Empty decoded item vector.
    #[test]
    fn packed_decode_empty_batch() {
        let decoded =
            decode_items_from_parts::<rpc::KvGetRequest>(Bytes::new(), &[]).expect("empty decode");
        assert!(decoded.is_empty());
    }

    /// Validate failure-path rejection of non-monotonic offsets.
    ///
    /// Purpose:
    /// - Ensure malformed boundaries are rejected.
    ///
    /// Design:
    /// - Provide offsets where a later offset moves backwards.
    ///
    /// Inputs:
    /// - Dummy frame and invalid offset sequence.
    ///
    /// Outputs:
    /// - `OffsetNotStrictlyIncreasing` error.
    #[test]
    fn packed_decode_rejects_non_monotonic_offsets() {
        let requests = vec![
            rpc::KvGetRequest {
                key: Bytes::from_static(b"k0"),
            },
            rpc::KvGetRequest {
                key: Bytes::from_static(b"k1"),
            },
        ];
        let (frame, end_offsets) = encode_items_to_parts(&requests).expect("encode packed batch");
        let err = decode_items_from_parts::<rpc::KvGetRequest>(
            frame,
            &[end_offsets[0], end_offsets[0] - 1, end_offsets[1]],
        )
        .expect_err("expected non-monotonic offset failure");
        assert!(matches!(
            err,
            PackedBatchError::OffsetNotStrictlyIncreasing { .. }
        ));
    }

    /// Validate failure-path rejection of trailing unindexed frame bytes.
    ///
    /// Purpose:
    /// - Ensure offsets must cover the full frame exactly.
    ///
    /// Design:
    /// - Encode one request then drop one trailing byte from indexed length.
    ///
    /// Inputs:
    /// - One valid message frame with mismatched `end_offsets`.
    ///
    /// Outputs:
    /// - `FrameLengthMismatch` error.
    #[test]
    fn packed_decode_rejects_trailing_unindexed_bytes() {
        let requests = vec![rpc::KvGetRequest {
            key: Bytes::from_static(b"z"),
        }];
        let (frame, end_offsets) = encode_items_to_parts(&requests).expect("encode packed batch");
        let mut frame_with_trailing_byte = frame.to_vec();
        frame_with_trailing_byte.push(0u8);

        let err = decode_items_from_parts::<rpc::KvGetRequest>(
            Bytes::from(frame_with_trailing_byte),
            &end_offsets,
        )
        .expect_err("expected trailing byte rejection");
        assert!(matches!(err, PackedBatchError::FrameLengthMismatch { .. }));
    }

    /// Provide measurable rationale that packed frames avoid per-item wrapper
    /// bytes in the frame payload.
    ///
    /// Purpose:
    /// - Validate that packed frame bytes are exactly the concatenation of item
    ///   encodings, with no extra delimiters.
    ///
    /// Design:
    /// - Compare packed frame against manual concatenation of encoded item bytes.
    ///
    /// Inputs:
    /// - Four `KvGetRequest` items.
    ///
    /// Outputs:
    /// - Packed frame equals exact concatenation of item encodings.
    #[test]
    fn packed_frame_is_exact_item_concat() {
        let requests = vec![
            rpc::KvGetRequest {
                key: Bytes::from_static(b"k0"),
            },
            rpc::KvGetRequest {
                key: Bytes::from_static(b"k1"),
            },
            rpc::KvGetRequest {
                key: Bytes::from_static(b"k2"),
            },
            rpc::KvGetRequest {
                key: Bytes::from_static(b"k3"),
            },
        ];

        let (frame, end_offsets) = encode_items_to_parts(&requests).expect("encode packed");
        let mut manual = Vec::new();
        for request in &requests {
            let mut ctx = EncodeLengthContext::default();
            let encoded = request.encode_to_vec(&mut ctx);
            manual.extend_from_slice(&encoded);
        }

        assert_eq!(frame.as_ref(), manual.as_slice());
        assert_eq!(end_offsets.len(), requests.len());
        assert_eq!(
            end_offsets.last().copied(),
            Some(frame.len() as u32),
            "last offset should point at final frame byte"
        );
    }
}
