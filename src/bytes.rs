use bytes::{Buf, Bytes};
use prost::encoding::decode_varint;
use prost::DecodeError;
use std::slice;

#[inline(always)]
unsafe fn copy_to_bytes(data: &mut Bytes, len: usize) -> Bytes {
    if len == data.remaining() {
        std::mem::replace(data, Bytes::new())
    } else {
        let ret = split_to(data, len);
        data.advance(len);
        ret
    }
}

/// Similar to `Bytes::split_to`, but directly operates on underlying memory region.
/// # Safety
/// This function is safe as long as `data` is backed by a consecutive region of memory,
/// for example `Vec<u8>` or `&[u8]`, and caller must ensure that `buf` outlives
/// the `Bytes` returned.
#[inline(always)]
pub unsafe fn split_to(buf: &Bytes, end: usize) -> Bytes {
    let len = buf.len();
    assert!(
        end <= len,
        "range end out of bounds: {:?} <= {:?}",
        end,
        len,
    );

    if end == 0 {
        return Bytes::new();
    }

    let ptr = buf.as_ptr();
    // `Bytes::drop` does nothing when it's built via `from_static`.
    Bytes::from_static(unsafe { slice::from_raw_parts(ptr, end) })
}

/// Reads a variable-length encoded bytes field from `buf` and assign it to `value`.
/// # Safety
/// Callers must ensure `buf` outlives `value`.
#[inline(always)]
pub unsafe fn merge_bytes(value: &mut Bytes, buf: &mut Bytes) -> Result<(), DecodeError> {
    let len = decode_varint(buf)?;
    if len > buf.remaining() as u64 {
        return Err(DecodeError::new(format!(
            "buffer underflow, len: {}, remaining: {}",
            len,
            buf.remaining()
        )));
    }

    *value = copy_to_bytes(buf, len as usize);
    Ok(())
}
