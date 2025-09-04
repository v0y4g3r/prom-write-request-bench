use crate::prom_write_request::RawBytes;
use bytes::{Buf, Bytes};
use prost::DecodeError;
use std::slice;

/// Reads a variable-length encoded bytes field from `buf` and assign it to `value`.
/// # Safety
/// Callers must ensure `buf` outlives `value`.
#[inline(always)]
pub unsafe fn merge_bytes(value: &mut RawBytes, buf: &mut Bytes) -> Result<(), DecodeError> {
    let len = decode_varint_unsafe(buf);
    if len > buf.remaining() as u64 {
        return Err(DecodeError::new(format!(
            "buffer underflow, len: {}, remaining: {}",
            len,
            buf.remaining()
        )));
    }

    *value = unsafe { slice::from_raw_parts(buf.as_ptr(), len as usize) };
    buf.advance(len as usize);
    Ok(())
}

#[inline(always)]
pub fn decode_varint_unsafe(data: &mut Bytes) -> u64 {
    let ptr = data.as_ptr();
    let b = unsafe { ptr.cast::<u64>().read_unaligned() };
    let msbs = !b & !0x7f7f7f7f7f7f7f7f;
    let len = msbs.trailing_zeros() + 1;
    let varint_part = b & (msbs ^ msbs.wrapping_sub(1));
    data.advance((len / 8) as usize);
    varint_part
}
