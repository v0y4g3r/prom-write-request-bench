use crate::prom_write_request::RawBytes;
use bytes::{Buf, Bytes};
use prost::encoding::decode_varint;
use prost::DecodeError;
use std::slice;

/// Reads a variable-length encoded bytes field from `buf` and assign it to `value`.
/// # Safety
/// Callers must ensure `buf` outlives `value`.
#[inline(always)]
pub unsafe fn merge_bytes(value: &mut RawBytes, buf: &mut Bytes) -> Result<(), DecodeError> {
    let len = decode_varint(buf)?;
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
