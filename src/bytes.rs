use bytes::{Buf, Bytes};
use prost::encoding::decode_varint;
use prost::DecodeError;

#[inline(always)]
fn copy_to_bytes(data: &mut Bytes, len: usize) -> Bytes {
    if len == data.remaining() {
        std::mem::replace(data, Bytes::new())
    } else {
        let ret = data.slice(0..len);
        data.advance(len);
        ret
    }
}

/// Reads a variable-length encoded bytes field from `buf` and assign it to `value`.
/// # Safety
/// Callers must ensure `buf` outlives `value`.
#[inline(always)]
pub fn merge_bytes(value: &mut Bytes, buf: &mut Bytes) -> Result<(), DecodeError> {
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
