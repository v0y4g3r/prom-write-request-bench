use crate::bytes::merge_bytes;
use crate::repeated_field::{Clear, RepeatedField};
use bytes::{Buf, Bytes};
use greptime_proto::prometheus::remote::Sample;
use prost::encoding::{decode_key, decode_varint, DecodeContext, WireType};
use prost::DecodeError;
use std::fmt;

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq)]
pub struct Label {
    pub name: Bytes,
    pub value: Bytes,
}

impl Clear for Label {
    fn clear(&mut self) {
        self.value.clear();
        self.name.clear();
    }
}

impl Label {
    #[allow(unused_variables)]
    unsafe fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut Bytes,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        const STRUCT_NAME: &str = "Label";
        match tag {
            1u32 => {
                let value = &mut self.name;
                merge_bytes(value, buf).map_err(|mut error| {
                    error.push(STRUCT_NAME, "name");
                    error
                })
            }
            2u32 => {
                let value = &mut self.value;
                merge_bytes(value, buf).map_err(|mut error| {
                    error.push(STRUCT_NAME, "value");
                    error
                })
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }
}

impl Default for Label {
    fn default() -> Self {
        Label {
            name: Bytes::new(),
            value: Bytes::new(),
        }
    }
}

impl ::core::fmt::Debug for Label {
    fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
        let mut builder = f.debug_struct("Label");
        let builder = {
            let wrapper = {
                #[allow(non_snake_case)]
                fn ScalarWrapper<T>(v: T) -> T {
                    v
                }
                ScalarWrapper(&self.name)
            };
            builder.field("name", &wrapper)
        };
        let builder = {
            let wrapper = {
                #[allow(non_snake_case)]
                fn ScalarWrapper<T>(v: T) -> T {
                    v
                }
                ScalarWrapper(&self.value)
            };
            builder.field("value", &wrapper)
        };
        builder.finish()
    }
}

impl Clear for Sample {
    fn clear(&mut self) {
        self.value = 0.0;
        self.timestamp = 0;
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq)]
pub struct TimeSeries {
    /// For a timeseries to be valid, and for the samples and exemplars
    /// to be ingested by the remote system properly, the labels field is required.
    pub labels: RepeatedField<Label>,
    pub samples: RepeatedField<Sample>,
}

impl Clear for TimeSeries {
    fn clear(&mut self) {
        self.labels.clear();
        self.samples.clear();
    }
}

impl TimeSeries {
    #[allow(unused_variables)]
    unsafe fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut Bytes,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        const STRUCT_NAME: &str = "TimeSeries";
        match tag {
            1u32 => {
                // decode labels
                let label = self.labels.push_default();

                let len = decode_varint(buf).map_err(|mut error| {
                    error.push(STRUCT_NAME, "labels");
                    error
                })?;
                let remaining = buf.remaining();
                if len > remaining as u64 {
                    return Err(DecodeError::new("buffer underflow"));
                }

                let limit = remaining - len as usize;
                while buf.remaining() > limit {
                    let (tag, wire_type) = decode_key(buf)?;
                    label.merge_field(tag, wire_type, buf, ctx.clone())?;
                }
                if buf.remaining() != limit {
                    return Err(DecodeError::new("delimited length exceeded"));
                }
                Ok(())
            }
            2u32 => {
                let sample = self.samples.push_default();
                prost::encoding::message::merge(
                    WireType::LengthDelimited,
                    sample,
                    buf,
                    Default::default(),
                )
                .map_err(|mut error| {
                    error.push(STRUCT_NAME, "samples");
                    error
                })?;
                Ok(())
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }
}
impl ::core::default::Default for TimeSeries {
    fn default() -> Self {
        TimeSeries {
            labels: ::core::default::Default::default(),
            samples: ::core::default::Default::default(),
        }
    }
}
impl fmt::Debug for TimeSeries {
    fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
        let mut builder = f.debug_struct("TimeSeries");
        let builder = {
            let wrapper = &self.labels;
            builder.field("labels", &wrapper)
        };
        let builder = {
            let wrapper = &self.samples;
            builder.field("samples", &wrapper)
        };
        builder.finish()
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq)]
pub struct WriteRequest {
    pub timeseries: RepeatedField<TimeSeries>,
}

impl Clear for WriteRequest {
    fn clear(&mut self) {
        self.timeseries.clear();
    }
}

impl WriteRequest {
    // Safety: caller must ensure `buf` outlive current [WriteRequest] instance.
    pub unsafe fn merge(&mut self, mut buf: Bytes) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "PromWriteRequest";
        let ctx = DecodeContext::default();
        while buf.has_remaining() {
            let (tag, wire_type) = decode_key(&mut buf)?;
            assert_eq!(WireType::LengthDelimited, wire_type);
            match tag {
                1u32 => {
                    let series = self.timeseries.push_default();
                    // decode TimeSeries
                    let len = decode_varint(&mut buf).map_err(|mut e| {
                        e.push(STRUCT_NAME, "timeseries");
                        e
                    })?;
                    let remaining = buf.remaining();
                    if len > remaining as u64 {
                        return Err(DecodeError::new("buffer underflow"));
                    }

                    let limit = remaining - len as usize;
                    while buf.remaining() > limit {
                        let (tag, wire_type) = decode_key(&mut buf)?;
                        series.merge_field(tag, wire_type, &mut buf, ctx.clone())?;
                    }
                }
                3u32 => {
                    // todo(hl): metadata are skipped.
                    prost::encoding::skip_field(wire_type, tag, &mut buf, Default::default())?;
                }
                _ => prost::encoding::skip_field(wire_type, tag, &mut buf, Default::default())?,
            }
        }
        Ok(())
    }
}

impl Default for WriteRequest {
    fn default() -> Self {
        WriteRequest {
            timeseries: Default::default(),
        }
    }
}

impl fmt::Debug for WriteRequest {
    fn fmt(&self, f: &mut ::core::fmt::Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("WriteRequest");
        let builder = {
            let wrapper = &self.timeseries;
            builder.field("timeseries", &wrapper)
        };
        builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::prom_write_request::WriteRequest;
    use bytes::Bytes;
    use prost::Message;

    #[test]
    fn test_decode_correctness() {
        let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("assets");
        d.push("1709380533560664458.data");
        let data = Bytes::from(std::fs::read(d).unwrap());
        let mut proto_request = greptime_proto::prometheus::remote::WriteRequest::default();
        proto_request.merge(data.clone()).unwrap();

        let mut request = WriteRequest::default();

        // Safety: data is dropped at the end of function.
        unsafe {
            request.merge(data.clone()).unwrap();
        }
        assert_eq!(proto_request.timeseries.len(), request.timeseries.len());

        for ts_idx in 0..request.timeseries.len() {
            let proto_ts: &greptime_proto::prometheus::remote::TimeSeries =
                &proto_request.timeseries[ts_idx];
            let ts = &request.timeseries[ts_idx];
            assert_eq!(proto_ts.labels.len(), ts.labels.len());
            assert_eq!(proto_ts.samples.len(), ts.samples.len());

            for idx in 0..proto_ts.labels.len() {
                assert_eq!(&proto_ts.labels[idx].name, &ts.labels[idx].name);
                assert_eq!(&proto_ts.labels[idx].value, &ts.labels[idx].value);
            }

            for idx in 0..proto_ts.samples.len() {
                assert_eq!(&proto_ts.samples[idx].value, &ts.samples[idx].value);
                assert_eq!(&proto_ts.samples[idx].timestamp, &ts.samples[idx].timestamp);
            }
        }

        // finally drop data to ensure `WriteRequest` will not access invalid memory address.
        drop(data);
    }
}
