use crate::repeated_field::{Clear, RepeatedField};
use prost::encoding::{decode_key, decode_varint, WireType};
use prost::DecodeError;
use std::fmt;

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl Clear for Label {
    fn clear(&mut self) {
        self.value.clear();
        self.name.clear();
    }
}

impl prost::Message for Label {
    #[allow(unused_variables)]
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        if !self.name.is_empty() {
            prost::encoding::string::encode(1u32, &self.name, buf);
        }
        if !self.value.is_empty() {
            prost::encoding::string::encode(2u32, &self.value, buf);
        }
    }
    #[allow(unused_variables)]
    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut B,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        B: bytes::Buf,
    {
        const STRUCT_NAME: &str = "Label";
        match tag {
            1u32 => {
                let value = &mut self.name;
                prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "name");
                    error
                })
            }
            2u32 => {
                let value = &mut self.value;
                prost::encoding::string::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "value");
                    error
                })
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }
    #[inline]
    fn encoded_len(&self) -> usize {
        0 + if self.name != "" {
            prost::encoding::string::encoded_len(1u32, &self.name)
        } else {
            0
        } + if self.value != "" {
            prost::encoding::string::encoded_len(2u32, &self.value)
        } else {
            0
        }
    }
    fn clear(&mut self) {
        self.name.clear();
        self.value.clear();
    }
}
impl Default for Label {
    fn default() -> Self {
        Label {
            name: prost::alloc::string::String::new(),
            value: prost::alloc::string::String::new(),
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

impl prost::Message for Sample {
    #[allow(unused_variables)]
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        if self.value != 0f64 {
            prost::encoding::double::encode(1u32, &self.value, buf);
        }
        if self.timestamp != 0i64 {
            prost::encoding::int64::encode(2u32, &self.timestamp, buf);
        }
    }
    #[allow(unused_variables)]
    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut B,
        ctx: prost::encoding::DecodeContext,
    ) -> ::core::result::Result<(), prost::DecodeError>
    where
        B: prost::bytes::Buf,
    {
        const STRUCT_NAME: &str = "Sample";
        match tag {
            1u32 => {
                let value = &mut self.value;
                prost::encoding::double::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "value");
                    error
                })
            }
            2u32 => {
                let value = &mut self.timestamp;
                prost::encoding::int64::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "timestamp");
                    error
                })
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }
    #[inline]
    fn encoded_len(&self) -> usize {
        0 + if self.value != 0f64 {
            prost::encoding::double::encoded_len(1u32, &self.value)
        } else {
            0
        } + if self.timestamp != 0i64 {
            prost::encoding::int64::encoded_len(2u32, &self.timestamp)
        } else {
            0
        }
    }
    fn clear(&mut self) {
        self.value = 0f64;
        self.timestamp = 0i64;
    }
}
impl ::core::default::Default for Sample {
    fn default() -> Self {
        Sample {
            value: 0f64,
            timestamp: 0i64,
        }
    }
}

impl fmt::Debug for Sample {
    fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
        let mut builder = f.debug_struct("Sample");
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
        let builder = {
            let wrapper = {
                #[allow(non_snake_case)]
                fn ScalarWrapper<T>(v: T) -> T {
                    v
                }
                ScalarWrapper(&self.timestamp)
            };
            builder.field("timestamp", &wrapper)
        };
        builder.finish()
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq)]
pub struct Sample {
    pub value: f64,
    /// timestamp is in ms format, see model/timestamp/timestamp.go for
    /// conversion from time.Time to Prometheus timestamp.
    pub timestamp: i64,
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

impl prost::Message for TimeSeries {
    #[allow(unused_variables)]
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: prost::bytes::BufMut,
    {
        for msg in &self.labels {
            prost::encoding::message::encode(1u32, msg, buf);
        }
        for msg in &self.samples {
            prost::encoding::message::encode(2u32, msg, buf);
        }
    }
    #[allow(unused_variables)]
    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut B,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        B: bytes::Buf,
    {
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
                    ctx.clone(),
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
    #[inline]
    fn encoded_len(&self) -> usize {
        0 + prost::encoding::message::encoded_len_repeated(1u32, &self.labels)
            + prost::encoding::message::encoded_len_repeated(2u32, &self.samples)
    }
    fn clear(&mut self) {
        self.labels.clear();
        self.samples.clear();
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

impl prost::Message for WriteRequest {
    #[allow(unused_variables)]
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: prost::bytes::BufMut,
    {
        for msg in &self.timeseries {
            prost::encoding::message::encode(1u32, msg, buf);
        }
    }

    #[allow(unused_variables)]
    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut B,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        B: bytes::Buf,
    {
        const STRUCT_NAME: &str = "WriteRequest";
        match tag {
            1u32 => {
                let series = self.timeseries.push_default();
                let len = decode_varint(buf).map_err(|mut e| {
                    e.push(STRUCT_NAME, "timeseries");
                    e
                })?;
                let remaining = buf.remaining();
                if len > remaining as u64 {
                    return Err(DecodeError::new("buffer underflow"));
                }

                let limit = remaining - len as usize;
                while buf.remaining() > limit {
                    let (tag, wire_type) = decode_key(buf)?;
                    series.merge_field(tag, wire_type, buf, ctx.clone())?;
                }
                Ok(())
            }
            _ => prost::encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }
    #[inline]
    fn encoded_len(&self) -> usize {
        0 + prost::encoding::message::encoded_len_repeated(1u32, &self.timeseries)
    }
    fn clear(&mut self) {
        self.timeseries.clear();
    }
}

impl Default for WriteRequest {
    fn default() -> Self {
        WriteRequest {
            timeseries: Default::default(),
        }
    }
}

impl ::core::fmt::Debug for WriteRequest {
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
        request.merge(data).unwrap();
        assert_eq!(proto_request.timeseries.len(), request.timeseries.len());

        for ts_idx in 0..request.timeseries.len() {
            let proto_ts: &greptime_proto::prometheus::remote::TimeSeries =
                &proto_request.timeseries[ts_idx];
            let ts = &request.timeseries[ts_idx];
            assert_eq!(proto_ts.labels.len(), ts.labels.len());
            assert_eq!(proto_ts.samples.len(), ts.samples.len());

            for idx in 0..proto_ts.labels.len(){
                assert_eq!(&proto_ts.labels[idx].name, &ts.labels[idx].name);
                assert_eq!(&proto_ts.labels[idx].value, &ts.labels[idx].value);
            }

            for idx in 0..proto_ts.samples.len(){
                assert_eq!(&proto_ts.samples[idx].value, &ts.samples[idx].value);
                assert_eq!(&proto_ts.samples[idx].timestamp, &ts.samples[idx].timestamp);
            }
        }
    }
}
