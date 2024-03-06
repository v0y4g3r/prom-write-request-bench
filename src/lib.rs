pub mod write_request;
mod row_writer;
pub mod prom_write_request;
mod prom_row_builder;
#[allow(clippy::all)]
pub mod repeated_field;

pub const METRIC_NAME_LABEL: &str = "__name__";
pub const METRIC_NAME_LABEL_BYTES: &[u8] = b"__name__";
pub const GREPTIME_TIMESTAMP: &str = "ts";
pub const GREPTIME_VALUE: &str = "val";

