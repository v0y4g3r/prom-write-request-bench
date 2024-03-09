mod prom_row_builder;
pub mod prom_write_request;
#[allow(clippy::all)]
pub mod repeated_field;
mod row_writer;
pub mod write_request;

pub const METRIC_NAME_LABEL: &str = "__name__";
pub const METRIC_NAME_LABEL_BYTES: &[u8] = b"__name__";
pub const GREPTIME_TIMESTAMP: &str = "ts";
pub const GREPTIME_VALUE: &str = "val";

#[cfg(test)]
mod tests {
    use crate::prom_write_request::PromWriteRequest;
    use crate::write_request::to_grpc_row_insert_requests;
    use bytes::Bytes;
    use greptime_proto::prometheus::remote::WriteRequest;
    use prost::Message;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn test_decode() {
        let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("assets");
        d.push("1709380533560664458.data");

        let data = Bytes::from(std::fs::read(d).unwrap());

        let mut expected = WriteRequest::default();
        expected.merge(data.clone()).unwrap();

        let (expected, expected_samples) = to_grpc_row_insert_requests(&expected);

        let mut result = PromWriteRequest::default();
        result.merge(data).unwrap();
        let (res, samples_decoded) = result.as_row_insert_requests();
        assert_eq!(expected_samples, samples_decoded);

        let mut expected_table_rows = HashMap::new();
        let mut expected_schemas: HashMap<String, HashSet<(String, i32, i32)>> = HashMap::new();
        for row_insert in expected.inserts {
            let rows = row_insert.rows.unwrap();
            expected_schemas.insert(
                row_insert.table_name.clone(),
                rows.schema
                    .iter()
                    .map(|c| (c.column_name.clone(), c.datatype, c.semantic_type))
                    .collect(),
            );
            let table_rows: &mut usize = expected_table_rows
                .entry(row_insert.table_name)
                .or_default();
            *table_rows += rows.rows.len();
        }

        let mut decoded_table_rows = HashMap::with_capacity(expected_table_rows.len());
        let mut decoded_schemas: HashMap<String, HashSet<(String, i32, i32)>> =
            HashMap::with_capacity(expected_schemas.len());
        for insert in res.inserts {
            let rows = insert.rows.unwrap();
            decoded_schemas.insert(
                insert.table_name.clone(),
                rows.schema
                    .iter()
                    .map(|c| (c.column_name.clone(), c.datatype, c.semantic_type))
                    .collect(),
            );
            let table_rows: &mut usize = decoded_table_rows.entry(insert.table_name).or_default();
            *table_rows += rows.rows.len();
        }

        assert_eq!(expected_table_rows.len(), decoded_table_rows.len());
        assert_eq!(expected_schemas, decoded_schemas);
    }
}
