use greptime_proto::prometheus::remote::{Sample, WriteRequest};
use greptime_proto::v1::RowInsertRequests;
use crate::{GREPTIME_TIMESTAMP, GREPTIME_VALUE, METRIC_NAME_LABEL, row_writer};
use crate::row_writer::MultiTableData;

pub fn to_grpc_row_insert_requests(request: &WriteRequest) -> (RowInsertRequests, usize) {
    let mut multi_table_data = MultiTableData::new();

    for series in &request.timeseries {
        let table_name = &series
            .labels
            .iter()
            .find(|label| {
                // The metric name is a special label
                label.name == METRIC_NAME_LABEL
            })
            .unwrap()
            .value;

        // The metric name is a special label,
        // num_columns = labels.len() - 1 + 1 (value) + 1 (timestamp)
        let num_columns = series.labels.len() + 1;

        let table_data = multi_table_data.get_or_default_table_data(
            table_name,
            num_columns,
            series.samples.len(),
        );

        // labels
        let kvs = series.labels.iter().filter_map(|label| {
            if label.name == METRIC_NAME_LABEL {
                None
            } else {
                Some((label.name.clone(), label.value.clone()))
            }
        });

        if series.samples.len() == 1 {
            let mut one_row = table_data.alloc_one_row();

            row_writer::write_tags(table_data, kvs, &mut one_row);
            // value
            row_writer::write_f64(
                table_data,
                GREPTIME_VALUE,
                series.samples[0].value,
                &mut one_row,
            );
            // timestamp
            row_writer::write_ts_millis(
                table_data,
                GREPTIME_TIMESTAMP,
                Some(series.samples[0].timestamp),
                &mut one_row,
            );

            table_data.add_row(one_row);
        } else {
            for Sample { value, timestamp } in &series.samples {
                let mut one_row = table_data.alloc_one_row();

                // labels
                let kvs = kvs.clone();
                row_writer::write_tags(table_data, kvs, &mut one_row);
                // value
                row_writer::write_f64(table_data, GREPTIME_VALUE, *value, &mut one_row);
                // timestamp
                row_writer::write_ts_millis(
                    table_data,
                    GREPTIME_TIMESTAMP,
                    Some(*timestamp),
                    &mut one_row,
                );

                table_data.add_row(one_row);
            }
        }
    }
    multi_table_data.into_row_insert_requests()
}
