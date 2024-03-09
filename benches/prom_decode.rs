use bench_prom::prom_write_request::{copy_to_bytes, PromWriteRequest};
use bench_prom::repeated_field::Clear;
use bench_prom::write_request::to_grpc_row_insert_requests;
use bytes::{ Bytes};
use criterion::{criterion_group, criterion_main, Criterion};
use greptime_proto::prometheus::remote::WriteRequest;
use prost::Message;

fn bench_decode_prom_request(c: &mut Criterion) {
    let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("assets");
    d.push("1709380533560664458.data");

    let data = Bytes::from(std::fs::read(d).unwrap());

    let mut request = WriteRequest::default();
    let mut prom_request = PromWriteRequest::default();

    c.benchmark_group("decode")
        .bench_function("write_request", |b| {
            b.iter(|| {
                request.clear();
                let data = data.clone();
                request.merge(data).unwrap();
                to_grpc_row_insert_requests(&request);
            });
        })
        .bench_function("prom_write_request", |b| {
            b.iter(|| {
                let data = data.clone();
                prom_request.clear();
                prom_request.merge(data).unwrap();
                prom_request.as_row_insert_requests();
            });
        })
        .bench_function("slice_bytes", |b| {
            b.iter(|| {
                let mut data = data.clone();
                for _ in 0..128766 {
                    let _ = copy_to_bytes(&mut data, 1);
                }
            });
        });
}

criterion_group!(benches, bench_decode_prom_request);
criterion_main!(benches);
