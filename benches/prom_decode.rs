use bench_prom::prom_write_request::WriteRequest;
use bench_prom::repeated_field::Clear;
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_decode_prom_request(c: &mut Criterion) {
    let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("assets");
    d.push("1709380533560664458.data");
    let data = Bytes::from(std::fs::read(d).unwrap());
    let mut request_pooled = WriteRequest::default();
    c.benchmark_group("decode")
        .bench_function("write_request", |b| {
            b.iter(|| {
                let mut request = WriteRequest::default();
                let data = data.clone();
                unsafe {
                    request.merge(data).unwrap();
                }
            });
        })
        .bench_function("pooled_write_request", |b| {
            b.iter(|| {
                request_pooled.clear();
                let data = data.clone();
                unsafe {
                    request_pooled.merge(data).unwrap();
                }
                black_box(&request_pooled);
            });
        });
}

criterion_group!(benches, bench_decode_prom_request);
criterion_main!(benches);
