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
                request.merge(data).unwrap();
            });
        })
        .bench_function("pooled_write_request", |b| {
            b.iter(|| {
                request_pooled.clear();
                let data = data.clone();
                request_pooled.merge(data).unwrap();
            });
        });
    c.benchmark_group("slice").bench_function("bytes", |b| {
        let mut data = data.clone();

        b.iter(|| {
            let mut bytes = data.clone();
            for _ in 0..10000 {
                bytes = bytes.slice(1..);
            }
            black_box(bytes.len());
        });
    });
}

criterion_group!(benches, bench_decode_prom_request);
criterion_main!(benches);
