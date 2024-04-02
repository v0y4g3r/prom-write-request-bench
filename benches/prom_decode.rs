use bench_prom::bytes::split_to;
use bench_prom::prom_write_request::WriteRequest;
use bench_prom::repeated_field::Clear;
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_decode_prom_request(c: &mut Criterion) {
    let mut d = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    d.push("assets");
    d.push("1709380533560664458.data");
    let data = Bytes::from(std::fs::read(d).unwrap());
    let mut bump_alloc_once = bumpalo::Bump::new();
    let mut bump_alloc = bumpalo::Bump::new();
    let mut request_pooled = WriteRequest::new_in(&bump_alloc);
    c.benchmark_group("decode")
        .bench_function("write_request", |b| {
            b.iter(|| {
                let mut request = WriteRequest::new_in(&bump_alloc_once);
                let data = data.clone();
                unsafe {
                    request.merge(data).unwrap();
                }
                drop(request);
                bump_alloc_once.reset();
            });
        })
        .bench_function("pooled_write_request", |b| {
            let mut bump_alloc = bumpalo::Bump::new();
            let mut request_pooled = WriteRequest::new_in(&bump_alloc);
            b.iter(|| {
                request_pooled.clear();
                let data = data.clone();
                unsafe {
                    request_pooled.merge(data).unwrap();
                }
            });
            drop(request_pooled);
            bump_alloc.reset();
        });
    c.benchmark_group("slice")
        .bench_function("bytes", |b| {
            let data = data.clone();
            b.iter(move || {
                let mut bytes = data.clone();
                for _ in 0..10000 {
                    bytes = black_box(bytes.slice(0..1));
                }
            });
        })
        .bench_function("split_to", |b| {
            let data = data.clone();
            b.iter(|| {
                let mut bytes = data.clone();
                for _ in 0..10000 {
                    bytes = black_box(unsafe { split_to(&bytes, 1) });
                }
            });
        })
        .bench_function("slice", |b| {
            let data = data.clone();
            let mut slice = data.as_ref();
            b.iter(move || {
                for _ in 0..10000 {
                    slice = black_box(&slice[..1]);
                }
            });
        });
}

criterion_group!(benches, bench_decode_prom_request);
criterion_main!(benches);
