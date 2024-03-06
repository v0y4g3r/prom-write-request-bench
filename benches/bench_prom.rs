use criterion::criterion_main;

mod prom_decode;

criterion_main! {
    prom_decode::benches
}
