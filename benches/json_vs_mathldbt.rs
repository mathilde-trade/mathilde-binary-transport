use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use mathilde_binary_transport::batch::{ColumnData, ColumnarBatch, ValidityBitmap};
use mathilde_binary_transport::codec::mathldbt_v1::{
    MathldbtV1DecodeWorkspace, MathldbtV1EncodeWorkspace, decode_mathldbt_v1_with_workspace,
    encode_mathldbt_v1_into_with_workspace,
};
use mathilde_binary_transport::schema::{ColumnarField, ColumnarSchema, ColumnarType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize)]
struct BarRowBorrowed<'a> {
    pair: &'a str,
    tf: &'a str,
    e_ms: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct BarRowOwned {
    pair: String,
    tf: String,
    e_ms: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

fn make_bars_like_batch(rows: usize) -> ColumnarBatch {
    let schema = ColumnarSchema::new(vec![
        ColumnarField {
            name: Some("pair".to_string()),
            ty: ColumnarType::Utf8,
        },
        ColumnarField {
            name: Some("tf".to_string()),
            ty: ColumnarType::Utf8,
        },
        ColumnarField {
            name: Some("e_ms".to_string()),
            ty: ColumnarType::I64,
        },
        ColumnarField {
            name: Some("open".to_string()),
            ty: ColumnarType::F64,
        },
        ColumnarField {
            name: Some("high".to_string()),
            ty: ColumnarType::F64,
        },
        ColumnarField {
            name: Some("low".to_string()),
            ty: ColumnarType::F64,
        },
        ColumnarField {
            name: Some("close".to_string()),
            ty: ColumnarType::F64,
        },
        ColumnarField {
            name: Some("volume".to_string()),
            ty: ColumnarType::F64,
        },
    ])
    .unwrap();

    let validity_all = ValidityBitmap::new_all_valid(rows).unwrap();

    fn make_repeated_utf8(
        rows: usize,
        a: &'static [u8],
        b: &'static [u8],
    ) -> (ValidityBitmap, Vec<u32>, Vec<u8>) {
        let validity = ValidityBitmap::new_all_valid(rows).unwrap();
        let mut offsets = vec![0u32; rows + 1];
        let mut data = Vec::new();
        for i in 0..rows {
            let v = if (i & 1) == 0 { a } else { b };
            data.extend_from_slice(v);
            offsets[i + 1] = data.len() as u32;
        }
        (validity, offsets, data)
    }

    let (pair_validity, pair_offsets, pair_data) = make_repeated_utf8(rows, b"BTCUSDT", b"ETHUSDT");
    let (tf_validity, tf_offsets, tf_data) = make_repeated_utf8(rows, b"1m", b"1m");

    let mut e_vals = Vec::with_capacity(rows);
    for i in 0..rows {
        e_vals.push(1_700_000_000_000i64 + i as i64 * 60_000);
    }

    let mut open_bits = Vec::with_capacity(rows);
    let mut high_bits = Vec::with_capacity(rows);
    let mut low_bits = Vec::with_capacity(rows);
    let mut close_bits = Vec::with_capacity(rows);
    let mut vol_bits = Vec::with_capacity(rows);

    for i in 0..rows {
        let base = 10_000.0 + i as f64 * 0.25;
        open_bits.push((base + 0.10).to_bits());
        high_bits.push((base + 0.20).to_bits());
        low_bits.push((base + 0.05).to_bits());
        close_bits.push((base + 0.15).to_bits());
        vol_bits.push((100.0 + (i % 10) as f64).to_bits());
    }

    ColumnarBatch::new(
        schema,
        rows,
        vec![
            ColumnData::Var {
                ty: ColumnarType::Utf8,
                validity: pair_validity,
                offsets: pair_offsets,
                data: pair_data,
            },
            ColumnData::Var {
                ty: ColumnarType::Utf8,
                validity: tf_validity,
                offsets: tf_offsets,
                data: tf_data,
            },
            ColumnData::FixedI64 {
                validity: validity_all.clone(),
                values: e_vals,
            },
            ColumnData::FixedF64Bits {
                validity: validity_all.clone(),
                values: open_bits,
            },
            ColumnData::FixedF64Bits {
                validity: validity_all.clone(),
                values: high_bits,
            },
            ColumnData::FixedF64Bits {
                validity: validity_all.clone(),
                values: low_bits,
            },
            ColumnData::FixedF64Bits {
                validity: validity_all.clone(),
                values: close_bits,
            },
            ColumnData::FixedF64Bits {
                validity: validity_all,
                values: vol_bits,
            },
        ],
    )
    .unwrap()
}

fn make_bars_like_rows(rows: usize) -> Vec<BarRowBorrowed<'static>> {
    let mut out = Vec::with_capacity(rows);
    for i in 0..rows {
        let pair = if (i & 1) == 0 { "BTCUSDT" } else { "ETHUSDT" };
        let tf = "1m";
        let e_ms = 1_700_000_000_000i64 + i as i64 * 60_000;
        let base = 10_000.0 + i as f64 * 0.25;
        out.push(BarRowBorrowed {
            pair,
            tf,
            e_ms,
            open: base + 0.10,
            high: base + 0.20,
            low: base + 0.05,
            close: base + 0.15,
            volume: 100.0 + (i % 10) as f64,
        });
    }
    out
}

fn bench_json_vs_mathldbt(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_vs_mathldbt");

    for &rows in &[2_000usize, 100_000usize] {
        let batch = make_bars_like_batch(rows);
        let rows_json = make_bars_like_rows(rows);

        let mut enc_ws = MathldbtV1EncodeWorkspace::default();
        enc_ws
            .set_enable_dict_utf8(true)
            .set_enable_delta_varint_i64(true);

        let mut mathldbt_bytes = Vec::new();
        encode_mathldbt_v1_into_with_workspace(&batch, &mut mathldbt_bytes, &mut enc_ws).unwrap();

        let json_bytes = serde_json::to_vec(&rows_json).unwrap();

        group.bench_with_input(
            BenchmarkId::new("mathldbt_encode_ws", rows),
            &rows,
            |b, _| {
                let mut out = Vec::new();
                let mut ws = MathldbtV1EncodeWorkspace::default();
                ws.set_enable_dict_utf8(true)
                    .set_enable_delta_varint_i64(true);
                b.iter(|| {
                    encode_mathldbt_v1_into_with_workspace(black_box(&batch), &mut out, &mut ws)
                        .unwrap();
                    black_box(out.len());
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mathldbt_decode_ws", rows),
            &rows,
            |b, _| {
                let mut ws = MathldbtV1DecodeWorkspace::default();
                b.iter(|| {
                    let decoded = decode_mathldbt_v1_with_workspace(
                        black_box(mathldbt_bytes.as_slice()),
                        &mut ws,
                    )
                    .unwrap();
                    black_box(decoded.row_count);
                })
            },
        );

        group.bench_with_input(BenchmarkId::new("json_serialize", rows), &rows, |b, _| {
            b.iter(|| {
                let bytes = serde_json::to_vec(black_box(&rows_json)).unwrap();
                black_box(bytes.len());
            })
        });

        group.bench_with_input(BenchmarkId::new("json_deserialize", rows), &rows, |b, _| {
            b.iter(|| {
                let decoded: Vec<BarRowOwned> =
                    serde_json::from_slice(black_box(json_bytes.as_slice())).unwrap();
                black_box(decoded.len());
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_json_vs_mathldbt);
criterion_main!(benches);
