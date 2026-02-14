use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use mathilde_binary_transport::batch::{ColumnData, ColumnarBatch, ValidityBitmap};
use mathilde_binary_transport::codec::mathldbt_v1::{
    MathldbtV1DecodeWorkspace, MathldbtV1EncodeWorkspace, decode_mathldbt_v1_with_workspace,
    encode_mathldbt_v1_into_with_workspace,
};
#[cfg(any(feature = "compression-zstd", feature = "compression-gzip"))]
use mathilde_binary_transport::codec::mathldbt_v1_compressed::{
    Compression, MathldbtV1CompressedDecodeWorkspace, MathldbtV1CompressedEncodeWorkspace,
    decode_mathldbt_v1_compressed_with_workspace,
    encode_mathldbt_v1_compressed_into_with_workspace,
};
use mathilde_binary_transport::schema::{ColumnarField, ColumnarSchema, ColumnarType};

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

fn bench_transport(c: &mut Criterion) {
    let mut group = c.benchmark_group("mathldbt_transport");
    for &rows in &[2_000usize, 100_000usize] {
        let batch = make_bars_like_batch(rows);

        let mut enc_ws_plain = MathldbtV1EncodeWorkspace::default();
        let mut enc_ws_opt = MathldbtV1EncodeWorkspace::default();
        enc_ws_opt
            .set_enable_dict_utf8(true)
            .set_enable_delta_varint_i64(true);

        let mut encoded_plain = Vec::new();
        encode_mathldbt_v1_into_with_workspace(&batch, &mut encoded_plain, &mut enc_ws_plain)
            .unwrap();

        let mut encoded_opt = Vec::new();
        encode_mathldbt_v1_into_with_workspace(&batch, &mut encoded_opt, &mut enc_ws_opt).unwrap();

        group.bench_with_input(BenchmarkId::new("encode_plain_ws", rows), &rows, |b, _| {
            let mut out = Vec::new();
            let mut ws = MathldbtV1EncodeWorkspace::default();
            b.iter(|| {
                encode_mathldbt_v1_into_with_workspace(black_box(&batch), &mut out, &mut ws)
                    .unwrap();
                black_box(out.len());
            })
        });

        group.bench_with_input(
            BenchmarkId::new("encode_dict_delta_ws", rows),
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

        group.bench_with_input(BenchmarkId::new("decode_plain_ws", rows), &rows, |b, _| {
            let mut ws = MathldbtV1DecodeWorkspace::default();
            b.iter(|| {
                let decoded =
                    decode_mathldbt_v1_with_workspace(black_box(encoded_plain.as_slice()), &mut ws)
                        .unwrap();
                black_box(decoded.row_count);
            })
        });

        group.bench_with_input(
            BenchmarkId::new("decode_dict_delta_ws", rows),
            &rows,
            |b, _| {
                let mut ws = MathldbtV1DecodeWorkspace::default();
                b.iter(|| {
                    let decoded = decode_mathldbt_v1_with_workspace(
                        black_box(encoded_opt.as_slice()),
                        &mut ws,
                    )
                    .unwrap();
                    black_box(decoded.row_count);
                })
            },
        );

        #[cfg(feature = "compression-zstd")]
        {
            let c = Compression::Zstd { level: 3 };

            let mut encoded_zstd = Vec::new();
            {
                let mut codec_ws = MathldbtV1EncodeWorkspace::default();
                codec_ws
                    .set_enable_dict_utf8(true)
                    .set_enable_delta_varint_i64(true);
                let mut compress_ws = MathldbtV1CompressedEncodeWorkspace::default();
                encode_mathldbt_v1_compressed_into_with_workspace(
                    &batch,
                    &mut encoded_zstd,
                    c,
                    &mut codec_ws,
                    &mut compress_ws,
                )
                .unwrap();
            }

            group.bench_with_input(BenchmarkId::new("encode_zstd_ws", rows), &rows, |b, _| {
                let mut out = Vec::new();
                let mut codec_ws = MathldbtV1EncodeWorkspace::default();
                codec_ws
                    .set_enable_dict_utf8(true)
                    .set_enable_delta_varint_i64(true);
                let mut compress_ws = MathldbtV1CompressedEncodeWorkspace::default();
                b.iter(|| {
                    encode_mathldbt_v1_compressed_into_with_workspace(
                        black_box(&batch),
                        &mut out,
                        c,
                        &mut codec_ws,
                        &mut compress_ws,
                    )
                    .unwrap();
                    black_box(out.len());
                })
            });

            group.bench_with_input(BenchmarkId::new("decode_zstd_ws", rows), &rows, |b, _| {
                let mut codec_ws = MathldbtV1DecodeWorkspace::default();
                let mut decompress_ws = MathldbtV1CompressedDecodeWorkspace::default();
                b.iter(|| {
                    let decoded = decode_mathldbt_v1_compressed_with_workspace(
                        black_box(encoded_zstd.as_slice()),
                        c,
                        1024 * 1024 * 1024,
                        &mut codec_ws,
                        &mut decompress_ws,
                    )
                    .unwrap();
                    black_box(decoded.row_count);
                })
            });
        }

        #[cfg(feature = "compression-gzip")]
        {
            let c = Compression::Gzip { level: 6 };

            let mut encoded_gzip = Vec::new();
            {
                let mut codec_ws = MathldbtV1EncodeWorkspace::default();
                codec_ws
                    .set_enable_dict_utf8(true)
                    .set_enable_delta_varint_i64(true);
                let mut compress_ws = MathldbtV1CompressedEncodeWorkspace::default();
                encode_mathldbt_v1_compressed_into_with_workspace(
                    &batch,
                    &mut encoded_gzip,
                    c,
                    &mut codec_ws,
                    &mut compress_ws,
                )
                .unwrap();
            }

            group.bench_with_input(BenchmarkId::new("encode_gzip_ws", rows), &rows, |b, _| {
                let mut out = Vec::new();
                let mut codec_ws = MathldbtV1EncodeWorkspace::default();
                codec_ws
                    .set_enable_dict_utf8(true)
                    .set_enable_delta_varint_i64(true);
                let mut compress_ws = MathldbtV1CompressedEncodeWorkspace::default();
                b.iter(|| {
                    encode_mathldbt_v1_compressed_into_with_workspace(
                        black_box(&batch),
                        &mut out,
                        c,
                        &mut codec_ws,
                        &mut compress_ws,
                    )
                    .unwrap();
                    black_box(out.len());
                })
            });

            group.bench_with_input(BenchmarkId::new("decode_gzip_ws", rows), &rows, |b, _| {
                let mut codec_ws = MathldbtV1DecodeWorkspace::default();
                let mut decompress_ws = MathldbtV1CompressedDecodeWorkspace::default();
                b.iter(|| {
                    let decoded = decode_mathldbt_v1_compressed_with_workspace(
                        black_box(encoded_gzip.as_slice()),
                        c,
                        1024 * 1024 * 1024,
                        &mut codec_ws,
                        &mut decompress_ws,
                    )
                    .unwrap();
                    black_box(decoded.row_count);
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_transport);
criterion_main!(benches);
