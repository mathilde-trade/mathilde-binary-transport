use crate::Error;
use crate::batch::{ColumnData, ColumnarBatch, ValidityBitmap};
use crate::codec::mathldbt_v1::{decode_mathldbt_v1, encode_mathldbt_v1_into};
use crate::codec::mathldbt_v1_compressed::{
    Compression, decode_mathldbt_v1_compressed, encode_mathldbt_v1_compressed_into,
};
use crate::schema::{ColumnarField, ColumnarSchema, ColumnarType};

fn sample_batch(rows: usize) -> ColumnarBatch {
    let schema = ColumnarSchema::new(vec![
        ColumnarField {
            name: Some("pair".to_string()),
            ty: ColumnarType::Utf8,
        },
        ColumnarField {
            name: Some("e_ms".to_string()),
            ty: ColumnarType::I64,
        },
        ColumnarField {
            name: Some("close".to_string()),
            ty: ColumnarType::F64,
        },
    ])
    .unwrap();

    let pair_validity = ValidityBitmap::new_all_valid(rows).unwrap();
    let mut offsets = vec![0u32; rows + 1];
    let mut data = Vec::new();
    for i in 0..rows {
        let v = if i % 2 == 0 { b"BTCUSDT" } else { b"ETHUSDT" };
        data.extend_from_slice(v);
        offsets[i + 1] = data.len() as u32;
    }

    let e_validity = ValidityBitmap::new_all_valid(rows).unwrap();
    let mut e_vals = Vec::with_capacity(rows);
    for i in 0..rows {
        e_vals.push(1_700_000_000_000i64 + i as i64 * 60_000);
    }

    let f_validity = ValidityBitmap::new_all_valid(rows).unwrap();
    let mut f_bits = Vec::with_capacity(rows);
    for i in 0..rows {
        f_bits.push((10000.0 + i as f64 * 0.25).to_bits());
    }

    ColumnarBatch::new(
        schema,
        rows,
        vec![
            ColumnData::Var {
                ty: ColumnarType::Utf8,
                validity: pair_validity,
                offsets,
                data,
            },
            ColumnData::FixedI64 {
                validity: e_validity,
                values: e_vals,
            },
            ColumnData::FixedF64Bits {
                validity: f_validity,
                values: f_bits,
            },
        ],
    )
    .unwrap()
}

#[test]
fn compression_none_round_trip_and_bytes_match_plain() {
    let batch = sample_batch(128);

    let mut plain = Vec::new();
    encode_mathldbt_v1_into(&batch, &mut plain).unwrap();

    let mut none = Vec::new();
    encode_mathldbt_v1_compressed_into(&batch, &mut none, Compression::None).unwrap();
    assert_eq!(none, plain);

    let decoded = decode_mathldbt_v1_compressed(&none, Compression::None, 0).unwrap();
    assert_eq!(decoded, batch);

    let decoded_plain = decode_mathldbt_v1(&plain).unwrap();
    assert_eq!(decoded_plain, batch);
}

#[cfg(feature = "compression-zstd")]
#[test]
fn zstd_round_trip_and_deterministic_bytes() {
    let batch = sample_batch(2_000);
    let c = Compression::Zstd { level: 3 };

    let mut a = Vec::new();
    let mut b = Vec::new();
    encode_mathldbt_v1_compressed_into(&batch, &mut a, c).unwrap();
    encode_mathldbt_v1_compressed_into(&batch, &mut b, c).unwrap();
    assert_eq!(a, b);

    let decoded = decode_mathldbt_v1_compressed(a.as_slice(), c, 128 * 1024 * 1024).unwrap();
    assert_eq!(decoded, batch);
}

#[cfg(feature = "compression-gzip")]
#[test]
fn gzip_round_trip_and_deterministic_bytes() {
    let batch = sample_batch(2_000);
    let c = Compression::Gzip { level: 6 };

    let mut a = Vec::new();
    let mut b = Vec::new();
    encode_mathldbt_v1_compressed_into(&batch, &mut a, c).unwrap();
    encode_mathldbt_v1_compressed_into(&batch, &mut b, c).unwrap();
    assert_eq!(a, b);

    let decoded = decode_mathldbt_v1_compressed(a.as_slice(), c, 128 * 1024 * 1024).unwrap();
    assert_eq!(decoded, batch);
}

#[cfg(feature = "compression-zstd")]
#[test]
fn zstd_decompression_bound_is_enforced() {
    let batch = sample_batch(256);
    let c = Compression::Zstd { level: 3 };

    let mut bytes = Vec::new();
    encode_mathldbt_v1_compressed_into(&batch, &mut bytes, c).unwrap();

    let err = decode_mathldbt_v1_compressed(bytes.as_slice(), c, 0).unwrap_err();
    assert_eq!(
        err,
        Error::Other("decompressed payload exceeds max_uncompressed_len".to_string())
    );
}

#[cfg(feature = "compression-gzip")]
#[test]
fn gzip_decompression_bound_is_enforced() {
    let batch = sample_batch(256);
    let c = Compression::Gzip { level: 6 };

    let mut bytes = Vec::new();
    encode_mathldbt_v1_compressed_into(&batch, &mut bytes, c).unwrap();

    let err = decode_mathldbt_v1_compressed(bytes.as_slice(), c, 0).unwrap_err();
    assert_eq!(
        err,
        Error::Other("decompressed payload exceeds max_uncompressed_len".to_string())
    );
}

#[cfg(feature = "compression-zstd")]
#[test]
fn zstd_malformed_payload_returns_err() {
    let bytes = b"not zstd";
    let err =
        decode_mathldbt_v1_compressed(bytes, Compression::Zstd { level: 3 }, 1024).unwrap_err();
    let _ = err;
}

#[cfg(feature = "compression-gzip")]
#[test]
fn gzip_malformed_payload_returns_err() {
    let bytes = b"not gzip";
    let err =
        decode_mathldbt_v1_compressed(bytes, Compression::Gzip { level: 6 }, 1024).unwrap_err();
    let _ = err;
}

#[cfg(not(feature = "compression-zstd"))]
#[test]
fn zstd_feature_not_enabled_errors_deterministically() {
    let batch = sample_batch(8);
    let mut out = Vec::new();
    let err = encode_mathldbt_v1_compressed_into(&batch, &mut out, Compression::Zstd { level: 3 })
        .unwrap_err();
    assert_eq!(
        err,
        Error::Other("zstd compression feature not enabled".to_string())
    );

    let err =
        decode_mathldbt_v1_compressed(b"x", Compression::Zstd { level: 3 }, 1024).unwrap_err();
    assert_eq!(
        err,
        Error::Other("zstd compression feature not enabled".to_string())
    );
}

#[cfg(not(feature = "compression-gzip"))]
#[test]
fn gzip_feature_not_enabled_errors_deterministically() {
    let batch = sample_batch(8);
    let mut out = Vec::new();
    let err = encode_mathldbt_v1_compressed_into(&batch, &mut out, Compression::Gzip { level: 6 })
        .unwrap_err();
    assert_eq!(
        err,
        Error::Other("gzip compression feature not enabled".to_string())
    );

    let err =
        decode_mathldbt_v1_compressed(b"x", Compression::Gzip { level: 6 }, 1024).unwrap_err();
    assert_eq!(
        err,
        Error::Other("gzip compression feature not enabled".to_string())
    );
}

#[cfg(feature = "compression-zstd")]
#[test]
fn invalid_zstd_level_is_rejected_deterministically() {
    let batch = sample_batch(8);

    let mut out = Vec::new();
    let err =
        encode_mathldbt_v1_compressed_into(&batch, &mut out, Compression::Zstd { level: 999 })
            .unwrap_err();
    assert_eq!(err, Error::Other("invalid zstd level".to_string()));
}

#[cfg(feature = "compression-gzip")]
#[test]
fn invalid_gzip_level_is_rejected_deterministically() {
    let batch = sample_batch(8);

    let mut out = Vec::new();
    let err =
        encode_mathldbt_v1_compressed_into(&batch, &mut out, Compression::Gzip { level: 999 })
            .unwrap_err();
    assert_eq!(err, Error::Other("invalid gzip level".to_string()));
}
