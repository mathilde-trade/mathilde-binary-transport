use crate::batch::{ColumnData, ColumnarBatch, ValidityBitmap};
use crate::codec::mathldbt_v1::{
    MathldbtV1EncodeWorkspace, decode_mathldbt_v1, decode_mathldbt_v1_into_with_workspace,
    encode_mathldbt_v1_into_with_workspace,
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

    let mut pair_validity = ValidityBitmap::new_all_valid(rows).unwrap();
    if rows > 0 {
        pair_validity.set(0, false).unwrap();
    }
    let mut offsets = vec![0u32; rows + 1];
    let mut data = Vec::new();
    for i in 0..rows {
        if pair_validity.is_valid(i).unwrap() {
            let v = if i % 2 == 0 { b"BTCUSDT" } else { b"ETHUSDT" };
            data.extend_from_slice(v);
            offsets[i + 1] = data.len() as u32;
        } else {
            offsets[i + 1] = offsets[i];
        }
    }

    let e_validity = ValidityBitmap::new_all_valid(rows).unwrap();
    let mut e_vals = Vec::with_capacity(rows);
    for i in 0..rows {
        e_vals.push(1_700_000_000_000i64 + i as i64 * 60_000);
    }

    let f_validity = ValidityBitmap::new_all_valid(rows).unwrap();
    let mut f_bits = Vec::with_capacity(rows);
    for i in 0..rows {
        f_bits.push((10_000.0 + i as f64 * 0.25).to_bits());
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

fn make_out(schema: ColumnarSchema) -> ColumnarBatch {
    let mut cols = Vec::with_capacity(schema.len());
    for f in schema.fields().iter() {
        cols.push(ColumnData::new_all_invalid(f.ty, 0).unwrap());
    }
    ColumnarBatch::new(schema, 0, cols).unwrap()
}

#[test]
fn decode_into_matches_allocating_decode_plain() {
    let batch = sample_batch(256);
    let mut bytes = Vec::new();
    let mut enc_ws = MathldbtV1EncodeWorkspace::default();
    encode_mathldbt_v1_into_with_workspace(&batch, &mut bytes, &mut enc_ws).unwrap();

    let decoded = decode_mathldbt_v1(&bytes).unwrap();

    let mut out = make_out(batch.schema.clone());
    let mut dec_ws = crate::codec::mathldbt_v1::MathldbtV1DecodeWorkspace::default();
    decode_mathldbt_v1_into_with_workspace(&bytes, &mut out, &mut dec_ws).unwrap();
    assert_eq!(out, decoded);
}

#[test]
fn decode_into_matches_allocating_decode_dict_and_delta() {
    let batch = sample_batch(1024);
    let mut bytes = Vec::new();
    let mut enc_ws = MathldbtV1EncodeWorkspace::default();
    enc_ws
        .set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);
    encode_mathldbt_v1_into_with_workspace(&batch, &mut bytes, &mut enc_ws).unwrap();

    let decoded = decode_mathldbt_v1(&bytes).unwrap();

    let mut out = make_out(batch.schema.clone());
    let mut dec_ws = crate::codec::mathldbt_v1::MathldbtV1DecodeWorkspace::default();
    decode_mathldbt_v1_into_with_workspace(&bytes, &mut out, &mut dec_ws).unwrap();
    assert_eq!(out, decoded);
}

