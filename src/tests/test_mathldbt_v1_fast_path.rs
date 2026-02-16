use crate::Error;
use crate::batch::{ColumnData, ColumnarBatch, ValidityBitmap};
use crate::batch_view::{ColumnDataView, ColumnarBatchView, VarDataView};
use crate::codec::mathldbt_v1::{
    MathldbtV1EncodeWorkspace, encode_mathldbt_v1_fast_path_into_with_workspace,
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
fn fast_path_plain_bytes_match_owned_plain() {
    let batch = sample_batch(2_000);
    let (ty, validity, offsets, data) = match &batch.columns[0] {
        ColumnData::Var {
            ty,
            validity,
            offsets,
            data,
        } => (*ty, validity.as_bytes(), offsets.as_slice(), data.as_slice()),
        _ => unreachable!(),
    };
    let (v_i64, vals_i64) = match &batch.columns[1] {
        ColumnData::FixedI64 { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };
    let (v_f64, vals_f64) = match &batch.columns[2] {
        ColumnData::FixedF64Bits { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };

    let cols = vec![
        ColumnDataView::Var {
            ty,
            validity,
            offsets,
            data: VarDataView::Contiguous(data),
        },
        ColumnDataView::FixedI64 {
            validity: v_i64,
            values: vals_i64,
        },
        ColumnDataView::FixedF64Bits {
            validity: v_f64,
            values: vals_f64,
        },
    ];
    let view = ColumnarBatchView {
        schema: &batch.schema,
        row_count: batch.row_count,
        columns: cols.as_slice(),
    };

    let mut owned = Vec::new();
    let mut fast = Vec::new();

    let mut ws_owned = MathldbtV1EncodeWorkspace::default();
    let mut ws_fast = MathldbtV1EncodeWorkspace::default();

    encode_mathldbt_v1_into_with_workspace(&batch, &mut owned, &mut ws_owned).unwrap();
    encode_mathldbt_v1_fast_path_into_with_workspace(&view, &mut fast, &mut ws_fast).unwrap();

    assert_eq!(owned, fast);
}

#[test]
fn fast_path_plain_bytes_match_owned_plain_for_chunked_varlen() {
    let batch = sample_batch(2_000);
    let (ty, validity, offsets, data) = match &batch.columns[0] {
        ColumnData::Var {
            ty,
            validity,
            offsets,
            data,
        } => (*ty, validity.as_bytes(), offsets.as_slice(), data.as_slice()),
        _ => unreachable!(),
    };
    let (v_i64, vals_i64) = match &batch.columns[1] {
        ColumnData::FixedI64 { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };
    let (v_f64, vals_f64) = match &batch.columns[2] {
        ColumnData::FixedF64Bits { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };

    let split = (data.len() / 2).max(1);
    let (a, b) = data.split_at(split);
    let chunks: [&[u8]; 1] = [b];

    let cols = vec![
        ColumnDataView::Var {
            ty,
            validity,
            offsets,
            data: VarDataView::Chunks {
                inline: a,
                chunks: &chunks,
            },
        },
        ColumnDataView::FixedI64 {
            validity: v_i64,
            values: vals_i64,
        },
        ColumnDataView::FixedF64Bits {
            validity: v_f64,
            values: vals_f64,
        },
    ];
    let view = ColumnarBatchView {
        schema: &batch.schema,
        row_count: batch.row_count,
        columns: cols.as_slice(),
    };

    let mut owned = Vec::new();
    let mut fast = Vec::new();

    let mut ws_owned = MathldbtV1EncodeWorkspace::default();
    let mut ws_fast = MathldbtV1EncodeWorkspace::default();

    encode_mathldbt_v1_into_with_workspace(&batch, &mut owned, &mut ws_owned).unwrap();
    encode_mathldbt_v1_fast_path_into_with_workspace(&view, &mut fast, &mut ws_fast).unwrap();

    assert_eq!(owned, fast);
}

#[test]
fn fast_path_opt_bytes_match_owned_opt_including_chunked_varlen() {
    let batch = sample_batch(2_000);
    let (ty, validity, offsets, data) = match &batch.columns[0] {
        ColumnData::Var {
            ty,
            validity,
            offsets,
            data,
        } => (*ty, validity.as_bytes(), offsets.as_slice(), data.as_slice()),
        _ => unreachable!(),
    };
    let (v_i64, vals_i64) = match &batch.columns[1] {
        ColumnData::FixedI64 { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };
    let (v_f64, vals_f64) = match &batch.columns[2] {
        ColumnData::FixedF64Bits { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };

    let split = (data.len() / 2).max(1);
    let (a, b) = data.split_at(split);
    let chunks: [&[u8]; 1] = [b];

    let cols = vec![
        ColumnDataView::Var {
            ty,
            validity,
            offsets,
            data: VarDataView::Chunks {
                inline: a,
                chunks: &chunks,
            },
        },
        ColumnDataView::FixedI64 {
            validity: v_i64,
            values: vals_i64,
        },
        ColumnDataView::FixedF64Bits {
            validity: v_f64,
            values: vals_f64,
        },
    ];
    let view = ColumnarBatchView {
        schema: &batch.schema,
        row_count: batch.row_count,
        columns: cols.as_slice(),
    };

    let mut owned = Vec::new();
    let mut fast = Vec::new();

    let mut ws_owned = MathldbtV1EncodeWorkspace::default();
    ws_owned
        .set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);
    let mut ws_fast = MathldbtV1EncodeWorkspace::default();
    ws_fast
        .set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);

    encode_mathldbt_v1_into_with_workspace(&batch, &mut owned, &mut ws_owned).unwrap();
    encode_mathldbt_v1_fast_path_into_with_workspace(&view, &mut fast, &mut ws_fast).unwrap();

    assert_eq!(owned, fast);
}

#[test]
fn fast_path_is_deterministic_with_workspace_reuse() {
    let batch = sample_batch(2_000);
    let (ty, validity, offsets, data) = match &batch.columns[0] {
        ColumnData::Var {
            ty,
            validity,
            offsets,
            data,
        } => (*ty, validity.as_bytes(), offsets.as_slice(), data.as_slice()),
        _ => unreachable!(),
    };
    let (v_i64, vals_i64) = match &batch.columns[1] {
        ColumnData::FixedI64 { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };
    let (v_f64, vals_f64) = match &batch.columns[2] {
        ColumnData::FixedF64Bits { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };

    let split = (data.len() / 2).max(1);
    let (a, b) = data.split_at(split);
    let chunks: [&[u8]; 1] = [b];

    let cols = vec![
        ColumnDataView::Var {
            ty,
            validity,
            offsets,
            data: VarDataView::Chunks {
                inline: a,
                chunks: &chunks,
            },
        },
        ColumnDataView::FixedI64 {
            validity: v_i64,
            values: vals_i64,
        },
        ColumnDataView::FixedF64Bits {
            validity: v_f64,
            values: vals_f64,
        },
    ];
    let view = ColumnarBatchView {
        schema: &batch.schema,
        row_count: batch.row_count,
        columns: cols.as_slice(),
    };

    let mut out_a = Vec::new();
    let mut out_b = Vec::new();
    let mut ws = MathldbtV1EncodeWorkspace::default();
    ws.set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);

    encode_mathldbt_v1_fast_path_into_with_workspace(&view, &mut out_a, &mut ws).unwrap();
    encode_mathldbt_v1_fast_path_into_with_workspace(&view, &mut out_b, &mut ws).unwrap();
    assert_eq!(out_a, out_b);
}

#[test]
fn fast_path_invalid_validity_len_errors_deterministically() {
    let batch = sample_batch(8);
    let (validity, offsets, data) = match &batch.columns[0] {
        ColumnData::Var { validity, offsets, data, .. } => (validity, offsets, data),
        _ => unreachable!(),
    };

    let cols = vec![ColumnDataView::Var {
        ty: ColumnarType::Utf8,
        validity: &validity.as_bytes()[..0],
        offsets: offsets.as_slice(),
        data: VarDataView::Contiguous(data.as_slice()),
    }];

    let schema = ColumnarSchema::new(vec![ColumnarField {
        name: Some("pair".to_string()),
        ty: ColumnarType::Utf8,
    }])
    .unwrap();

    let view = ColumnarBatchView {
        schema: &schema,
        row_count: 8,
        columns: cols.as_slice(),
    };

    let mut out = Vec::new();
    let mut ws = MathldbtV1EncodeWorkspace::default();
    let err = encode_mathldbt_v1_fast_path_into_with_workspace(&view, &mut out, &mut ws).unwrap_err();
    assert_eq!(err, Error::Other("validity length mismatch".to_string()));
}

#[test]
fn fast_path_invalid_offsets_len_errors_deterministically() {
    let batch = sample_batch(8);
    let (validity, offsets, data) = match &batch.columns[0] {
        ColumnData::Var { validity, offsets, data, .. } => (validity, offsets, data),
        _ => unreachable!(),
    };

    let cols = vec![ColumnDataView::Var {
        ty: ColumnarType::Utf8,
        validity: validity.as_bytes(),
        offsets: &offsets.as_slice()[..offsets.len() - 1],
        data: VarDataView::Contiguous(data.as_slice()),
    }];

    let schema = ColumnarSchema::new(vec![ColumnarField {
        name: Some("pair".to_string()),
        ty: ColumnarType::Utf8,
    }])
    .unwrap();

    let view = ColumnarBatchView {
        schema: &schema,
        row_count: 8,
        columns: cols.as_slice(),
    };

    let mut out = Vec::new();
    let mut ws = MathldbtV1EncodeWorkspace::default();
    let err = encode_mathldbt_v1_fast_path_into_with_workspace(&view, &mut out, &mut ws).unwrap_err();
    assert_eq!(err, Error::Other("offsets length mismatch".to_string()));
}

#[test]
fn fast_path_offsets_first_must_be_zero_errors_deterministically() {
    let batch = sample_batch(8);
    let (validity, offsets, data) = match &batch.columns[0] {
        ColumnData::Var { validity, offsets, data, .. } => (validity, offsets, data),
        _ => unreachable!(),
    };

    let mut bad_offsets = offsets.clone();
    if let Some(first) = bad_offsets.first_mut() {
        *first = 1;
    }

    let cols = vec![ColumnDataView::Var {
        ty: ColumnarType::Utf8,
        validity: validity.as_bytes(),
        offsets: bad_offsets.as_slice(),
        data: VarDataView::Contiguous(data.as_slice()),
    }];

    let schema = ColumnarSchema::new(vec![ColumnarField {
        name: Some("pair".to_string()),
        ty: ColumnarType::Utf8,
    }])
    .unwrap();

    let view = ColumnarBatchView {
        schema: &schema,
        row_count: 8,
        columns: cols.as_slice(),
    };

    let mut out = Vec::new();
    let mut ws = MathldbtV1EncodeWorkspace::default();
    let err = encode_mathldbt_v1_fast_path_into_with_workspace(&view, &mut out, &mut ws).unwrap_err();
    assert_eq!(err, Error::Other("offsets[0] must be 0".to_string()));
}

#[test]
fn fast_path_offsets_non_monotonic_errors_deterministically() {
    let batch = sample_batch(8);
    let (validity, offsets, data) = match &batch.columns[0] {
        ColumnData::Var { validity, offsets, data, .. } => (validity, offsets, data),
        _ => unreachable!(),
    };

    let mut bad_offsets = offsets.clone();
    if bad_offsets.len() >= 3 {
        bad_offsets[2] = bad_offsets[1].saturating_sub(1);
    }

    let cols = vec![ColumnDataView::Var {
        ty: ColumnarType::Utf8,
        validity: validity.as_bytes(),
        offsets: bad_offsets.as_slice(),
        data: VarDataView::Contiguous(data.as_slice()),
    }];

    let schema = ColumnarSchema::new(vec![ColumnarField {
        name: Some("pair".to_string()),
        ty: ColumnarType::Utf8,
    }])
    .unwrap();

    let view = ColumnarBatchView {
        schema: &schema,
        row_count: 8,
        columns: cols.as_slice(),
    };

    let mut out = Vec::new();
    let mut ws = MathldbtV1EncodeWorkspace::default();
    let err = encode_mathldbt_v1_fast_path_into_with_workspace(&view, &mut out, &mut ws).unwrap_err();
    assert_eq!(err, Error::Other("offsets must be non-decreasing".to_string()));
}

#[test]
fn fast_path_final_offset_mismatch_errors_deterministically() {
    let batch = sample_batch(8);
    let (validity, offsets, data) = match &batch.columns[0] {
        ColumnData::Var { validity, offsets, data, .. } => (validity, offsets, data),
        _ => unreachable!(),
    };

    let mut bad_offsets = offsets.clone();
    if let Some(last) = bad_offsets.last_mut() {
        *last = (*last).saturating_add(1);
    }

    let cols = vec![ColumnDataView::Var {
        ty: ColumnarType::Utf8,
        validity: validity.as_bytes(),
        offsets: bad_offsets.as_slice(),
        data: VarDataView::Contiguous(data.as_slice()),
    }];

    let schema = ColumnarSchema::new(vec![ColumnarField {
        name: Some("pair".to_string()),
        ty: ColumnarType::Utf8,
    }])
    .unwrap();

    let view = ColumnarBatchView {
        schema: &schema,
        row_count: 8,
        columns: cols.as_slice(),
    };

    let mut out = Vec::new();
    let mut ws = MathldbtV1EncodeWorkspace::default();
    let err = encode_mathldbt_v1_fast_path_into_with_workspace(&view, &mut out, &mut ws).unwrap_err();
    assert_eq!(err, Error::Other("final offset mismatch".to_string()));
}

#[test]
fn fast_path_chunked_data_len_mismatch_errors_deterministically() {
    let schema = ColumnarSchema::new(vec![ColumnarField {
        name: Some("pair".to_string()),
        ty: ColumnarType::Utf8,
    }])
    .unwrap();
    let row_count = 2usize;
    let validity = ValidityBitmap::new_all_valid(row_count).unwrap();
    let offsets: [u32; 3] = [0, 1, 3];

    let inline = b"a";
    let chunk = b"b";
    let chunks: [&[u8]; 1] = [chunk.as_slice()];

    let cols = vec![ColumnDataView::Var {
        ty: ColumnarType::Utf8,
        validity: validity.as_bytes(),
        offsets: offsets.as_slice(),
        data: VarDataView::Chunks {
            inline,
            chunks: &chunks,
        },
    }];

    let view = ColumnarBatchView {
        schema: &schema,
        row_count,
        columns: cols.as_slice(),
    };

    let mut out = Vec::new();
    let mut ws = MathldbtV1EncodeWorkspace::default();
    let err = encode_mathldbt_v1_fast_path_into_with_workspace(&view, &mut out, &mut ws).unwrap_err();
    assert_eq!(err, Error::Other("final offset mismatch".to_string()));
}

#[cfg(feature = "compression-zstd")]
#[test]
fn compressed_fast_path_zstd_bytes_match_owned() {
    use crate::codec::mathldbt_v1_compressed::{
        encode_mathldbt_v1_compressed_fast_path_into_with_workspace,
        encode_mathldbt_v1_compressed_into_with_workspace, Compression,
        MathldbtV1CompressedEncodeWorkspace,
    };

    let batch = sample_batch(2_000);
    let (ty, validity, offsets, data) = match &batch.columns[0] {
        ColumnData::Var {
            ty,
            validity,
            offsets,
            data,
        } => (*ty, validity.as_bytes(), offsets.as_slice(), data.as_slice()),
        _ => unreachable!(),
    };
    let (v_i64, vals_i64) = match &batch.columns[1] {
        ColumnData::FixedI64 { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };
    let (v_f64, vals_f64) = match &batch.columns[2] {
        ColumnData::FixedF64Bits { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };

    let split = (data.len() / 2).max(1);
    let (a, b) = data.split_at(split);
    let chunks: [&[u8]; 1] = [b];

    let cols = vec![
        ColumnDataView::Var {
            ty,
            validity,
            offsets,
            data: VarDataView::Chunks {
                inline: a,
                chunks: &chunks,
            },
        },
        ColumnDataView::FixedI64 {
            validity: v_i64,
            values: vals_i64,
        },
        ColumnDataView::FixedF64Bits {
            validity: v_f64,
            values: vals_f64,
        },
    ];
    let view = ColumnarBatchView {
        schema: &batch.schema,
        row_count: batch.row_count,
        columns: cols.as_slice(),
    };
    let c = Compression::Zstd { level: 3 };

    let mut owned = Vec::new();
    let mut fast = Vec::new();

    let mut codec_ws_owned = MathldbtV1EncodeWorkspace::default();
    codec_ws_owned
        .set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);
    let mut codec_ws_fast = MathldbtV1EncodeWorkspace::default();
    codec_ws_fast
        .set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);

    let mut ws_owned = MathldbtV1CompressedEncodeWorkspace::default();
    let mut ws_fast = MathldbtV1CompressedEncodeWorkspace::default();

    encode_mathldbt_v1_compressed_into_with_workspace(
        &batch,
        &mut owned,
        c,
        &mut codec_ws_owned,
        &mut ws_owned,
    )
    .unwrap();
    encode_mathldbt_v1_compressed_fast_path_into_with_workspace(
        &view,
        &mut fast,
        c,
        &mut codec_ws_fast,
        &mut ws_fast,
    )
    .unwrap();

    assert_eq!(owned, fast);
}

#[cfg(feature = "compression-zstd")]
#[test]
fn compressed_fast_path_zstd_is_deterministic_with_workspace_reuse() {
    use crate::codec::mathldbt_v1_compressed::{
        encode_mathldbt_v1_compressed_fast_path_into_with_workspace, Compression,
        MathldbtV1CompressedEncodeWorkspace,
    };

    let batch = sample_batch(2_000);
    let (ty, validity, offsets, data) = match &batch.columns[0] {
        ColumnData::Var {
            ty,
            validity,
            offsets,
            data,
        } => (*ty, validity.as_bytes(), offsets.as_slice(), data.as_slice()),
        _ => unreachable!(),
    };
    let (v_i64, vals_i64) = match &batch.columns[1] {
        ColumnData::FixedI64 { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };
    let (v_f64, vals_f64) = match &batch.columns[2] {
        ColumnData::FixedF64Bits { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };

    let split = (data.len() / 2).max(1);
    let (a, b) = data.split_at(split);
    let chunks: [&[u8]; 1] = [b];

    let cols = vec![
        ColumnDataView::Var {
            ty,
            validity,
            offsets,
            data: VarDataView::Chunks {
                inline: a,
                chunks: &chunks,
            },
        },
        ColumnDataView::FixedI64 {
            validity: v_i64,
            values: vals_i64,
        },
        ColumnDataView::FixedF64Bits {
            validity: v_f64,
            values: vals_f64,
        },
    ];
    let view = ColumnarBatchView {
        schema: &batch.schema,
        row_count: batch.row_count,
        columns: cols.as_slice(),
    };
    let c = Compression::Zstd { level: 3 };

    let mut out_a = Vec::new();
    let mut out_b = Vec::new();

    let mut codec_ws = MathldbtV1EncodeWorkspace::default();
    codec_ws
        .set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);
    let mut ws = MathldbtV1CompressedEncodeWorkspace::default();

    encode_mathldbt_v1_compressed_fast_path_into_with_workspace(
        &view,
        &mut out_a,
        c,
        &mut codec_ws,
        &mut ws,
    )
    .unwrap();
    encode_mathldbt_v1_compressed_fast_path_into_with_workspace(
        &view,
        &mut out_b,
        c,
        &mut codec_ws,
        &mut ws,
    )
    .unwrap();
    assert_eq!(out_a, out_b);
}

#[cfg(feature = "compression-gzip")]
#[test]
fn compressed_fast_path_gzip_bytes_match_owned() {
    use crate::codec::mathldbt_v1_compressed::{
        encode_mathldbt_v1_compressed_fast_path_into_with_workspace,
        encode_mathldbt_v1_compressed_into_with_workspace, Compression,
        MathldbtV1CompressedEncodeWorkspace,
    };

    let batch = sample_batch(2_000);
    let (ty, validity, offsets, data) = match &batch.columns[0] {
        ColumnData::Var {
            ty,
            validity,
            offsets,
            data,
        } => (*ty, validity.as_bytes(), offsets.as_slice(), data.as_slice()),
        _ => unreachable!(),
    };
    let (v_i64, vals_i64) = match &batch.columns[1] {
        ColumnData::FixedI64 { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };
    let (v_f64, vals_f64) = match &batch.columns[2] {
        ColumnData::FixedF64Bits { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };

    let split = (data.len() / 2).max(1);
    let (a, b) = data.split_at(split);
    let chunks: [&[u8]; 1] = [b];

    let cols = vec![
        ColumnDataView::Var {
            ty,
            validity,
            offsets,
            data: VarDataView::Chunks {
                inline: a,
                chunks: &chunks,
            },
        },
        ColumnDataView::FixedI64 {
            validity: v_i64,
            values: vals_i64,
        },
        ColumnDataView::FixedF64Bits {
            validity: v_f64,
            values: vals_f64,
        },
    ];
    let view = ColumnarBatchView {
        schema: &batch.schema,
        row_count: batch.row_count,
        columns: cols.as_slice(),
    };
    let c = Compression::Gzip { level: 6 };

    let mut owned = Vec::new();
    let mut fast = Vec::new();

    let mut codec_ws_owned = MathldbtV1EncodeWorkspace::default();
    codec_ws_owned
        .set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);
    let mut codec_ws_fast = MathldbtV1EncodeWorkspace::default();
    codec_ws_fast
        .set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);

    let mut ws_owned = MathldbtV1CompressedEncodeWorkspace::default();
    let mut ws_fast = MathldbtV1CompressedEncodeWorkspace::default();

    encode_mathldbt_v1_compressed_into_with_workspace(
        &batch,
        &mut owned,
        c,
        &mut codec_ws_owned,
        &mut ws_owned,
    )
    .unwrap();
    encode_mathldbt_v1_compressed_fast_path_into_with_workspace(
        &view,
        &mut fast,
        c,
        &mut codec_ws_fast,
        &mut ws_fast,
    )
    .unwrap();

    assert_eq!(owned, fast);
}

#[cfg(feature = "compression-gzip")]
#[test]
fn compressed_fast_path_gzip_is_deterministic_with_workspace_reuse() {
    use crate::codec::mathldbt_v1_compressed::{
        encode_mathldbt_v1_compressed_fast_path_into_with_workspace, Compression,
        MathldbtV1CompressedEncodeWorkspace,
    };

    let batch = sample_batch(2_000);
    let (ty, validity, offsets, data) = match &batch.columns[0] {
        ColumnData::Var {
            ty,
            validity,
            offsets,
            data,
        } => (*ty, validity.as_bytes(), offsets.as_slice(), data.as_slice()),
        _ => unreachable!(),
    };
    let (v_i64, vals_i64) = match &batch.columns[1] {
        ColumnData::FixedI64 { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };
    let (v_f64, vals_f64) = match &batch.columns[2] {
        ColumnData::FixedF64Bits { validity, values } => (validity.as_bytes(), values.as_slice()),
        _ => unreachable!(),
    };

    let split = (data.len() / 2).max(1);
    let (a, b) = data.split_at(split);
    let chunks: [&[u8]; 1] = [b];

    let cols = vec![
        ColumnDataView::Var {
            ty,
            validity,
            offsets,
            data: VarDataView::Chunks {
                inline: a,
                chunks: &chunks,
            },
        },
        ColumnDataView::FixedI64 {
            validity: v_i64,
            values: vals_i64,
        },
        ColumnDataView::FixedF64Bits {
            validity: v_f64,
            values: vals_f64,
        },
    ];
    let view = ColumnarBatchView {
        schema: &batch.schema,
        row_count: batch.row_count,
        columns: cols.as_slice(),
    };
    let c = Compression::Gzip { level: 6 };

    let mut out_a = Vec::new();
    let mut out_b = Vec::new();

    let mut codec_ws = MathldbtV1EncodeWorkspace::default();
    codec_ws
        .set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);
    let mut ws = MathldbtV1CompressedEncodeWorkspace::default();

    encode_mathldbt_v1_compressed_fast_path_into_with_workspace(
        &view,
        &mut out_a,
        c,
        &mut codec_ws,
        &mut ws,
    )
    .unwrap();
    encode_mathldbt_v1_compressed_fast_path_into_with_workspace(
        &view,
        &mut out_b,
        c,
        &mut codec_ws,
        &mut ws,
    )
    .unwrap();
    assert_eq!(out_a, out_b);
}
