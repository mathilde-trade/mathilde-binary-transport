use crate::Error;
use crate::batch::{ColumnData, ColumnarBatch};
use crate::codec::mathldbt_v1::{decode_mathldbt_v1, decode_mathldbt_v1_into};
use crate::schema::{ColumnarField, ColumnarSchema, ColumnarType};

fn write_u16_le(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn write_u32_le(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn build_minimal_header(row_count: u32, col_count: u16) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"MATHLDBT"); // magic
    write_u16_le(&mut out, 1); // version
    write_u16_le(&mut out, 0); // flags
    write_u32_le(&mut out, row_count);
    write_u16_le(&mut out, col_count);
    write_u16_le(&mut out, 0); // schema_id_len
    out
}

fn push_col_descriptor(
    out: &mut Vec<u8>,
    type_id: u16,
    encoding_id: u16,
    name_bytes: &[u8],
    validity_len: u32,
    validity_bytes: &[u8],
    payload1: &[u8],
    payload2: &[u8],
) {
    write_u16_le(out, type_id);
    write_u16_le(out, encoding_id);
    write_u16_le(out, 0); // col_flags

    write_u16_le(out, name_bytes.len() as u16);
    out.extend_from_slice(name_bytes);

    write_u32_le(out, validity_len);
    out.extend_from_slice(validity_bytes);

    write_u32_le(out, payload1.len() as u32);
    out.extend_from_slice(payload1);
    write_u32_le(out, payload2.len() as u32);
    out.extend_from_slice(payload2);
}

#[test]
fn truncated_header_is_rejected() {
    let bytes = b"MATHLDBT".to_vec(); // missing version+...
    let err = decode_mathldbt_v1(&bytes).unwrap_err();
    assert_eq!(err, Error::Other("truncated mathldbt".to_string()));
}

#[test]
fn col_count_zero_is_rejected() {
    let bytes = build_minimal_header(1, 0);
    let err = decode_mathldbt_v1(&bytes).unwrap_err();
    assert_eq!(
        err,
        Error::Other("MATHLDBT must have at least one column".to_string())
    );
}

#[test]
fn invalid_utf8_column_name_is_rejected() {
    let mut bytes = build_minimal_header(1, 1);
    // Bool column (type_id=1), PlainLE (encoding=0)
    let validity = [0b0000_0001u8];
    let payload1 = [1u8];
    push_col_descriptor(&mut bytes, 1, 0, &[0xFF], 1, &validity, &payload1, &[]);
    let err = decode_mathldbt_v1(&bytes).unwrap_err();
    assert_eq!(err, Error::Other("invalid UTF-8 column name".to_string()));
}

#[test]
fn validity_length_mismatch_is_rejected() {
    let mut bytes = build_minimal_header(1, 1);
    // expected validity_len = 1, but set 0 so the decoder errors before consuming bytes.
    push_col_descriptor(&mut bytes, 1, 0, b"", 0, &[], &[1u8], &[]);
    let err = decode_mathldbt_v1(&bytes).unwrap_err();
    assert_eq!(err, Error::Other("validity length mismatch".to_string()));
}

#[test]
fn var_offsets_length_mismatch_is_rejected() {
    let mut bytes = build_minimal_header(1, 1);
    let validity = [0b0000_0001u8];
    // Utf8 PlainVar expects offsets len = (rows+1)*4 = 8 bytes; provide 0.
    push_col_descriptor(&mut bytes, 9, 0, b"", 1, &validity, &[], b"a");
    let err = decode_mathldbt_v1(&bytes).unwrap_err();
    assert_eq!(err, Error::Other("offsets length mismatch".to_string()));
}

#[test]
fn var_offsets_non_monotonic_is_rejected() {
    let mut bytes = build_minimal_header(2, 1);
    let validity = [0b0000_0011u8];
    // offsets [0,2,1] (non-monotonic), data len 2 is irrelevant (monotonic check happens first)
    let mut offsets_bytes = Vec::new();
    offsets_bytes.extend_from_slice(&0u32.to_le_bytes());
    offsets_bytes.extend_from_slice(&2u32.to_le_bytes());
    offsets_bytes.extend_from_slice(&1u32.to_le_bytes());
    push_col_descriptor(&mut bytes, 9, 0, b"", 1, &validity, &offsets_bytes, b"ab");
    let err = decode_mathldbt_v1(&bytes).unwrap_err();
    assert_eq!(
        err,
        Error::Other("offsets must be non-decreasing".to_string())
    );
}

#[test]
fn var_final_offset_mismatch_is_rejected() {
    let mut bytes = build_minimal_header(2, 1);
    let validity = [0b0000_0011u8];
    // offsets [0,1,2] but data len is 1 -> final mismatch
    let mut offsets_bytes = Vec::new();
    offsets_bytes.extend_from_slice(&0u32.to_le_bytes());
    offsets_bytes.extend_from_slice(&1u32.to_le_bytes());
    offsets_bytes.extend_from_slice(&2u32.to_le_bytes());
    push_col_descriptor(&mut bytes, 9, 0, b"", 1, &validity, &offsets_bytes, b"a");
    let err = decode_mathldbt_v1(&bytes).unwrap_err();
    assert_eq!(err, Error::Other("final offset mismatch".to_string()));
}

#[test]
fn dict_utf8_invalid_width_is_rejected() {
    let mut bytes = build_minimal_header(1, 1);
    let validity = [0b0000_0001u8];
    let indices = [0u8]; // one row, index_width=1
    // dict blob: width=3 (invalid), dict_count=1, offsets=[0,1], bytes=[b'a']
    let mut dict = Vec::new();
    dict.push(3u8);
    dict.extend_from_slice(&1u32.to_le_bytes());
    dict.extend_from_slice(&0u32.to_le_bytes());
    dict.extend_from_slice(&1u32.to_le_bytes());
    dict.push(b'a');
    push_col_descriptor(&mut bytes, 9, 2, b"", 1, &validity, &indices, &dict);
    let err = decode_mathldbt_v1(&bytes).unwrap_err();
    assert_eq!(err, Error::Other("invalid dict index width".to_string()));
}

#[test]
fn dict_utf8_index_out_of_bounds_is_rejected() {
    let mut bytes = build_minimal_header(1, 1);
    let validity = [0b0000_0001u8];
    let indices = [1u8]; // dict_count will be 1 -> idx=1 oob
    let mut dict = Vec::new();
    dict.push(1u8); // width
    dict.extend_from_slice(&1u32.to_le_bytes()); // dict_count
    dict.extend_from_slice(&0u32.to_le_bytes());
    dict.extend_from_slice(&1u32.to_le_bytes());
    dict.push(b'a');
    push_col_descriptor(&mut bytes, 9, 2, b"", 1, &validity, &indices, &dict);
    let err = decode_mathldbt_v1(&bytes).unwrap_err();
    assert_eq!(err, Error::Other("dict index out of bounds".to_string()));
}

#[test]
fn delta_truncated_base_is_rejected() {
    let mut bytes = build_minimal_header(1, 1);
    let validity = [0b0000_0001u8];
    let payload = [0u8; 7]; // must be at least 8
    push_col_descriptor(&mut bytes, 4, 3, b"", 1, &validity, &payload, &[]);
    let err = decode_mathldbt_v1(&bytes).unwrap_err();
    assert_eq!(err, Error::Other("delta payload truncated".to_string()));
}

#[test]
fn delta_truncated_varint_is_rejected() {
    let mut bytes = build_minimal_header(2, 1);
    let validity = [0b0000_0011u8];
    // base (8 bytes) + delta varint byte with continuation bit but no continuation
    let mut payload = Vec::new();
    payload.extend_from_slice(&0i64.to_le_bytes());
    payload.push(0x80);
    push_col_descriptor(&mut bytes, 4, 3, b"", 1, &validity, &payload, &[]);
    let err = decode_mathldbt_v1(&bytes).unwrap_err();
    assert_eq!(err, Error::Other("truncated varint".to_string()));
}

#[test]
fn delta_trailing_bytes_is_rejected() {
    let mut bytes = build_minimal_header(2, 1);
    let validity = [0b0000_0011u8];
    // base + one delta varint (0) + extra 0 -> trailing bytes
    let mut payload = Vec::new();
    payload.extend_from_slice(&0i64.to_le_bytes());
    payload.push(0x00);
    payload.push(0x00);
    push_col_descriptor(&mut bytes, 4, 3, b"", 1, &validity, &payload, &[]);
    let err = decode_mathldbt_v1(&bytes).unwrap_err();
    assert_eq!(
        err,
        Error::Other("trailing bytes in delta payload".to_string())
    );
}

#[test]
fn fixed_width_payload_2_nonempty_is_rejected() {
    let mut bytes = build_minimal_header(1, 1);
    let validity = [0b0000_0001u8];
    let payload1 = [0u8; 4]; // one i32
    let payload2 = [0u8; 1]; // forbidden
    push_col_descriptor(&mut bytes, 3, 0, b"", 1, &validity, &payload1, &payload2);
    let err = decode_mathldbt_v1(&bytes).unwrap_err();
    assert_eq!(
        err,
        Error::Other("fixed-width payload_2 must be empty".to_string())
    );
}

#[test]
fn decode_into_schema_mismatch_is_rejected() {
    // Build a minimal valid bool column payload.
    let mut bytes = build_minimal_header(1, 1);
    let validity = [0b0000_0001u8];
    let payload1 = [1u8];
    push_col_descriptor(&mut bytes, 1, 0, b"a", 1, &validity, &payload1, &[]);

    let schema = ColumnarSchema::new(vec![ColumnarField {
        name: Some("different".to_string()),
        ty: ColumnarType::Bool,
    }])
    .unwrap();
    let out = ColumnarBatch::new(
        schema,
        1,
        vec![ColumnData::new_all_invalid(ColumnarType::Bool, 1).unwrap()],
    )
    .unwrap();
    let mut out = out;

    let err = decode_mathldbt_v1_into(&bytes, &mut out).unwrap_err();
    assert_eq!(
        err,
        Error::Other("decode_mathldbt_v1_into requires matching schema".to_string())
    );
}
