use crate::Error;
use crate::batch::{ColumnData, ColumnarBatch, ValidityBitmap};
use crate::schema::{ColumnarField, ColumnarSchema, ColumnarType};

#[test]
fn validity_len_is_ceil_div_8() {
    assert_eq!(ValidityBitmap::len_for_row_count(0).unwrap(), 0);
    assert_eq!(ValidityBitmap::len_for_row_count(1).unwrap(), 1);
    assert_eq!(ValidityBitmap::len_for_row_count(8).unwrap(), 1);
    assert_eq!(ValidityBitmap::len_for_row_count(9).unwrap(), 2);
}

#[test]
fn batch_rejects_schema_columns_len_mismatch() {
    let schema = ColumnarSchema::new(vec![ColumnarField {
        name: Some("a".to_string()),
        ty: ColumnarType::I64,
    }])
    .unwrap();
    let err = ColumnarBatch::new(schema, 3, vec![]).unwrap_err();
    assert_eq!(
        err,
        Error::Other("schema/columns length mismatch".to_string())
    );
}

#[test]
fn fixed_column_length_must_match_row_count() {
    let schema = ColumnarSchema::new(vec![ColumnarField {
        name: None,
        ty: ColumnarType::I32,
    }])
    .unwrap();
    let col = ColumnData::FixedI32 {
        validity: ValidityBitmap::new_all_invalid(3).unwrap(),
        values: vec![0i32; 2],
    };
    let err = ColumnarBatch::new(schema, 3, vec![col]).unwrap_err();
    assert_eq!(err, Error::Other("values length mismatch".to_string()));
}

#[test]
fn var_offsets_must_be_monotonic_and_match_data_len() {
    let schema = ColumnarSchema::new(vec![ColumnarField {
        name: None,
        ty: ColumnarType::Utf8,
    }])
    .unwrap();
    let col = ColumnData::Var {
        ty: ColumnarType::Utf8,
        validity: ValidityBitmap::new_all_valid(2).unwrap(),
        offsets: vec![0, 2, 1],
        data: b"ab".to_vec(),
    };
    let err = ColumnarBatch::new(schema, 2, vec![col]).unwrap_err();
    assert_eq!(
        err,
        Error::Other("offsets must be non-decreasing".to_string())
    );

    let schema = ColumnarSchema::new(vec![ColumnarField {
        name: None,
        ty: ColumnarType::Utf8,
    }])
    .unwrap();
    let col = ColumnData::Var {
        ty: ColumnarType::Utf8,
        validity: ValidityBitmap::new_all_valid(2).unwrap(),
        offsets: vec![0, 1, 1],
        data: b"ab".to_vec(),
    };
    let err = ColumnarBatch::new(schema, 2, vec![col]).unwrap_err();
    assert_eq!(err, Error::Other("final offset mismatch".to_string()));
}
