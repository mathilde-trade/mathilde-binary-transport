use crate::batch::ValidityBitmap;
use crate::schema::{ColumnarSchema, ColumnarType};
use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnarBatchView<'a> {
    pub schema: &'a ColumnarSchema,
    pub row_count: usize,
    pub columns: &'a [ColumnDataView<'a>],
}

impl<'a> ColumnarBatchView<'a> {
    pub fn validate(&self) -> Result<()> {
        if self.schema.is_empty() {
            return Err(Error::Other(
                "columnar schema must have at least one field".to_string(),
            ));
        }
        if self.schema.len() != self.columns.len() {
            return Err(Error::Other("schema/columns length mismatch".to_string()));
        }
        for (field, col) in self.schema.fields().iter().zip(self.columns.iter()) {
            col.validate_for_row_count(field.ty, self.row_count)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnDataView<'a> {
    FixedBool {
        validity: &'a [u8],
        values: &'a [u8],
    },
    FixedI16 {
        validity: &'a [u8],
        values: &'a [i16],
    },
    FixedI32 {
        validity: &'a [u8],
        values: &'a [i32],
    },
    FixedI64 {
        validity: &'a [u8],
        values: &'a [i64],
    },
    FixedF32Bits {
        validity: &'a [u8],
        values: &'a [u32],
    },
    FixedF64Bits {
        validity: &'a [u8],
        values: &'a [u64],
    },
    FixedUuid {
        validity: &'a [u8],
        values: &'a [[u8; 16]],
    },
    FixedTimestampMicros {
        validity: &'a [u8],
        values: &'a [i64],
    },
    Var {
        ty: ColumnarType,
        validity: &'a [u8],
        offsets: &'a [u32],
        data: VarDataView<'a>,
    },
}

impl<'a> ColumnDataView<'a> {
    pub fn ty(&self) -> ColumnarType {
        match self {
            ColumnDataView::FixedBool { .. } => ColumnarType::Bool,
            ColumnDataView::FixedI16 { .. } => ColumnarType::I16,
            ColumnDataView::FixedI32 { .. } => ColumnarType::I32,
            ColumnDataView::FixedI64 { .. } => ColumnarType::I64,
            ColumnDataView::FixedF32Bits { .. } => ColumnarType::F32,
            ColumnDataView::FixedF64Bits { .. } => ColumnarType::F64,
            ColumnDataView::FixedUuid { .. } => ColumnarType::Uuid,
            ColumnDataView::FixedTimestampMicros { .. } => ColumnarType::TimestampTzMicros,
            ColumnDataView::Var { ty, .. } => *ty,
        }
    }

    pub fn validate_for_row_count(&self, ty: ColumnarType, row_count: usize) -> Result<()> {
        if self.ty() != ty {
            return Err(Error::Other("column type mismatch".to_string()));
        }

        let expected_validity = ValidityBitmap::len_for_row_count(row_count)?;
        match self {
            ColumnDataView::FixedBool { validity, values } => {
                if validity.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnDataView::FixedI16 { validity, values } => {
                if validity.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnDataView::FixedI32 { validity, values } => {
                if validity.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnDataView::FixedI64 { validity, values } => {
                if validity.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnDataView::FixedF32Bits { validity, values } => {
                if validity.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnDataView::FixedF64Bits { validity, values } => {
                if validity.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnDataView::FixedUuid { validity, values } => {
                if validity.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnDataView::FixedTimestampMicros { validity, values } => {
                if validity.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnDataView::Var {
                validity,
                offsets,
                data,
                ..
            } => {
                if validity.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                let data_len = data.len()?;
                if data_len > (u32::MAX as usize) {
                    return Err(Error::Other("data too large".to_string()));
                }
                let expected_offsets_len = row_count
                    .checked_add(1)
                    .ok_or_else(|| Error::Other("row_count too large".to_string()))?;
                if offsets.len() != expected_offsets_len {
                    return Err(Error::Other("offsets length mismatch".to_string()));
                }
                if offsets.first().copied().unwrap_or(1) != 0 {
                    return Err(Error::Other("offsets[0] must be 0".to_string()));
                }
                let mut prev = 0u32;
                for &o in offsets.iter() {
                    if o < prev {
                        return Err(Error::Other("offsets must be non-decreasing".to_string()));
                    }
                    prev = o;
                }
                let final_off = offsets
                    .last()
                    .copied()
                    .ok_or_else(|| Error::Other("offsets length mismatch".to_string()))?;
                if final_off != (data_len as u32) {
                    return Err(Error::Other("final offset mismatch".to_string()));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarDataView<'a> {
    Contiguous(&'a [u8]),
    Chunks {
        inline: &'a [u8],
        chunks: &'a [&'a [u8]],
    },
}

impl<'a> VarDataView<'a> {
    pub fn len(&self) -> Result<usize> {
        match self {
            VarDataView::Contiguous(bytes) => Ok(bytes.len()),
            VarDataView::Chunks { inline, chunks } => {
                let mut total = inline.len();
                for c in *chunks {
                    total = total
                        .checked_add(c.len())
                        .ok_or_else(|| Error::Other("data too large".to_string()))?;
                }
                Ok(total)
            }
        }
    }
}

