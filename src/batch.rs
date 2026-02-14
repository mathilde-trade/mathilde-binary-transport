use crate::schema::{ColumnarSchema, ColumnarType};
use crate::{Error, Result};

fn ceil_div_8(n: usize) -> Result<usize> {
    n.checked_add(7)
        .ok_or_else(|| Error::Other("size overflow".to_string()))
        .map(|v| v / 8)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidityBitmap {
    pub bytes: Vec<u8>,
}

impl ValidityBitmap {
    pub fn new_all_invalid(row_count: usize) -> Result<Self> {
        let len = ceil_div_8(row_count)?;
        Ok(Self {
            bytes: vec![0u8; len],
        })
    }

    pub fn new_all_valid(row_count: usize) -> Result<Self> {
        let len = ceil_div_8(row_count)?;
        if len == 0 {
            return Ok(Self { bytes: Vec::new() });
        }
        let mut bytes = vec![0xFFu8; len];
        let rem = row_count % 8;
        if rem != 0 {
            let mask = (1u8 << rem) - 1;
            let last = bytes
                .last_mut()
                .ok_or_else(|| Error::Other("validity bitmap out of bounds".to_string()))?;
            *last = mask;
        }
        Ok(Self { bytes })
    }

    pub fn len_for_row_count(row_count: usize) -> Result<usize> {
        ceil_div_8(row_count)
    }

    pub fn set(&mut self, row_idx: usize, is_valid: bool) -> Result<()> {
        let byte_idx = row_idx / 8;
        let bit_idx = row_idx % 8;
        if byte_idx >= self.bytes.len() {
            return Err(Error::Other("validity bitmap out of bounds".to_string()));
        }
        let mask = 1u8 << bit_idx;
        if is_valid {
            self.bytes[byte_idx] |= mask;
        } else {
            self.bytes[byte_idx] &= !mask;
        }
        Ok(())
    }

    pub fn is_valid(&self, row_idx: usize) -> Result<bool> {
        let byte_idx = row_idx / 8;
        let bit_idx = row_idx % 8;
        if byte_idx >= self.bytes.len() {
            return Err(Error::Other("validity bitmap out of bounds".to_string()));
        }
        Ok((self.bytes[byte_idx] & (1u8 << bit_idx)) != 0)
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnData {
    FixedBool {
        validity: ValidityBitmap,
        values: Vec<u8>,
    },
    FixedI16 {
        validity: ValidityBitmap,
        values: Vec<i16>,
    },
    FixedI32 {
        validity: ValidityBitmap,
        values: Vec<i32>,
    },
    FixedI64 {
        validity: ValidityBitmap,
        values: Vec<i64>,
    },
    FixedF32Bits {
        validity: ValidityBitmap,
        values: Vec<u32>,
    },
    FixedF64Bits {
        validity: ValidityBitmap,
        values: Vec<u64>,
    },
    FixedUuid {
        validity: ValidityBitmap,
        values: Vec<[u8; 16]>,
    },
    FixedTimestampMicros {
        validity: ValidityBitmap,
        values: Vec<i64>,
    },
    Var {
        ty: ColumnarType,
        validity: ValidityBitmap,
        offsets: Vec<u32>,
        data: Vec<u8>,
    },
}

impl ColumnData {
    pub fn ty(&self) -> ColumnarType {
        match self {
            ColumnData::FixedBool { .. } => ColumnarType::Bool,
            ColumnData::FixedI16 { .. } => ColumnarType::I16,
            ColumnData::FixedI32 { .. } => ColumnarType::I32,
            ColumnData::FixedI64 { .. } => ColumnarType::I64,
            ColumnData::FixedF32Bits { .. } => ColumnarType::F32,
            ColumnData::FixedF64Bits { .. } => ColumnarType::F64,
            ColumnData::FixedUuid { .. } => ColumnarType::Uuid,
            ColumnData::FixedTimestampMicros { .. } => ColumnarType::TimestampTzMicros,
            ColumnData::Var { ty, .. } => *ty,
        }
    }

    pub fn new_all_invalid(ty: ColumnarType, row_count: usize) -> Result<Self> {
        let validity = ValidityBitmap::new_all_invalid(row_count)?;
        match ty {
            ColumnarType::Utf8 | ColumnarType::Bytes | ColumnarType::JsonbText => {
                let offsets_len = row_count
                    .checked_add(1)
                    .ok_or_else(|| Error::Other("row_count too large".to_string()))?;
                Ok(ColumnData::Var {
                    ty,
                    validity,
                    offsets: vec![0u32; offsets_len],
                    data: Vec::new(),
                })
            }
            ColumnarType::Bool => Ok(ColumnData::FixedBool {
                validity,
                values: vec![0u8; row_count],
            }),
            ColumnarType::I16 => Ok(ColumnData::FixedI16 {
                validity,
                values: vec![0i16; row_count],
            }),
            ColumnarType::I32 => Ok(ColumnData::FixedI32 {
                validity,
                values: vec![0i32; row_count],
            }),
            ColumnarType::I64 => Ok(ColumnData::FixedI64 {
                validity,
                values: vec![0i64; row_count],
            }),
            ColumnarType::F32 => Ok(ColumnData::FixedF32Bits {
                validity,
                values: vec![0u32; row_count],
            }),
            ColumnarType::F64 => Ok(ColumnData::FixedF64Bits {
                validity,
                values: vec![0u64; row_count],
            }),
            ColumnarType::Uuid => Ok(ColumnData::FixedUuid {
                validity,
                values: vec![[0u8; 16]; row_count],
            }),
            ColumnarType::TimestampTzMicros => Ok(ColumnData::FixedTimestampMicros {
                validity,
                values: vec![0i64; row_count],
            }),
        }
    }

    pub fn validate_for_row_count(&self, ty: ColumnarType, row_count: usize) -> Result<()> {
        if self.ty() != ty {
            return Err(Error::Other("column type mismatch".to_string()));
        }

        let expected_validity = ValidityBitmap::len_for_row_count(row_count)?;
        match self {
            ColumnData::FixedBool { validity, values } => {
                if validity.bytes.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnData::FixedI16 { validity, values } => {
                if validity.bytes.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnData::FixedI32 { validity, values } => {
                if validity.bytes.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnData::FixedI64 { validity, values } => {
                if validity.bytes.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnData::FixedF32Bits { validity, values } => {
                if validity.bytes.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnData::FixedF64Bits { validity, values } => {
                if validity.bytes.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnData::FixedUuid { validity, values } => {
                if validity.bytes.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnData::FixedTimestampMicros { validity, values } => {
                if validity.bytes.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if values.len() != row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
            }
            ColumnData::Var {
                validity,
                offsets,
                data,
                ..
            } => {
                if validity.bytes.len() != expected_validity {
                    return Err(Error::Other("validity length mismatch".to_string()));
                }
                if data.len() > (u32::MAX as usize) {
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
                if final_off != (data.len() as u32) {
                    return Err(Error::Other("final offset mismatch".to_string()));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarBatch {
    pub schema: ColumnarSchema,
    pub row_count: usize,
    pub columns: Vec<ColumnData>,
}

impl ColumnarBatch {
    pub fn new(schema: ColumnarSchema, row_count: usize, columns: Vec<ColumnData>) -> Result<Self> {
        let batch = Self {
            schema,
            row_count,
            columns,
        };
        batch.validate()?;
        Ok(batch)
    }

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
