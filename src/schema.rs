use crate::{Error, Result};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ColumnarType {
    Bool,
    I16,
    I32,
    I64,
    F32,
    F64,
    Uuid,
    TimestampTzMicros,
    Utf8,
    Bytes,
    JsonbText,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarField {
    pub name: Option<String>,
    pub ty: ColumnarType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarSchema {
    fields: Vec<ColumnarField>,
}

impl ColumnarSchema {
    pub fn new(fields: Vec<ColumnarField>) -> Result<Self> {
        if fields.is_empty() {
            return Err(Error::Other(
                "columnar schema must have at least one field".to_string(),
            ));
        }
        Ok(Self { fields })
    }

    pub fn fields(&self) -> &[ColumnarField] {
        &self.fields
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}
