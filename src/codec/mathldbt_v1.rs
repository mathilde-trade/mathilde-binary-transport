use crate::batch::{ColumnData, ColumnarBatch, ValidityBitmap};
use crate::batch_view::{ColumnarBatchView, ColumnDataView, VarDataView};
use crate::schema::{ColumnarField, ColumnarSchema, ColumnarType};
use crate::{Error, Result};
use std::collections::HashMap;

const MAGIC: &[u8; 8] = b"MATHLDBT";
const VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixedEncodingId {
    PlainLe = 0,
    PgBeFixed = 1,
}

impl FixedEncodingId {
    fn from_u16(v: u16) -> Option<Self> {
        match v {
            0 => Some(FixedEncodingId::PlainLe),
            1 => Some(FixedEncodingId::PgBeFixed),
            _ => None,
        }
    }
}

const ENC_PLAIN: u16 = 0;
const ENC_DICT_UTF8: u16 = 2;
const ENC_DELTA_VARINT_I64: u16 = 3;

fn type_id(ty: ColumnarType) -> u16 {
    match ty {
        ColumnarType::Bool => 1,
        ColumnarType::I16 => 2,
        ColumnarType::I32 => 3,
        ColumnarType::I64 => 4,
        ColumnarType::F32 => 5,
        ColumnarType::F64 => 6,
        ColumnarType::Uuid => 7,
        ColumnarType::TimestampTzMicros => 8,
        ColumnarType::Utf8 => 9,
        ColumnarType::Bytes => 10,
        ColumnarType::JsonbText => 11,
    }
}

fn type_from_id(id: u16) -> Result<ColumnarType> {
    match id {
        1 => Ok(ColumnarType::Bool),
        2 => Ok(ColumnarType::I16),
        3 => Ok(ColumnarType::I32),
        4 => Ok(ColumnarType::I64),
        5 => Ok(ColumnarType::F32),
        6 => Ok(ColumnarType::F64),
        7 => Ok(ColumnarType::Uuid),
        8 => Ok(ColumnarType::TimestampTzMicros),
        9 => Ok(ColumnarType::Utf8),
        10 => Ok(ColumnarType::Bytes),
        11 => Ok(ColumnarType::JsonbText),
        _ => Err(Error::Other(format!("unknown column type id: {id}"))),
    }
}

fn ceil_div_8(n: usize) -> Result<usize> {
    n.checked_add(7)
        .ok_or_else(|| Error::Other("size overflow".to_string()))
        .map(|v| v / 8)
}

fn write_u16_le(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn write_u32_le(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn write_u16_len_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let len: u16 = bytes
        .len()
        .try_into()
        .map_err(|_| Error::Other("name too long".to_string()))?;
    write_u16_le(out, len);
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_u32_len_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    let len: u32 = bytes
        .len()
        .try_into()
        .map_err(|_| Error::Other("payload too large".to_string()))?;
    write_u32_le(out, len);
    out.extend_from_slice(bytes);
    Ok(())
}

#[inline]
fn checked_byte_len(count: usize, elem_size: usize, err: &'static str) -> Result<usize> {
    count
        .checked_mul(elem_size)
        .ok_or_else(|| Error::Other(err.to_string()))
}

#[derive(Debug, Default, Clone)]
pub struct MathldbtV1EncodeWorkspace {
    enable_dict_utf8: bool,
    enable_delta_varint_i64: bool,

    dict_values: Vec<Vec<u8>>,
    dict_map: HashMap<Vec<u8>, u32>,
    dict_indices: Vec<u32>,
    dict_indices_bytes: Vec<u8>,
    dict_offsets: Vec<u32>,
    dict_blob: Vec<u8>,

    view_var_coalesce: Vec<u8>,

    delta_buf: Vec<u8>,
}

#[derive(Debug, Default, Clone)]
pub struct MathldbtV1DecodeWorkspace {
    dict_offsets: Vec<u32>,
}

impl MathldbtV1EncodeWorkspace {
    pub fn set_enable_dict_utf8(&mut self, enabled: bool) -> &mut Self {
        self.enable_dict_utf8 = enabled;
        self
    }

    pub fn set_enable_delta_varint_i64(&mut self, enabled: bool) -> &mut Self {
        self.enable_delta_varint_i64 = enabled;
        self
    }
}

#[inline]
fn zigzag_i64_to_u64(x: i64) -> u64 {
    ((x as u64) << 1) ^ (((x >> 63) as u64) & 1)
}

#[inline]
fn zigzag_u64_to_i64(x: u64) -> i64 {
    let v = (x >> 1) as i64;
    let neg = (x & 1) as i64;
    v ^ -neg
}

#[inline]
fn write_u64_varint(out: &mut Vec<u8>, mut x: u64) {
    while x >= 0x80 {
        out.push((x as u8) | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
}

fn read_u64_varint(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    let mut x: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= bytes.len() {
            return Err(Error::Other("truncated varint".to_string()));
        }
        let b = bytes[*pos];
        *pos += 1;
        let lo = (b & 0x7F) as u64;
        if shift >= 64 {
            return Err(Error::Other("varint overflow".to_string()));
        }
        x |= lo << shift;
        if (b & 0x80) == 0 {
            return Ok(x);
        }
        shift = shift
            .checked_add(7)
            .ok_or_else(|| Error::Other("varint overflow".to_string()))?;
    }
}

#[inline]
fn validity_all_valid(validity: &[u8], row_count: usize) -> bool {
    if row_count == 0 {
        return true;
    }
    let needed = match row_count.checked_add(7) {
        Some(v) => v / 8,
        None => return false,
    };
    if validity.len() < needed {
        return false;
    }
    if needed == 0 {
        return true;
    }
    let rem = row_count % 8;
    let full = if rem == 0 { needed } else { needed - 1 };
    for &b in &validity[..full] {
        if b != 0xFF {
            return false;
        }
    }
    if rem == 0 {
        return true;
    }
    let mask = (1u8 << rem) - 1;
    (validity[needed - 1] & mask) == mask
}

fn build_delta_varint_i64_payload<'a>(
    ws: &'a mut MathldbtV1EncodeWorkspace,
    values: &[i64],
) -> Result<Option<&'a [u8]>> {
    if values.is_empty() {
        return Ok(None);
    }
    ws.delta_buf.clear();
    ws.delta_buf.reserve(8 + values.len().saturating_mul(2));
    ws.delta_buf.extend_from_slice(&values[0].to_le_bytes());
    let mut prev = values[0];
    for &v in &values[1..] {
        let delta = v.wrapping_sub(prev);
        write_u64_varint(&mut ws.delta_buf, zigzag_i64_to_u64(delta));
        prev = v;
    }
    if ws.delta_buf.len() >= values.len() * 8 {
        return Ok(None);
    }
    Ok(Some(ws.delta_buf.as_slice()))
}

fn build_dict_utf8_payload<'a>(
    ws: &'a mut MathldbtV1EncodeWorkspace,
    validity: &[u8],
    row_count: usize,
    offsets: &[u32],
    data: &[u8],
) -> Result<Option<(&'a [u8], &'a [u8])>> {
    if row_count == 0 {
        return Ok(None);
    }
    if offsets.len() != row_count + 1 {
        return Ok(None);
    }
    if data.len() > (u32::MAX as usize) {
        return Ok(None);
    }

    ws.dict_values.clear();
    ws.dict_map.clear();
    ws.dict_indices.clear();
    ws.dict_indices.reserve(row_count);

    for row in 0..row_count {
        let is_valid = (validity[row / 8] & (1u8 << (row % 8))) != 0;
        if !is_valid {
            ws.dict_indices.push(0);
            continue;
        }
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        if end < start || end > data.len() {
            return Err(Error::Other("offset out of bounds".to_string()));
        }
        let bytes = &data[start..end];
        if let Some(&idx) = ws.dict_map.get(bytes) {
            ws.dict_indices.push(idx);
            continue;
        }
        let idx: u32 = ws
            .dict_values
            .len()
            .try_into()
            .map_err(|_| Error::Other("dict too large".to_string()))?;
        let key = bytes.to_vec();
        ws.dict_values.push(key.clone());
        ws.dict_map.insert(key, idx);
        ws.dict_indices.push(idx);
    }

    let dict_count = ws.dict_values.len();
    if dict_count == 0 {
        return Ok(None);
    }

    let index_width: usize = if dict_count <= 0x100 {
        1
    } else if dict_count <= 0x1_0000 {
        2
    } else {
        4
    };

    let indices_len = row_count
        .checked_mul(index_width)
        .ok_or_else(|| Error::Other("indices overflow".to_string()))?;

    ws.dict_indices_bytes.clear();
    ws.dict_indices_bytes.reserve(indices_len);
    match index_width {
        1 => {
            for &idx in &ws.dict_indices {
                ws.dict_indices_bytes.push(idx as u8);
            }
        }
        2 => {
            for &idx in &ws.dict_indices {
                let v: u16 = idx
                    .try_into()
                    .map_err(|_| Error::Other("dict index overflow".to_string()))?;
                ws.dict_indices_bytes.extend_from_slice(&v.to_le_bytes());
            }
        }
        4 => {
            for &idx in &ws.dict_indices {
                ws.dict_indices_bytes.extend_from_slice(&idx.to_le_bytes());
            }
        }
        _ => return Err(Error::Other("invalid index width".to_string())),
    }
    if ws.dict_indices_bytes.len() != indices_len {
        return Err(Error::Other("indices length mismatch".to_string()));
    }

    ws.dict_offsets.clear();
    ws.dict_offsets.reserve(dict_count + 1);
    ws.dict_offsets.push(0u32);
    let mut total: u32 = 0;
    for v in &ws.dict_values {
        let len_u32: u32 = v
            .len()
            .try_into()
            .map_err(|_| Error::Other("dict entry too large".to_string()))?;
        total = total
            .checked_add(len_u32)
            .ok_or_else(|| Error::Other("dict overflow".to_string()))?;
        ws.dict_offsets.push(total);
    }

    ws.dict_blob.clear();
    ws.dict_blob
        .reserve(1 + 4 + (dict_count + 1) * 4 + total as usize);
    ws.dict_blob.push(index_width as u8);
    let dict_count_u32: u32 = dict_count
        .try_into()
        .map_err(|_| Error::Other("dict too large".to_string()))?;
    write_u32_le(&mut ws.dict_blob, dict_count_u32);
    for &o in &ws.dict_offsets {
        write_u32_le(&mut ws.dict_blob, o);
    }
    for v in &ws.dict_values {
        ws.dict_blob.extend_from_slice(v.as_slice());
    }

    let plain_offsets_len = (row_count + 1)
        .checked_mul(4)
        .ok_or_else(|| Error::Other("offsets overflow".to_string()))?;
    let plain_total = plain_offsets_len
        .checked_add(data.len())
        .ok_or_else(|| Error::Other("payload overflow".to_string()))?;
    let dict_total = ws
        .dict_indices_bytes
        .len()
        .checked_add(ws.dict_blob.len())
        .ok_or_else(|| Error::Other("payload overflow".to_string()))?;
    if dict_total >= plain_total {
        return Ok(None);
    }

    Ok(Some((
        ws.dict_indices_bytes.as_slice(),
        ws.dict_blob.as_slice(),
    )))
}

fn decode_dict_utf8_to_var_col(
    ws: &mut MathldbtV1DecodeWorkspace,
    row_count: usize,
    validity: &[u8],
    indices_bytes: &[u8],
    dict_blob: &[u8],
    out_offsets: &mut Vec<u32>,
    out_data: &mut Vec<u8>,
) -> Result<()> {
    if row_count == 0 {
        out_offsets.clear();
        out_offsets.push(0);
        out_data.clear();
        return Ok(());
    }

    if dict_blob.len() < 1 + 4 {
        return Err(Error::Other("dict blob truncated".to_string()));
    }
    let index_width = dict_blob[0] as usize;
    if index_width != 1 && index_width != 2 && index_width != 4 {
        return Err(Error::Other("invalid dict index width".to_string()));
    }
    let dict_count =
        u32::from_le_bytes([dict_blob[1], dict_blob[2], dict_blob[3], dict_blob[4]]) as usize;
    let offsets_bytes_len = (dict_count + 1)
        .checked_mul(4)
        .ok_or_else(|| Error::Other("dict offsets overflow".to_string()))?;
    let header_len: usize = 1 + 4;
    let offsets_start: usize = header_len;
    let offsets_end = offsets_start
        .checked_add(offsets_bytes_len)
        .ok_or_else(|| Error::Other("dict offsets overflow".to_string()))?;
    if offsets_end > dict_blob.len() {
        return Err(Error::Other("dict offsets truncated".to_string()));
    }

    ws.dict_offsets.clear();
    ws.dict_offsets.resize(dict_count + 1, 0u32);
    #[cfg(target_endian = "little")]
    {
        let src = &dict_blob[offsets_start..offsets_end];
        let dst = unsafe {
            std::slice::from_raw_parts_mut(
                ws.dict_offsets.as_mut_ptr() as *mut u8,
                offsets_bytes_len,
            )
        };
        dst.copy_from_slice(src);
    }
    #[cfg(not(target_endian = "little"))]
    {
        for i in 0..(dict_count + 1) {
            let j = offsets_start + i * 4;
            ws.dict_offsets[i] = u32::from_le_bytes([
                dict_blob[j],
                dict_blob[j + 1],
                dict_blob[j + 2],
                dict_blob[j + 3],
            ]);
        }
    }

    let dict_bytes = &dict_blob[offsets_end..];
    let dict_total = ws.dict_offsets.last().copied().unwrap_or(0);
    if dict_total as usize != dict_bytes.len() {
        return Err(Error::Other("dict final offset mismatch".to_string()));
    }
    let mut prev = 0u32;
    for &o in ws.dict_offsets.iter() {
        if o < prev {
            return Err(Error::Other(
                "dict offsets must be non-decreasing".to_string(),
            ));
        }
        prev = o;
    }

    let expected_indices_len = row_count
        .checked_mul(index_width)
        .ok_or_else(|| Error::Other("indices overflow".to_string()))?;
    if indices_bytes.len() != expected_indices_len {
        return Err(Error::Other("indices length mismatch".to_string()));
    }

    out_offsets.clear();
    out_offsets.reserve(row_count + 1);
    out_offsets.push(0u32);
    out_data.clear();

    let mut total: u32 = 0;
    for row in 0..row_count {
        let is_valid = (validity[row / 8] & (1u8 << (row % 8))) != 0;
        if !is_valid {
            out_offsets.push(total);
            continue;
        }
        let idx: usize = match index_width {
            1 => indices_bytes[row] as usize,
            2 => {
                let j = row * 2;
                u16::from_le_bytes([indices_bytes[j], indices_bytes[j + 1]]) as usize
            }
            4 => {
                let j = row * 4;
                u32::from_le_bytes([
                    indices_bytes[j],
                    indices_bytes[j + 1],
                    indices_bytes[j + 2],
                    indices_bytes[j + 3],
                ]) as usize
            }
            _ => return Err(Error::Other("invalid index width".to_string())),
        };
        if idx >= dict_count {
            return Err(Error::Other("dict index out of bounds".to_string()));
        }
        let start = ws.dict_offsets[idx] as usize;
        let end = ws.dict_offsets[idx + 1] as usize;
        out_data.extend_from_slice(&dict_bytes[start..end]);
        let add: u32 = (end - start)
            .try_into()
            .map_err(|_| Error::Other("dict expansion too large".to_string()))?;
        total = total
            .checked_add(add)
            .ok_or_else(|| Error::Other("dict expansion overflow".to_string()))?;
        out_offsets.push(total);
    }

    Ok(())
}

fn decode_delta_varint_i64_from_payload(
    payload: &[u8],
    row_count: usize,
    out: &mut [i64],
) -> Result<()> {
    if row_count == 0 {
        return Ok(());
    }
    if out.len() < row_count {
        return Err(Error::Other("output length mismatch".to_string()));
    }
    if payload.len() < 8 {
        return Err(Error::Other("delta payload truncated".to_string()));
    }
    let mut pos = 8usize;
    let base_bytes = &payload[..8];
    let mut prev = i64::from_le_bytes([
        base_bytes[0],
        base_bytes[1],
        base_bytes[2],
        base_bytes[3],
        base_bytes[4],
        base_bytes[5],
        base_bytes[6],
        base_bytes[7],
    ]);
    out[0] = prev;
    for i in 1..row_count {
        let zz = read_u64_varint(payload, &mut pos)?;
        let delta = zigzag_u64_to_i64(zz);
        prev = prev.wrapping_add(delta);
        out[i] = prev;
    }
    if pos != payload.len() {
        return Err(Error::Other("trailing bytes in delta payload".to_string()));
    }
    Ok(())
}

pub fn encode_mathldbt_v1_into(batch: &ColumnarBatch, out: &mut Vec<u8>) -> Result<()> {
    let mut ws = MathldbtV1EncodeWorkspace::default();
    encode_mathldbt_v1_into_with_workspace(batch, out, &mut ws)
}

pub fn encode_mathldbt_v1_fast_path_into(view: &ColumnarBatchView<'_>, out: &mut Vec<u8>) -> Result<()> {
    let mut ws = MathldbtV1EncodeWorkspace::default();
    encode_mathldbt_v1_fast_path_into_with_workspace(view, out, &mut ws)
}

pub fn encode_mathldbt_v1_fast_path_into_with_workspace(
    view: &ColumnarBatchView<'_>,
    out: &mut Vec<u8>,
    ws: &mut MathldbtV1EncodeWorkspace,
) -> Result<()> {
    view.validate()?;
    out.clear();

    out.extend_from_slice(MAGIC);
    write_u16_le(out, VERSION);
    write_u16_le(out, 0); // flags

    let row_count_u32: u32 = view
        .row_count
        .try_into()
        .map_err(|_| Error::Other("row_count too large".to_string()))?;
    write_u32_le(out, row_count_u32);

    let col_count_u16: u16 = view
        .columns
        .len()
        .try_into()
        .map_err(|_| Error::Other("col_count too large".to_string()))?;
    if col_count_u16 == 0 {
        return Err(Error::Other(
            "MATHLDBT must have at least one column".to_string(),
        ));
    }
    write_u16_le(out, col_count_u16);

    write_u16_le(out, 0); // schema_id_len (v1: none)

    let expected_validity = ceil_div_8(view.row_count)?;

    for (field, col) in view.schema.fields().iter().zip(view.columns.iter()) {
        write_u16_le(out, type_id(field.ty));

        let name_bytes = field.name.as_deref().unwrap_or("").as_bytes();

        let validity: &[u8] = match col {
            ColumnDataView::FixedBool { validity, .. } => validity,
            ColumnDataView::FixedI16 { validity, .. } => validity,
            ColumnDataView::FixedI32 { validity, .. } => validity,
            ColumnDataView::FixedI64 { validity, .. } => validity,
            ColumnDataView::FixedF32Bits { validity, .. } => validity,
            ColumnDataView::FixedF64Bits { validity, .. } => validity,
            ColumnDataView::FixedUuid { validity, .. } => validity,
            ColumnDataView::FixedTimestampMicros { validity, .. } => validity,
            ColumnDataView::Var { validity, .. } => validity,
        };
        if validity.len() != expected_validity {
            return Err(Error::Other("validity length mismatch".to_string()));
        }

        let mut dict_payload: Option<(&[u8], &[u8])> = None;
        let mut delta_payload: Option<&[u8]> = None;
        let mut view_var_coalesce_to_restore: Option<Vec<u8>> = None;

        let encoding_id: u16 = match col {
            ColumnDataView::Var {
                offsets, data, ty, ..
            } if ws.enable_dict_utf8 && matches!(ty, ColumnarType::Utf8 | ColumnarType::JsonbText) =>
            {
                let maybe = match data {
                    VarDataView::Contiguous(bytes) => build_dict_utf8_payload(
                        ws,
                        validity,
                        view.row_count,
                        offsets,
                        bytes,
                    )?,
                    VarDataView::Chunks { inline, chunks } => {
                        let mut coalesced = std::mem::take(&mut ws.view_var_coalesce);
                        coalesced.clear();
                        let expected_len = offsets.last().copied().unwrap_or(0) as usize;
                        coalesced.reserve(expected_len);
                        coalesced.extend_from_slice(inline);
                        for &c in *chunks {
                            coalesced.extend_from_slice(c);
                        }
                        match build_dict_utf8_payload(
                            ws,
                            validity,
                            view.row_count,
                            offsets,
                            coalesced.as_slice(),
                        ) {
                            Ok(v) => {
                                view_var_coalesce_to_restore = Some(coalesced);
                                v
                            }
                            Err(e) => {
                                ws.view_var_coalesce = coalesced;
                                return Err(e);
                            }
                        }
                    }
                };
                if let Some((idx_bytes, dict_blob)) = maybe {
                    dict_payload = Some((idx_bytes, dict_blob));
                    ENC_DICT_UTF8
                } else {
                    ENC_PLAIN
                }
            }
            ColumnDataView::FixedI64 { values, .. }
                if ws.enable_delta_varint_i64
                    && field.ty == ColumnarType::I64
                    && validity_all_valid(validity, view.row_count) =>
            {
                if let Some(payload) = build_delta_varint_i64_payload(ws, values)? {
                    delta_payload = Some(payload);
                    ENC_DELTA_VARINT_I64
                } else {
                    ENC_PLAIN
                }
            }
            ColumnDataView::FixedTimestampMicros { values, .. }
                if ws.enable_delta_varint_i64
                    && field.ty == ColumnarType::TimestampTzMicros
                    && validity_all_valid(validity, view.row_count) =>
            {
                if let Some(payload) = build_delta_varint_i64_payload(ws, values)? {
                    delta_payload = Some(payload);
                    ENC_DELTA_VARINT_I64
                } else {
                    ENC_PLAIN
                }
            }
            ColumnDataView::Var { .. } => ENC_PLAIN,
            _ => FixedEncodingId::PlainLe as u16,
        };

        write_u16_le(out, encoding_id);
        write_u16_le(out, 0); // col_flags
        write_u16_len_bytes(out, name_bytes)?;
        write_u32_len_bytes(out, validity)?;

        match col {
            ColumnDataView::FixedBool { values, .. } => {
                if values.len() != view.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                write_u32_len_bytes(out, values)?;
                write_u32_le(out, 0);
            }
            ColumnDataView::FixedI16 { values, .. } => {
                if values.len() != view.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                let byte_len = checked_byte_len(view.row_count, 2, "values overflow")?;
                write_u32_le(
                    out,
                    byte_len
                        .try_into()
                        .map_err(|_| Error::Other("payload too large".to_string()))?,
                );
                out.reserve(byte_len);
                #[cfg(target_endian = "little")]
                {
                    let values_bytes =
                        unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len) };
                    out.extend_from_slice(values_bytes);
                }
                #[cfg(not(target_endian = "little"))]
                {
                    for &v in *values {
                        out.extend_from_slice(&v.to_le_bytes());
                    }
                }
                write_u32_le(out, 0);
            }
            ColumnDataView::FixedI32 { values, .. } => {
                if values.len() != view.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                let byte_len = checked_byte_len(view.row_count, 4, "values overflow")?;
                write_u32_le(
                    out,
                    byte_len
                        .try_into()
                        .map_err(|_| Error::Other("payload too large".to_string()))?,
                );
                out.reserve(byte_len);
                #[cfg(target_endian = "little")]
                {
                    let values_bytes =
                        unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len) };
                    out.extend_from_slice(values_bytes);
                }
                #[cfg(not(target_endian = "little"))]
                {
                    for &v in *values {
                        out.extend_from_slice(&v.to_le_bytes());
                    }
                }
                write_u32_le(out, 0);
            }
            ColumnDataView::FixedI64 { values, .. } => {
                if values.len() != view.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                if encoding_id == ENC_DELTA_VARINT_I64 {
                    let payload =
                        delta_payload.ok_or_else(|| Error::Other("missing delta payload".to_string()))?;
                    write_u32_len_bytes(out, payload)?;
                    write_u32_le(out, 0);
                } else {
                    let byte_len = checked_byte_len(view.row_count, 8, "values overflow")?;
                    write_u32_le(
                        out,
                        byte_len
                            .try_into()
                            .map_err(|_| Error::Other("payload too large".to_string()))?,
                    );
                    out.reserve(byte_len);
                    #[cfg(target_endian = "little")]
                    {
                        let values_bytes = unsafe {
                            std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len)
                        };
                        out.extend_from_slice(values_bytes);
                    }
                    #[cfg(not(target_endian = "little"))]
                    {
                        for &v in *values {
                            out.extend_from_slice(&v.to_le_bytes());
                        }
                    }
                    write_u32_le(out, 0);
                }
            }
            ColumnDataView::FixedF32Bits { values, .. } => {
                if values.len() != view.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                let byte_len = checked_byte_len(view.row_count, 4, "values overflow")?;
                write_u32_le(
                    out,
                    byte_len
                        .try_into()
                        .map_err(|_| Error::Other("payload too large".to_string()))?,
                );
                out.reserve(byte_len);
                #[cfg(target_endian = "little")]
                {
                    let values_bytes =
                        unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len) };
                    out.extend_from_slice(values_bytes);
                }
                #[cfg(not(target_endian = "little"))]
                {
                    for &v in *values {
                        out.extend_from_slice(&v.to_le_bytes());
                    }
                }
                write_u32_le(out, 0);
            }
            ColumnDataView::FixedF64Bits { values, .. } => {
                if values.len() != view.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                let byte_len = checked_byte_len(view.row_count, 8, "values overflow")?;
                write_u32_le(
                    out,
                    byte_len
                        .try_into()
                        .map_err(|_| Error::Other("payload too large".to_string()))?,
                );
                out.reserve(byte_len);
                #[cfg(target_endian = "little")]
                {
                    let values_bytes =
                        unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len) };
                    out.extend_from_slice(values_bytes);
                }
                #[cfg(not(target_endian = "little"))]
                {
                    for &v in *values {
                        out.extend_from_slice(&v.to_le_bytes());
                    }
                }
                write_u32_le(out, 0);
            }
            ColumnDataView::FixedUuid { values, .. } => {
                if values.len() != view.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                let byte_len = checked_byte_len(view.row_count, 16, "values overflow")?;
                write_u32_le(
                    out,
                    byte_len
                        .try_into()
                        .map_err(|_| Error::Other("payload too large".to_string()))?,
                );
                out.reserve(byte_len);
                let values_bytes =
                    unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len) };
                out.extend_from_slice(values_bytes);
                write_u32_le(out, 0);
            }
            ColumnDataView::FixedTimestampMicros { values, .. } => {
                if values.len() != view.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                if encoding_id == ENC_DELTA_VARINT_I64 {
                    let payload =
                        delta_payload.ok_or_else(|| Error::Other("missing delta payload".to_string()))?;
                    write_u32_len_bytes(out, payload)?;
                    write_u32_le(out, 0);
                } else {
                    let byte_len = checked_byte_len(view.row_count, 8, "values overflow")?;
                    write_u32_le(
                        out,
                        byte_len
                            .try_into()
                            .map_err(|_| Error::Other("payload too large".to_string()))?,
                    );
                    out.reserve(byte_len);
                    #[cfg(target_endian = "little")]
                    {
                        let values_bytes = unsafe {
                            std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len)
                        };
                        out.extend_from_slice(values_bytes);
                    }
                    #[cfg(not(target_endian = "little"))]
                    {
                        for &v in *values {
                            out.extend_from_slice(&v.to_le_bytes());
                        }
                    }
                    write_u32_le(out, 0);
                }
            }
            ColumnDataView::Var {
                ty, offsets, data, ..
            } => match encoding_id {
                ENC_PLAIN => {
                    if *ty != field.ty {
                        return Err(Error::Other("internal type mismatch".to_string()));
                    }
                    if offsets.len()
                        != view
                            .row_count
                            .checked_add(1)
                            .ok_or_else(|| Error::Other("row_count too large".to_string()))?
                    {
                        return Err(Error::Other("offsets length mismatch".to_string()));
                    }
                    let offsets_bytes_len =
                        checked_byte_len(view.row_count + 1, 4, "offsets overflow")?;
                    write_u32_le(
                        out,
                        offsets_bytes_len
                            .try_into()
                            .map_err(|_| Error::Other("payload too large".to_string()))?,
                    );
                    out.reserve(offsets_bytes_len);
                    #[cfg(target_endian = "little")]
                    {
                        let offsets_bytes = unsafe {
                            std::slice::from_raw_parts(offsets.as_ptr() as *const u8, offsets_bytes_len)
                        };
                        out.extend_from_slice(offsets_bytes);
                    }
                    #[cfg(not(target_endian = "little"))]
                    {
                        for &o in *offsets {
                            out.extend_from_slice(&o.to_le_bytes());
                        }
                    }

                    match data {
                        VarDataView::Contiguous(bytes) => {
                            write_u32_len_bytes(out, bytes)?;
                        }
                        VarDataView::Chunks { inline, chunks } => {
                            let data_len = data.len()?;
                            let data_len_u32: u32 = data_len
                                .try_into()
                                .map_err(|_| Error::Other("payload too large".to_string()))?;
                            write_u32_le(out, data_len_u32);
                            out.reserve(data_len);
                            out.extend_from_slice(inline);
                            for &c in *chunks {
                                out.extend_from_slice(c);
                            }
                        }
                    }
                }
                ENC_DICT_UTF8 => {
                    if *ty != field.ty {
                        return Err(Error::Other("internal type mismatch".to_string()));
                    }
                    let (idx_bytes, dict_blob) = dict_payload
                        .ok_or_else(|| Error::Other("missing dict payload".to_string()))?;
                    write_u32_len_bytes(out, idx_bytes)?;
                    write_u32_len_bytes(out, dict_blob)?;
                }
                _ => {
                    return Err(Error::Other(
                        "invalid encoding for varlen column".to_string(),
                    ));
                }
            },
        }

        if let Some(coalesced) = view_var_coalesce_to_restore {
            ws.view_var_coalesce = coalesced;
        }
    }

    Ok(())
}

pub fn encode_mathldbt_v1_fast_path_into_opt(
    view: &ColumnarBatchView<'_>,
    out: &mut Vec<u8>,
) -> Result<()> {
    let mut ws = MathldbtV1EncodeWorkspace::default();
    ws.set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);
    encode_mathldbt_v1_fast_path_into_with_workspace(view, out, &mut ws)
}

pub fn encode_mathldbt_v1_fast_path_into_opt_with_workspace(
    view: &ColumnarBatchView<'_>,
    out: &mut Vec<u8>,
    ws: &mut MathldbtV1EncodeWorkspace,
) -> Result<()> {
    ws.set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);
    encode_mathldbt_v1_fast_path_into_with_workspace(view, out, ws)
}

pub fn encode_mathldbt_v1_into_with_workspace(
    batch: &ColumnarBatch,
    out: &mut Vec<u8>,
    ws: &mut MathldbtV1EncodeWorkspace,
) -> Result<()> {
    batch.validate()?;
    out.clear();

    out.extend_from_slice(MAGIC);
    write_u16_le(out, VERSION);
    write_u16_le(out, 0); // flags

    let row_count_u32: u32 = batch
        .row_count
        .try_into()
        .map_err(|_| Error::Other("row_count too large".to_string()))?;
    write_u32_le(out, row_count_u32);

    let col_count_u16: u16 = batch
        .columns
        .len()
        .try_into()
        .map_err(|_| Error::Other("col_count too large".to_string()))?;
    if col_count_u16 == 0 {
        return Err(Error::Other(
            "MATHLDBT must have at least one column".to_string(),
        ));
    }
    write_u16_le(out, col_count_u16);

    write_u16_le(out, 0); // schema_id_len (v1: none)

    let expected_validity = ceil_div_8(batch.row_count)?;

    for (field, col) in batch.schema.fields().iter().zip(batch.columns.iter()) {
        write_u16_le(out, type_id(field.ty));

        let name_bytes = field.name.as_deref().unwrap_or("").as_bytes();

        let validity = match col {
            ColumnData::FixedBool { validity, .. } => validity.as_bytes(),
            ColumnData::FixedI16 { validity, .. } => validity.as_bytes(),
            ColumnData::FixedI32 { validity, .. } => validity.as_bytes(),
            ColumnData::FixedI64 { validity, .. } => validity.as_bytes(),
            ColumnData::FixedF32Bits { validity, .. } => validity.as_bytes(),
            ColumnData::FixedF64Bits { validity, .. } => validity.as_bytes(),
            ColumnData::FixedUuid { validity, .. } => validity.as_bytes(),
            ColumnData::FixedTimestampMicros { validity, .. } => validity.as_bytes(),
            ColumnData::Var { validity, .. } => validity.as_bytes(),
        };
        if validity.len() != expected_validity {
            return Err(Error::Other("validity length mismatch".to_string()));
        }

        let mut dict_payload: Option<(&[u8], &[u8])> = None;
        let mut delta_payload: Option<&[u8]> = None;

        let encoding_id: u16 = match col {
            ColumnData::Var {
                offsets, data, ty, ..
            } if ws.enable_dict_utf8
                && matches!(ty, ColumnarType::Utf8 | ColumnarType::JsonbText) =>
            {
                if let Some((idx_bytes, dict_blob)) = build_dict_utf8_payload(
                    ws,
                    validity,
                    batch.row_count,
                    offsets.as_slice(),
                    data.as_slice(),
                )? {
                    dict_payload = Some((idx_bytes, dict_blob));
                    ENC_DICT_UTF8
                } else {
                    ENC_PLAIN
                }
            }
            ColumnData::FixedI64 { values, .. }
                if ws.enable_delta_varint_i64
                    && field.ty == ColumnarType::I64
                    && validity_all_valid(validity, batch.row_count) =>
            {
                if let Some(payload) = build_delta_varint_i64_payload(ws, values.as_slice())? {
                    delta_payload = Some(payload);
                    ENC_DELTA_VARINT_I64
                } else {
                    ENC_PLAIN
                }
            }
            ColumnData::FixedTimestampMicros { values, .. }
                if ws.enable_delta_varint_i64
                    && field.ty == ColumnarType::TimestampTzMicros
                    && validity_all_valid(validity, batch.row_count) =>
            {
                if let Some(payload) = build_delta_varint_i64_payload(ws, values.as_slice())? {
                    delta_payload = Some(payload);
                    ENC_DELTA_VARINT_I64
                } else {
                    ENC_PLAIN
                }
            }
            ColumnData::Var { .. } => ENC_PLAIN,
            _ => FixedEncodingId::PlainLe as u16,
        };

        write_u16_le(out, encoding_id);
        write_u16_le(out, 0); // col_flags
        write_u16_len_bytes(out, name_bytes)?;
        write_u32_len_bytes(out, validity)?;

        match col {
            ColumnData::FixedBool { values, .. } => {
                if values.len() != batch.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                write_u32_len_bytes(out, values.as_slice())?;
                write_u32_le(out, 0);
            }
            ColumnData::FixedI16 { values, .. } => {
                if values.len() != batch.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                let byte_len = checked_byte_len(batch.row_count, 2, "values overflow")?;
                write_u32_le(
                    out,
                    byte_len
                        .try_into()
                        .map_err(|_| Error::Other("payload too large".to_string()))?,
                );
                out.reserve(byte_len);
                #[cfg(target_endian = "little")]
                {
                    let values_bytes = unsafe {
                        std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len)
                    };
                    out.extend_from_slice(values_bytes);
                }
                #[cfg(not(target_endian = "little"))]
                {
                    for &v in values {
                        out.extend_from_slice(&v.to_le_bytes());
                    }
                }
                write_u32_le(out, 0);
            }
            ColumnData::FixedI32 { values, .. } => {
                if values.len() != batch.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                let byte_len = checked_byte_len(batch.row_count, 4, "values overflow")?;
                write_u32_le(
                    out,
                    byte_len
                        .try_into()
                        .map_err(|_| Error::Other("payload too large".to_string()))?,
                );
                out.reserve(byte_len);
                #[cfg(target_endian = "little")]
                {
                    let values_bytes = unsafe {
                        std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len)
                    };
                    out.extend_from_slice(values_bytes);
                }
                #[cfg(not(target_endian = "little"))]
                {
                    for &v in values {
                        out.extend_from_slice(&v.to_le_bytes());
                    }
                }
                write_u32_le(out, 0);
            }
            ColumnData::FixedI64 { values, .. } => {
                if values.len() != batch.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                if encoding_id == ENC_DELTA_VARINT_I64 {
                    let payload = delta_payload
                        .ok_or_else(|| Error::Other("missing delta payload".to_string()))?;
                    write_u32_len_bytes(out, payload)?;
                    write_u32_le(out, 0);
                } else {
                    let byte_len = checked_byte_len(batch.row_count, 8, "values overflow")?;
                    write_u32_le(
                        out,
                        byte_len
                            .try_into()
                            .map_err(|_| Error::Other("payload too large".to_string()))?,
                    );
                    out.reserve(byte_len);
                    #[cfg(target_endian = "little")]
                    {
                        let values_bytes = unsafe {
                            std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len)
                        };
                        out.extend_from_slice(values_bytes);
                    }
                    #[cfg(not(target_endian = "little"))]
                    {
                        for &v in values {
                            out.extend_from_slice(&v.to_le_bytes());
                        }
                    }
                    write_u32_le(out, 0);
                }
            }
            ColumnData::FixedF32Bits { values, .. } => {
                if values.len() != batch.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                let byte_len = checked_byte_len(batch.row_count, 4, "values overflow")?;
                write_u32_le(
                    out,
                    byte_len
                        .try_into()
                        .map_err(|_| Error::Other("payload too large".to_string()))?,
                    );
                out.reserve(byte_len);
                #[cfg(target_endian = "little")]
                {
                    let values_bytes = unsafe {
                        std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len)
                    };
                    out.extend_from_slice(values_bytes);
                }
                #[cfg(not(target_endian = "little"))]
                {
                    for &v in values {
                        out.extend_from_slice(&v.to_le_bytes());
                    }
                }
                write_u32_le(out, 0);
            }
            ColumnData::FixedF64Bits { values, .. } => {
                if values.len() != batch.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                let byte_len = checked_byte_len(batch.row_count, 8, "values overflow")?;
                write_u32_le(
                    out,
                    byte_len
                        .try_into()
                        .map_err(|_| Error::Other("payload too large".to_string()))?,
                );
                out.reserve(byte_len);
                #[cfg(target_endian = "little")]
                {
                    let values_bytes = unsafe {
                        std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len)
                    };
                    out.extend_from_slice(values_bytes);
                }
                #[cfg(not(target_endian = "little"))]
                {
                    for &v in values {
                        out.extend_from_slice(&v.to_le_bytes());
                    }
                }
                write_u32_le(out, 0);
            }
            ColumnData::FixedUuid { values, .. } => {
                if values.len() != batch.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                let byte_len = checked_byte_len(batch.row_count, 16, "values overflow")?;
                write_u32_le(
                    out,
                    byte_len
                        .try_into()
                        .map_err(|_| Error::Other("payload too large".to_string()))?,
                    );
                out.reserve(byte_len);
                let values_bytes =
                    unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len) };
                out.extend_from_slice(values_bytes);
                write_u32_le(out, 0);
            }
            ColumnData::FixedTimestampMicros { values, .. } => {
                if values.len() != batch.row_count {
                    return Err(Error::Other("values length mismatch".to_string()));
                }
                if encoding_id == ENC_DELTA_VARINT_I64 {
                    let payload = delta_payload
                        .ok_or_else(|| Error::Other("missing delta payload".to_string()))?;
                    write_u32_len_bytes(out, payload)?;
                    write_u32_le(out, 0);
                } else {
                    let byte_len = checked_byte_len(batch.row_count, 8, "values overflow")?;
                    write_u32_le(
                        out,
                        byte_len
                            .try_into()
                            .map_err(|_| Error::Other("payload too large".to_string()))?,
                    );
                    out.reserve(byte_len);
                    #[cfg(target_endian = "little")]
                    {
                        let values_bytes = unsafe {
                            std::slice::from_raw_parts(values.as_ptr() as *const u8, byte_len)
                        };
                        out.extend_from_slice(values_bytes);
                    }
                    #[cfg(not(target_endian = "little"))]
                    {
                        for &v in values {
                            out.extend_from_slice(&v.to_le_bytes());
                        }
                    }
                    write_u32_le(out, 0);
                }
            }
            ColumnData::Var {
                ty, offsets, data, ..
            } => match encoding_id {
                ENC_PLAIN => {
                    if *ty != field.ty {
                        return Err(Error::Other("internal type mismatch".to_string()));
                    }
                    if offsets.len()
                        != batch
                            .row_count
                            .checked_add(1)
                            .ok_or_else(|| Error::Other("row_count too large".to_string()))?
                    {
                        return Err(Error::Other("offsets length mismatch".to_string()));
                    }
                    let offsets_bytes_len =
                        checked_byte_len(batch.row_count + 1, 4, "offsets overflow")?;
                    write_u32_le(
                        out,
                        offsets_bytes_len
                            .try_into()
                            .map_err(|_| Error::Other("payload too large".to_string()))?,
                    );
                    out.reserve(offsets_bytes_len);
                    #[cfg(target_endian = "little")]
                    {
                        let offsets_bytes = unsafe {
                            std::slice::from_raw_parts(
                                offsets.as_ptr() as *const u8,
                                offsets_bytes_len,
                            )
                        };
                        out.extend_from_slice(offsets_bytes);
                    }
                    #[cfg(not(target_endian = "little"))]
                    {
                        for &o in offsets {
                            out.extend_from_slice(&o.to_le_bytes());
                        }
                    }
                    write_u32_len_bytes(out, data.as_slice())?;
                }
                ENC_DICT_UTF8 => {
                    if *ty != field.ty {
                        return Err(Error::Other("internal type mismatch".to_string()));
                    }
                    let (idx_bytes, dict_blob) = dict_payload
                        .ok_or_else(|| Error::Other("missing dict payload".to_string()))?;
                    write_u32_len_bytes(out, idx_bytes)?;
                    write_u32_len_bytes(out, dict_blob)?;
                }
                _ => {
                    return Err(Error::Other(
                        "invalid encoding for varlen column".to_string(),
                    ));
                }
            },
        }
    }

    Ok(())
}

pub fn decode_mathldbt_v1(bytes: &[u8]) -> Result<ColumnarBatch> {
    let mut ws = MathldbtV1DecodeWorkspace::default();
    decode_mathldbt_v1_with_workspace(bytes, &mut ws)
}

pub fn decode_mathldbt_v1_with_workspace(
    bytes: &[u8],
    ws: &mut MathldbtV1DecodeWorkspace,
) -> Result<ColumnarBatch> {
    let mut pos = 0usize;

    fn take<'a>(bytes: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
        let end = pos
            .checked_add(n)
            .ok_or_else(|| Error::Other("decode overflow".to_string()))?;
        if end > bytes.len() {
            return Err(Error::Other("truncated mathldbt".to_string()));
        }
        let slice = &bytes[*pos..end];
        *pos = end;
        Ok(slice)
    }

    fn read_u16_le(bytes: &[u8], pos: &mut usize) -> Result<u16> {
        let b = take(bytes, pos, 2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    fn read_u32_le(bytes: &[u8], pos: &mut usize) -> Result<u32> {
        let b = take(bytes, pos, 4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    let magic = take(bytes, &mut pos, 8)?;
    if magic != MAGIC {
        return Err(Error::Other("invalid MATHLDBT magic".to_string()));
    }
    let version = read_u16_le(bytes, &mut pos)?;
    if version != VERSION {
        return Err(Error::Other(format!(
            "unsupported MATHLDBT version: {version}"
        )));
    }
    let _flags = read_u16_le(bytes, &mut pos)?;
    let row_count = read_u32_le(bytes, &mut pos)? as usize;
    let col_count = read_u16_le(bytes, &mut pos)? as usize;
    if col_count == 0 {
        return Err(Error::Other(
            "MATHLDBT must have at least one column".to_string(),
        ));
    }

    let schema_id_len = read_u16_le(bytes, &mut pos)? as usize;
    if schema_id_len > 0 {
        let _ = take(bytes, &mut pos, schema_id_len)?;
    }

    let expected_validity = ceil_div_8(row_count)?;
    let mut fields: Vec<ColumnarField> = Vec::with_capacity(col_count);
    let mut columns: Vec<ColumnData> = Vec::with_capacity(col_count);

    for _ in 0..col_count {
        let tid = read_u16_le(bytes, &mut pos)?;
        let ty = type_from_id(tid)?;
        let encoding_id_u16 = read_u16_le(bytes, &mut pos)?;
        let _col_flags = read_u16_le(bytes, &mut pos)?;

        let name_len = read_u16_le(bytes, &mut pos)? as usize;
        let name_bytes = take(bytes, &mut pos, name_len)?;
        let name = if name_len == 0 {
            None
        } else {
            Some(
                std::str::from_utf8(name_bytes)
                    .map_err(|_| Error::Other("invalid UTF-8 column name".to_string()))?
                    .to_string(),
            )
        };

        let validity_len = read_u32_le(bytes, &mut pos)? as usize;
        if validity_len != expected_validity {
            return Err(Error::Other("validity length mismatch".to_string()));
        }
        let validity_bytes = take(bytes, &mut pos, validity_len)?.to_vec();

        let payload1_len = read_u32_le(bytes, &mut pos)? as usize;
        let payload1 = take(bytes, &mut pos, payload1_len)?;
        let payload2_len = read_u32_le(bytes, &mut pos)? as usize;
        let payload2 = take(bytes, &mut pos, payload2_len)?;

        fields.push(ColumnarField { name, ty });

        match ty {
            ColumnarType::Utf8 | ColumnarType::Bytes | ColumnarType::JsonbText => {
                match encoding_id_u16 {
                    ENC_PLAIN => {
                        let expected_offsets_len = (row_count + 1)
                            .checked_mul(4)
                            .ok_or_else(|| Error::Other("offsets overflow".to_string()))?;
                        if payload1.len() != expected_offsets_len {
                            return Err(Error::Other("offsets length mismatch".to_string()));
                        }
                        let mut offsets: Vec<u32> = Vec::new();
                        offsets.resize(row_count + 1, 0u32);
                        #[cfg(target_endian = "little")]
                        {
                            let dst = unsafe {
                                std::slice::from_raw_parts_mut(
                                    offsets.as_mut_ptr() as *mut u8,
                                    expected_offsets_len,
                                )
                            };
                            dst.copy_from_slice(payload1);
                        }
                        #[cfg(not(target_endian = "little"))]
                        {
                            for i in 0..(row_count + 1) {
                                let j = i * 4;
                                offsets[i] = u32::from_le_bytes([
                                    payload1[j],
                                    payload1[j + 1],
                                    payload1[j + 2],
                                    payload1[j + 3],
                                ]);
                            }
                        }
                        let mut prev = 0u32;
                        for &o in &offsets {
                            if o < prev {
                                return Err(Error::Other(
                                    "offsets must be non-decreasing".to_string(),
                                ));
                            }
                            prev = o;
                        }
                        let final_off = *offsets.last().unwrap_or(&0);
                        let data_len_u32: u32 = payload2
                            .len()
                            .try_into()
                            .map_err(|_| Error::Other("data too large".to_string()))?;
                        if final_off != data_len_u32 {
                            return Err(Error::Other("final offset mismatch".to_string()));
                        }

                        let validity = ValidityBitmap {
                            bytes: validity_bytes,
                        };
                        columns.push(ColumnData::Var {
                            ty,
                            validity,
                            offsets,
                            data: payload2.to_vec(),
                        });
                    }
                    ENC_DICT_UTF8 => {
                        if ty == ColumnarType::Bytes {
                            return Err(Error::Other(
                                "DictUtf8 is not supported for Bytes".to_string(),
                            ));
                        }
                        let mut offsets: Vec<u32> = Vec::new();
                        let mut data: Vec<u8> = Vec::new();
                        decode_dict_utf8_to_var_col(
                            ws,
                            row_count,
                            validity_bytes.as_slice(),
                            payload1,
                            payload2,
                            &mut offsets,
                            &mut data,
                        )?;
                        let validity = ValidityBitmap {
                            bytes: validity_bytes,
                        };
                        columns.push(ColumnData::Var {
                            ty,
                            validity,
                            offsets,
                            data,
                        });
                    }
                    _ => {
                        return Err(Error::Other(
                            "invalid encoding for varlen column".to_string(),
                        ));
                    }
                }
            }
            _ => {
                if encoding_id_u16 == ENC_DELTA_VARINT_I64
                    && !(ty == ColumnarType::I64 || ty == ColumnarType::TimestampTzMicros)
                {
                    return Err(Error::Other(
                        "invalid encoding for fixed column".to_string(),
                    ));
                }
                let enc = if encoding_id_u16 == ENC_DELTA_VARINT_I64 {
                    FixedEncodingId::PlainLe
                } else {
                    FixedEncodingId::from_u16(encoding_id_u16).ok_or_else(|| {
                        Error::Other("invalid encoding for fixed column".to_string())
                    })?
                };
                if payload2_len != 0 {
                    return Err(Error::Other(
                        "fixed-width payload_2 must be empty".to_string(),
                    ));
                }

                let validity = ValidityBitmap {
                    bytes: validity_bytes,
                };

                match ty {
                    ColumnarType::Bool => {
                        if payload1.len() != row_count {
                            return Err(Error::Other("values length mismatch".to_string()));
                        }
                        columns.push(ColumnData::FixedBool {
                            validity,
                            values: payload1.to_vec(),
                        });
                    }
                    ColumnarType::I16 => {
                        if payload1.len() != row_count * 2 {
                            return Err(Error::Other("values length mismatch".to_string()));
                        }
                        let mut values: Vec<i16> = vec![0i16; row_count];
                        match enc {
                            FixedEncodingId::PlainLe => {
                                #[cfg(target_endian = "little")]
                                {
                                    let dst = unsafe {
                                        std::slice::from_raw_parts_mut(
                                            values.as_mut_ptr() as *mut u8,
                                            row_count * 2,
                                        )
                                    };
                                    dst.copy_from_slice(payload1);
                                }
                                #[cfg(not(target_endian = "little"))]
                                {
                                    for i in 0..row_count {
                                        let j = i * 2;
                                        let b = [payload1[j], payload1[j + 1]];
                                        values[i] = i16::from_le_bytes(b);
                                    }
                                }
                            }
                            FixedEncodingId::PgBeFixed => {
                                for i in 0..row_count {
                                    let j = i * 2;
                                    let b = [payload1[j], payload1[j + 1]];
                                    values[i] = i16::from_be_bytes(b);
                                }
                            }
                        }
                        columns.push(ColumnData::FixedI16 { validity, values });
                    }
                    ColumnarType::I32 => {
                        if payload1.len() != row_count * 4 {
                            return Err(Error::Other("values length mismatch".to_string()));
                        }
                        let mut values: Vec<i32> = vec![0i32; row_count];
                        match enc {
                            FixedEncodingId::PlainLe => {
                                #[cfg(target_endian = "little")]
                                {
                                    let dst = unsafe {
                                        std::slice::from_raw_parts_mut(
                                            values.as_mut_ptr() as *mut u8,
                                            row_count * 4,
                                        )
                                    };
                                    dst.copy_from_slice(payload1);
                                }
                                #[cfg(not(target_endian = "little"))]
                                {
                                    for i in 0..row_count {
                                        let j = i * 4;
                                        let b = [
                                            payload1[j],
                                            payload1[j + 1],
                                            payload1[j + 2],
                                            payload1[j + 3],
                                        ];
                                        values[i] = i32::from_le_bytes(b);
                                    }
                                }
                            }
                            FixedEncodingId::PgBeFixed => {
                                for i in 0..row_count {
                                    let j = i * 4;
                                    let b = [
                                        payload1[j],
                                        payload1[j + 1],
                                        payload1[j + 2],
                                        payload1[j + 3],
                                    ];
                                    values[i] = i32::from_be_bytes(b);
                                }
                            }
                        }
                        columns.push(ColumnData::FixedI32 { validity, values });
                    }
                    ColumnarType::I64 => {
                        let mut values: Vec<i64> = vec![0i64; row_count];
                        if encoding_id_u16 == ENC_DELTA_VARINT_I64 {
                            decode_delta_varint_i64_from_payload(
                                payload1,
                                row_count,
                                values.as_mut_slice(),
                            )?;
                        } else {
                            if payload1.len() != row_count * 8 {
                                return Err(Error::Other("values length mismatch".to_string()));
                            }
                            match enc {
                                FixedEncodingId::PlainLe => {
                                    #[cfg(target_endian = "little")]
                                    {
                                        let dst = unsafe {
                                            std::slice::from_raw_parts_mut(
                                                values.as_mut_ptr() as *mut u8,
                                                row_count * 8,
                                            )
                                        };
                                        dst.copy_from_slice(payload1);
                                    }
                                    #[cfg(not(target_endian = "little"))]
                                    {
                                        for i in 0..row_count {
                                            let j = i * 8;
                                            let b = [
                                                payload1[j],
                                                payload1[j + 1],
                                                payload1[j + 2],
                                                payload1[j + 3],
                                                payload1[j + 4],
                                                payload1[j + 5],
                                                payload1[j + 6],
                                                payload1[j + 7],
                                            ];
                                            values[i] = i64::from_le_bytes(b);
                                        }
                                    }
                                }
                                FixedEncodingId::PgBeFixed => {
                                    for i in 0..row_count {
                                        let j = i * 8;
                                        let b = [
                                            payload1[j],
                                            payload1[j + 1],
                                            payload1[j + 2],
                                            payload1[j + 3],
                                            payload1[j + 4],
                                            payload1[j + 5],
                                            payload1[j + 6],
                                            payload1[j + 7],
                                        ];
                                        values[i] = i64::from_be_bytes(b);
                                    }
                                }
                            }
                        }
                        columns.push(ColumnData::FixedI64 { validity, values });
                    }
                    ColumnarType::F32 => {
                        if payload1.len() != row_count * 4 {
                            return Err(Error::Other("values length mismatch".to_string()));
                        }
                        let mut values: Vec<u32> = vec![0u32; row_count];
                        match enc {
                            FixedEncodingId::PlainLe => {
                                #[cfg(target_endian = "little")]
                                {
                                    let dst = unsafe {
                                        std::slice::from_raw_parts_mut(
                                            values.as_mut_ptr() as *mut u8,
                                            row_count * 4,
                                        )
                                    };
                                    dst.copy_from_slice(payload1);
                                }
                                #[cfg(not(target_endian = "little"))]
                                {
                                    for i in 0..row_count {
                                        let j = i * 4;
                                        let b = [
                                            payload1[j],
                                            payload1[j + 1],
                                            payload1[j + 2],
                                            payload1[j + 3],
                                        ];
                                        values[i] = u32::from_le_bytes(b);
                                    }
                                }
                            }
                            FixedEncodingId::PgBeFixed => {
                                for i in 0..row_count {
                                    let j = i * 4;
                                    let b = [
                                        payload1[j],
                                        payload1[j + 1],
                                        payload1[j + 2],
                                        payload1[j + 3],
                                    ];
                                    values[i] = u32::from_be_bytes(b);
                                }
                            }
                        }
                        columns.push(ColumnData::FixedF32Bits { validity, values });
                    }
                    ColumnarType::F64 => {
                        if payload1.len() != row_count * 8 {
                            return Err(Error::Other("values length mismatch".to_string()));
                        }
                        let mut values: Vec<u64> = vec![0u64; row_count];
                        match enc {
                            FixedEncodingId::PlainLe => {
                                #[cfg(target_endian = "little")]
                                {
                                    let dst = unsafe {
                                        std::slice::from_raw_parts_mut(
                                            values.as_mut_ptr() as *mut u8,
                                            row_count * 8,
                                        )
                                    };
                                    dst.copy_from_slice(payload1);
                                }
                                #[cfg(not(target_endian = "little"))]
                                {
                                    for i in 0..row_count {
                                        let j = i * 8;
                                        let b = [
                                            payload1[j],
                                            payload1[j + 1],
                                            payload1[j + 2],
                                            payload1[j + 3],
                                            payload1[j + 4],
                                            payload1[j + 5],
                                            payload1[j + 6],
                                            payload1[j + 7],
                                        ];
                                        values[i] = u64::from_le_bytes(b);
                                    }
                                }
                            }
                            FixedEncodingId::PgBeFixed => {
                                for i in 0..row_count {
                                    let j = i * 8;
                                    let b = [
                                        payload1[j],
                                        payload1[j + 1],
                                        payload1[j + 2],
                                        payload1[j + 3],
                                        payload1[j + 4],
                                        payload1[j + 5],
                                        payload1[j + 6],
                                        payload1[j + 7],
                                    ];
                                    values[i] = u64::from_be_bytes(b);
                                }
                            }
                        }
                        columns.push(ColumnData::FixedF64Bits { validity, values });
                    }
                    ColumnarType::Uuid => {
                        if payload1.len() != row_count * 16 {
                            return Err(Error::Other("values length mismatch".to_string()));
                        }
                        let mut values: Vec<[u8; 16]> = vec![[0u8; 16]; row_count];
                        let dst = unsafe {
                            std::slice::from_raw_parts_mut(
                                values.as_mut_ptr() as *mut u8,
                                row_count * 16,
                            )
                        };
                        dst.copy_from_slice(payload1);
                        columns.push(ColumnData::FixedUuid { validity, values });
                    }
                    ColumnarType::TimestampTzMicros => {
                        let mut values: Vec<i64> = vec![0i64; row_count];
                        if encoding_id_u16 == ENC_DELTA_VARINT_I64 {
                            decode_delta_varint_i64_from_payload(
                                payload1,
                                row_count,
                                values.as_mut_slice(),
                            )?;
                        } else {
                            if payload1.len() != row_count * 8 {
                                return Err(Error::Other("values length mismatch".to_string()));
                            }
                            match enc {
                                FixedEncodingId::PlainLe => {
                                    #[cfg(target_endian = "little")]
                                    {
                                        let dst = unsafe {
                                            std::slice::from_raw_parts_mut(
                                                values.as_mut_ptr() as *mut u8,
                                                row_count * 8,
                                            )
                                        };
                                        dst.copy_from_slice(payload1);
                                    }
                                    #[cfg(not(target_endian = "little"))]
                                    {
                                        for i in 0..row_count {
                                            let j = i * 8;
                                            let b = [
                                                payload1[j],
                                                payload1[j + 1],
                                                payload1[j + 2],
                                                payload1[j + 3],
                                                payload1[j + 4],
                                                payload1[j + 5],
                                                payload1[j + 6],
                                                payload1[j + 7],
                                            ];
                                            values[i] = i64::from_le_bytes(b);
                                        }
                                    }
                                }
                                FixedEncodingId::PgBeFixed => {
                                    for i in 0..row_count {
                                        let j = i * 8;
                                        let b = [
                                            payload1[j],
                                            payload1[j + 1],
                                            payload1[j + 2],
                                            payload1[j + 3],
                                            payload1[j + 4],
                                            payload1[j + 5],
                                            payload1[j + 6],
                                            payload1[j + 7],
                                        ];
                                        values[i] = i64::from_be_bytes(b);
                                    }
                                }
                            }
                        }
                        columns.push(ColumnData::FixedTimestampMicros { validity, values });
                    }
                    ColumnarType::Utf8 | ColumnarType::Bytes | ColumnarType::JsonbText => {
                        return Err(Error::Other("invalid fixed type".to_string()));
                    }
                }
            }
        }
    }

    let schema = ColumnarSchema::new(fields)?;
    ColumnarBatch::new(schema, row_count, columns)
}

pub fn decode_mathldbt_v1_into(bytes: &[u8], out: &mut ColumnarBatch) -> Result<()> {
    let mut ws = MathldbtV1DecodeWorkspace::default();
    decode_mathldbt_v1_into_with_workspace(bytes, out, &mut ws)
}

pub fn decode_mathldbt_v1_into_with_workspace(
    bytes: &[u8],
    out: &mut ColumnarBatch,
    ws: &mut MathldbtV1DecodeWorkspace,
) -> Result<()> {
    let schema_err = || {
        Error::Other("decode_mathldbt_v1_into requires matching schema".to_string())
    };

    let mut pos = 0usize;

    fn take<'a>(bytes: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
        let end = pos
            .checked_add(n)
            .ok_or_else(|| Error::Other("decode overflow".to_string()))?;
        if end > bytes.len() {
            return Err(Error::Other("truncated mathldbt".to_string()));
        }
        let slice = &bytes[*pos..end];
        *pos = end;
        Ok(slice)
    }

    fn read_u16_le(bytes: &[u8], pos: &mut usize) -> Result<u16> {
        let b = take(bytes, pos, 2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    fn read_u32_le(bytes: &[u8], pos: &mut usize) -> Result<u32> {
        let b = take(bytes, pos, 4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    let magic = take(bytes, &mut pos, 8)?;
    if magic != MAGIC {
        return Err(Error::Other("invalid MATHLDBT magic".to_string()));
    }
    let version = read_u16_le(bytes, &mut pos)?;
    if version != VERSION {
        return Err(Error::Other(format!(
            "unsupported MATHLDBT version: {version}"
        )));
    }
    let _flags = read_u16_le(bytes, &mut pos)?;
    let row_count = read_u32_le(bytes, &mut pos)? as usize;
    let col_count = read_u16_le(bytes, &mut pos)? as usize;
    if col_count == 0 {
        return Err(Error::Other(
            "MATHLDBT must have at least one column".to_string(),
        ));
    }

    let schema_id_len = read_u16_le(bytes, &mut pos)? as usize;
    if schema_id_len > 0 {
        let _ = take(bytes, &mut pos, schema_id_len)?;
    }

    if col_count != out.schema.len() {
        return Err(schema_err());
    }
    if col_count != out.columns.len() {
        return Err(schema_err());
    }

    let expected_validity = ceil_div_8(row_count)?;
    let out_fields = out.schema.fields();

    for col_idx in 0..col_count {
        let tid = read_u16_le(bytes, &mut pos)?;
        let ty = type_from_id(tid)?;
        let encoding_id_u16 = read_u16_le(bytes, &mut pos)?;
        let _col_flags = read_u16_le(bytes, &mut pos)?;

        let name_len = read_u16_le(bytes, &mut pos)? as usize;
        let name_bytes = take(bytes, &mut pos, name_len)?;
        let name = if name_len == 0 {
            None
        } else {
            Some(
                std::str::from_utf8(name_bytes)
                    .map_err(|_| Error::Other("invalid UTF-8 column name".to_string()))?,
            )
        };

        let out_field = &out_fields[col_idx];
        if out_field.ty != ty {
            return Err(schema_err());
        }
        if out_field.name.as_deref() != name {
            return Err(schema_err());
        }

        let validity_len = read_u32_le(bytes, &mut pos)? as usize;
        if validity_len != expected_validity {
            return Err(Error::Other("validity length mismatch".to_string()));
        }
        let validity_src = take(bytes, &mut pos, validity_len)?;

        let payload1_len = read_u32_le(bytes, &mut pos)? as usize;
        let payload1 = take(bytes, &mut pos, payload1_len)?;
        let payload2_len = read_u32_le(bytes, &mut pos)? as usize;
        let payload2 = take(bytes, &mut pos, payload2_len)?;

        let out_col = &mut out.columns[col_idx];

        match ty {
            ColumnarType::Utf8 | ColumnarType::Bytes | ColumnarType::JsonbText => {
                let (col_ty, validity, offsets, data) = match out_col {
                    ColumnData::Var {
                        ty,
                        validity,
                        offsets,
                        data,
                    } => (*ty, validity, offsets, data),
                    _ => return Err(schema_err()),
                };
                if col_ty != ty {
                    return Err(schema_err());
                }
                validity.bytes.clear();
                validity.bytes.extend_from_slice(validity_src);

                match encoding_id_u16 {
                    ENC_PLAIN => {
                        let expected_offsets_len = (row_count + 1)
                            .checked_mul(4)
                            .ok_or_else(|| Error::Other("offsets overflow".to_string()))?;
                        if payload1.len() != expected_offsets_len {
                            return Err(Error::Other("offsets length mismatch".to_string()));
                        }

                        offsets.clear();
                        offsets.resize(row_count + 1, 0u32);
                        #[cfg(target_endian = "little")]
                        {
                            let dst = unsafe {
                                std::slice::from_raw_parts_mut(
                                    offsets.as_mut_ptr() as *mut u8,
                                    expected_offsets_len,
                                )
                            };
                            dst.copy_from_slice(payload1);
                        }
                        #[cfg(not(target_endian = "little"))]
                        {
                            for i in 0..(row_count + 1) {
                                let j = i * 4;
                                offsets[i] = u32::from_le_bytes([
                                    payload1[j],
                                    payload1[j + 1],
                                    payload1[j + 2],
                                    payload1[j + 3],
                                ]);
                            }
                        }

                        let mut prev = 0u32;
                        for &o in offsets.iter() {
                            if o < prev {
                                return Err(Error::Other(
                                    "offsets must be non-decreasing".to_string(),
                                ));
                            }
                            prev = o;
                        }
                        let final_off = offsets.last().copied().unwrap_or(0);
                        let data_len_u32: u32 = payload2
                            .len()
                            .try_into()
                            .map_err(|_| Error::Other("data too large".to_string()))?;
                        if final_off != data_len_u32 {
                            return Err(Error::Other("final offset mismatch".to_string()));
                        }

                        data.clear();
                        data.extend_from_slice(payload2);
                    }
                    ENC_DICT_UTF8 => {
                        if ty == ColumnarType::Bytes {
                            return Err(Error::Other(
                                "DictUtf8 is not supported for Bytes".to_string(),
                            ));
                        }
                        decode_dict_utf8_to_var_col(
                            ws,
                            row_count,
                            validity_src,
                            payload1,
                            payload2,
                            offsets,
                            data,
                        )?;
                    }
                    _ => {
                        return Err(Error::Other(
                            "invalid encoding for varlen column".to_string(),
                        ));
                    }
                }
            }
            _ => {
                if encoding_id_u16 == ENC_DELTA_VARINT_I64
                    && !(ty == ColumnarType::I64 || ty == ColumnarType::TimestampTzMicros)
                {
                    return Err(Error::Other(
                        "invalid encoding for fixed column".to_string(),
                    ));
                }
                let enc = if encoding_id_u16 == ENC_DELTA_VARINT_I64 {
                    FixedEncodingId::PlainLe
                } else {
                    FixedEncodingId::from_u16(encoding_id_u16).ok_or_else(|| {
                        Error::Other("invalid encoding for fixed column".to_string())
                    })?
                };
                if payload2_len != 0 {
                    return Err(Error::Other(
                        "fixed-width payload_2 must be empty".to_string(),
                    ));
                }

                match ty {
                    ColumnarType::Bool => {
                        let (validity, values) = match out_col {
                            ColumnData::FixedBool { validity, values } => (validity, values),
                            _ => return Err(schema_err()),
                        };
                        validity.bytes.clear();
                        validity.bytes.extend_from_slice(validity_src);
                        if payload1.len() != row_count {
                            return Err(Error::Other("values length mismatch".to_string()));
                        }
                        values.clear();
                        values.extend_from_slice(payload1);
                    }
                    ColumnarType::I16 => {
                        let (validity, values) = match out_col {
                            ColumnData::FixedI16 { validity, values } => (validity, values),
                            _ => return Err(schema_err()),
                        };
                        validity.bytes.clear();
                        validity.bytes.extend_from_slice(validity_src);
                        if payload1.len() != row_count * 2 {
                            return Err(Error::Other("values length mismatch".to_string()));
                        }
                        values.clear();
                        values.resize(row_count, 0i16);
                        match enc {
                            FixedEncodingId::PlainLe => {
                                #[cfg(target_endian = "little")]
                                {
                                    let dst = unsafe {
                                        std::slice::from_raw_parts_mut(
                                            values.as_mut_ptr() as *mut u8,
                                            row_count * 2,
                                        )
                                    };
                                    dst.copy_from_slice(payload1);
                                }
                                #[cfg(not(target_endian = "little"))]
                                {
                                    for i in 0..row_count {
                                        let j = i * 2;
                                        let b = [payload1[j], payload1[j + 1]];
                                        values[i] = i16::from_le_bytes(b);
                                    }
                                }
                            }
                            FixedEncodingId::PgBeFixed => {
                                for i in 0..row_count {
                                    let j = i * 2;
                                    let b = [payload1[j], payload1[j + 1]];
                                    values[i] = i16::from_be_bytes(b);
                                }
                            }
                        }
                    }
                    ColumnarType::I32 => {
                        let (validity, values) = match out_col {
                            ColumnData::FixedI32 { validity, values } => (validity, values),
                            _ => return Err(schema_err()),
                        };
                        validity.bytes.clear();
                        validity.bytes.extend_from_slice(validity_src);
                        if payload1.len() != row_count * 4 {
                            return Err(Error::Other("values length mismatch".to_string()));
                        }
                        values.clear();
                        values.resize(row_count, 0i32);
                        match enc {
                            FixedEncodingId::PlainLe => {
                                #[cfg(target_endian = "little")]
                                {
                                    let dst = unsafe {
                                        std::slice::from_raw_parts_mut(
                                            values.as_mut_ptr() as *mut u8,
                                            row_count * 4,
                                        )
                                    };
                                    dst.copy_from_slice(payload1);
                                }
                                #[cfg(not(target_endian = "little"))]
                                {
                                    for i in 0..row_count {
                                        let j = i * 4;
                                        let b = [
                                            payload1[j],
                                            payload1[j + 1],
                                            payload1[j + 2],
                                            payload1[j + 3],
                                        ];
                                        values[i] = i32::from_le_bytes(b);
                                    }
                                }
                            }
                            FixedEncodingId::PgBeFixed => {
                                for i in 0..row_count {
                                    let j = i * 4;
                                    let b = [
                                        payload1[j],
                                        payload1[j + 1],
                                        payload1[j + 2],
                                        payload1[j + 3],
                                    ];
                                    values[i] = i32::from_be_bytes(b);
                                }
                            }
                        }
                    }
                    ColumnarType::I64 => {
                        let (validity, values) = match out_col {
                            ColumnData::FixedI64 { validity, values } => (validity, values),
                            _ => return Err(schema_err()),
                        };
                        validity.bytes.clear();
                        validity.bytes.extend_from_slice(validity_src);
                        values.clear();
                        values.resize(row_count, 0i64);
                        if encoding_id_u16 == ENC_DELTA_VARINT_I64 {
                            decode_delta_varint_i64_from_payload(
                                payload1,
                                row_count,
                                values.as_mut_slice(),
                            )?;
                        } else {
                            if payload1.len() != row_count * 8 {
                                return Err(Error::Other("values length mismatch".to_string()));
                            }
                            match enc {
                                FixedEncodingId::PlainLe => {
                                    #[cfg(target_endian = "little")]
                                    {
                                        let dst = unsafe {
                                            std::slice::from_raw_parts_mut(
                                                values.as_mut_ptr() as *mut u8,
                                                row_count * 8,
                                            )
                                        };
                                        dst.copy_from_slice(payload1);
                                    }
                                    #[cfg(not(target_endian = "little"))]
                                    {
                                        for i in 0..row_count {
                                            let j = i * 8;
                                            let b = [
                                                payload1[j],
                                                payload1[j + 1],
                                                payload1[j + 2],
                                                payload1[j + 3],
                                                payload1[j + 4],
                                                payload1[j + 5],
                                                payload1[j + 6],
                                                payload1[j + 7],
                                            ];
                                            values[i] = i64::from_le_bytes(b);
                                        }
                                    }
                                }
                                FixedEncodingId::PgBeFixed => {
                                    for i in 0..row_count {
                                        let j = i * 8;
                                        let b = [
                                            payload1[j],
                                            payload1[j + 1],
                                            payload1[j + 2],
                                            payload1[j + 3],
                                            payload1[j + 4],
                                            payload1[j + 5],
                                            payload1[j + 6],
                                            payload1[j + 7],
                                        ];
                                        values[i] = i64::from_be_bytes(b);
                                    }
                                }
                            }
                        }
                    }
                    ColumnarType::F32 => {
                        let (validity, values) = match out_col {
                            ColumnData::FixedF32Bits { validity, values } => (validity, values),
                            _ => return Err(schema_err()),
                        };
                        validity.bytes.clear();
                        validity.bytes.extend_from_slice(validity_src);
                        if payload1.len() != row_count * 4 {
                            return Err(Error::Other("values length mismatch".to_string()));
                        }
                        values.clear();
                        values.resize(row_count, 0u32);
                        match enc {
                            FixedEncodingId::PlainLe => {
                                #[cfg(target_endian = "little")]
                                {
                                    let dst = unsafe {
                                        std::slice::from_raw_parts_mut(
                                            values.as_mut_ptr() as *mut u8,
                                            row_count * 4,
                                        )
                                    };
                                    dst.copy_from_slice(payload1);
                                }
                                #[cfg(not(target_endian = "little"))]
                                {
                                    for i in 0..row_count {
                                        let j = i * 4;
                                        let b = [
                                            payload1[j],
                                            payload1[j + 1],
                                            payload1[j + 2],
                                            payload1[j + 3],
                                        ];
                                        values[i] = u32::from_le_bytes(b);
                                    }
                                }
                            }
                            FixedEncodingId::PgBeFixed => {
                                for i in 0..row_count {
                                    let j = i * 4;
                                    let b = [
                                        payload1[j],
                                        payload1[j + 1],
                                        payload1[j + 2],
                                        payload1[j + 3],
                                    ];
                                    values[i] = u32::from_be_bytes(b);
                                }
                            }
                        }
                    }
                    ColumnarType::F64 => {
                        let (validity, values) = match out_col {
                            ColumnData::FixedF64Bits { validity, values } => (validity, values),
                            _ => return Err(schema_err()),
                        };
                        validity.bytes.clear();
                        validity.bytes.extend_from_slice(validity_src);
                        if payload1.len() != row_count * 8 {
                            return Err(Error::Other("values length mismatch".to_string()));
                        }
                        values.clear();
                        values.resize(row_count, 0u64);
                        match enc {
                            FixedEncodingId::PlainLe => {
                                #[cfg(target_endian = "little")]
                                {
                                    let dst = unsafe {
                                        std::slice::from_raw_parts_mut(
                                            values.as_mut_ptr() as *mut u8,
                                            row_count * 8,
                                        )
                                    };
                                    dst.copy_from_slice(payload1);
                                }
                                #[cfg(not(target_endian = "little"))]
                                {
                                    for i in 0..row_count {
                                        let j = i * 8;
                                        let b = [
                                            payload1[j],
                                            payload1[j + 1],
                                            payload1[j + 2],
                                            payload1[j + 3],
                                            payload1[j + 4],
                                            payload1[j + 5],
                                            payload1[j + 6],
                                            payload1[j + 7],
                                        ];
                                        values[i] = u64::from_le_bytes(b);
                                    }
                                }
                            }
                            FixedEncodingId::PgBeFixed => {
                                for i in 0..row_count {
                                    let j = i * 8;
                                    let b = [
                                        payload1[j],
                                        payload1[j + 1],
                                        payload1[j + 2],
                                        payload1[j + 3],
                                        payload1[j + 4],
                                        payload1[j + 5],
                                        payload1[j + 6],
                                        payload1[j + 7],
                                    ];
                                    values[i] = u64::from_be_bytes(b);
                                }
                            }
                        }
                    }
                    ColumnarType::Uuid => {
                        let (validity, values) = match out_col {
                            ColumnData::FixedUuid { validity, values } => (validity, values),
                            _ => return Err(schema_err()),
                        };
                        validity.bytes.clear();
                        validity.bytes.extend_from_slice(validity_src);
                        if payload1.len() != row_count * 16 {
                            return Err(Error::Other("values length mismatch".to_string()));
                        }
                        values.clear();
                        values.resize(row_count, [0u8; 16]);
                        let dst = unsafe {
                            std::slice::from_raw_parts_mut(
                                values.as_mut_ptr() as *mut u8,
                                row_count * 16,
                            )
                        };
                        dst.copy_from_slice(payload1);
                    }
                    ColumnarType::TimestampTzMicros => {
                        let (validity, values) = match out_col {
                            ColumnData::FixedTimestampMicros { validity, values } => {
                                (validity, values)
                            }
                            _ => return Err(schema_err()),
                        };
                        validity.bytes.clear();
                        validity.bytes.extend_from_slice(validity_src);
                        values.clear();
                        values.resize(row_count, 0i64);
                        if encoding_id_u16 == ENC_DELTA_VARINT_I64 {
                            decode_delta_varint_i64_from_payload(
                                payload1,
                                row_count,
                                values.as_mut_slice(),
                            )?;
                        } else {
                            if payload1.len() != row_count * 8 {
                                return Err(Error::Other("values length mismatch".to_string()));
                            }
                            match enc {
                                FixedEncodingId::PlainLe => {
                                    #[cfg(target_endian = "little")]
                                    {
                                        let dst = unsafe {
                                            std::slice::from_raw_parts_mut(
                                                values.as_mut_ptr() as *mut u8,
                                                row_count * 8,
                                            )
                                        };
                                        dst.copy_from_slice(payload1);
                                    }
                                    #[cfg(not(target_endian = "little"))]
                                    {
                                        for i in 0..row_count {
                                            let j = i * 8;
                                            let b = [
                                                payload1[j],
                                                payload1[j + 1],
                                                payload1[j + 2],
                                                payload1[j + 3],
                                                payload1[j + 4],
                                                payload1[j + 5],
                                                payload1[j + 6],
                                                payload1[j + 7],
                                            ];
                                            values[i] = i64::from_le_bytes(b);
                                        }
                                    }
                                }
                                FixedEncodingId::PgBeFixed => {
                                    for i in 0..row_count {
                                        let j = i * 8;
                                        let b = [
                                            payload1[j],
                                            payload1[j + 1],
                                            payload1[j + 2],
                                            payload1[j + 3],
                                            payload1[j + 4],
                                            payload1[j + 5],
                                            payload1[j + 6],
                                            payload1[j + 7],
                                        ];
                                        values[i] = i64::from_be_bytes(b);
                                    }
                                }
                            }
                        }
                    }
                    ColumnarType::Utf8 | ColumnarType::Bytes | ColumnarType::JsonbText => {
                        return Err(Error::Other("invalid fixed type".to_string()));
                    }
                }
            }
        }
    }

    out.row_count = row_count;
    out.validate()?;
    Ok(())
}
