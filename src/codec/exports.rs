use crate::Result;
use crate::batch::ColumnarBatch;
use crate::batch_view::ColumnarBatchView;
use crate::codec::mathldbt_v1::{
    decode_mathldbt_v1, decode_mathldbt_v1_into_with_workspace, decode_mathldbt_v1_with_workspace,
    encode_mathldbt_v1_fast_path_into, encode_mathldbt_v1_fast_path_into_opt_with_workspace,
    encode_mathldbt_v1_fast_path_into_with_workspace,
    encode_mathldbt_v1_into, encode_mathldbt_v1_into_with_workspace,
};
use crate::codec::mathldbt_v1_compressed::{
    decode_mathldbt_v1_compressed, decode_mathldbt_v1_compressed_into_with_workspace,
    decode_mathldbt_v1_compressed_with_workspace, encode_mathldbt_v1_compressed_into,
    encode_mathldbt_v1_compressed_into_with_workspace,
    encode_mathldbt_v1_compressed_fast_path_into,
    encode_mathldbt_v1_compressed_fast_path_into_opt_with_workspace,
    encode_mathldbt_v1_compressed_fast_path_into_with_workspace,
};

pub use crate::codec::mathldbt_v1::{MathldbtV1DecodeWorkspace, MathldbtV1EncodeWorkspace};
pub use crate::codec::mathldbt_v1_compressed::{
    Compression, MathldbtV1CompressedDecodeWorkspace, MathldbtV1CompressedEncodeWorkspace,
};

fn enable_opt_encodings(ws: &mut MathldbtV1EncodeWorkspace) {
    ws.set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);
}

// Plain (MATHLDBT v1)

pub fn encode_into(batch: &ColumnarBatch, out: &mut Vec<u8>) -> Result<()> {
    encode_mathldbt_v1_into(batch, out)
}

pub fn encode_into_with_workspace(
    batch: &ColumnarBatch,
    out: &mut Vec<u8>,
    ws: &mut MathldbtV1EncodeWorkspace,
) -> Result<()> {
    encode_mathldbt_v1_into_with_workspace(batch, out, ws)
}

pub fn encode_into_opt(batch: &ColumnarBatch, out: &mut Vec<u8>) -> Result<()> {
    let mut ws = MathldbtV1EncodeWorkspace::default();
    enable_opt_encodings(&mut ws);
    encode_mathldbt_v1_into_with_workspace(batch, out, &mut ws)
}

pub fn encode_into_opt_with_workspace(
    batch: &ColumnarBatch,
    out: &mut Vec<u8>,
    ws: &mut MathldbtV1EncodeWorkspace,
) -> Result<()> {
    enable_opt_encodings(ws);
    encode_mathldbt_v1_into_with_workspace(batch, out, ws)
}

// Fast-path (MATHLDBT v1; borrowed view)

pub fn encode_fast_path_into(view: &ColumnarBatchView<'_>, out: &mut Vec<u8>) -> Result<()> {
    encode_mathldbt_v1_fast_path_into(view, out)
}

pub fn encode_fast_path_into_with_workspace(
    view: &ColumnarBatchView<'_>,
    out: &mut Vec<u8>,
    ws: &mut MathldbtV1EncodeWorkspace,
) -> Result<()> {
    encode_mathldbt_v1_fast_path_into_with_workspace(view, out, ws)
}

pub fn encode_fast_path_into_opt(view: &ColumnarBatchView<'_>, out: &mut Vec<u8>) -> Result<()> {
    let mut ws = MathldbtV1EncodeWorkspace::default();
    ws.set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);
    encode_mathldbt_v1_fast_path_into_with_workspace(view, out, &mut ws)
}

pub fn encode_fast_path_into_opt_with_workspace(
    view: &ColumnarBatchView<'_>,
    out: &mut Vec<u8>,
    ws: &mut MathldbtV1EncodeWorkspace,
) -> Result<()> {
    encode_mathldbt_v1_fast_path_into_opt_with_workspace(view, out, ws)
}

pub fn decode(bytes: &[u8]) -> Result<ColumnarBatch> {
    decode_mathldbt_v1(bytes)
}

pub fn decode_with_workspace(
    bytes: &[u8],
    ws: &mut MathldbtV1DecodeWorkspace,
) -> Result<ColumnarBatch> {
    decode_mathldbt_v1_with_workspace(bytes, ws)
}

pub fn decode_into(bytes: &[u8], out: &mut ColumnarBatch) -> Result<()> {
    let mut ws = MathldbtV1DecodeWorkspace::default();
    decode_mathldbt_v1_into_with_workspace(bytes, out, &mut ws)
}

pub fn decode_into_with_workspace(
    bytes: &[u8],
    out: &mut ColumnarBatch,
    ws: &mut MathldbtV1DecodeWorkspace,
) -> Result<()> {
    decode_mathldbt_v1_into_with_workspace(bytes, out, ws)
}

// Compressed (compress(encode_v1(...)))

pub fn encode_compressed_into(
    batch: &ColumnarBatch,
    out: &mut Vec<u8>,
    c: Compression,
) -> Result<()> {
    encode_mathldbt_v1_compressed_into(batch, out, c)
}

pub fn encode_compressed_into_with_workspace(
    batch: &ColumnarBatch,
    out: &mut Vec<u8>,
    c: Compression,
    codec_ws: &mut MathldbtV1EncodeWorkspace,
    ws: &mut MathldbtV1CompressedEncodeWorkspace,
) -> Result<()> {
    encode_mathldbt_v1_compressed_into_with_workspace(batch, out, c, codec_ws, ws)
}

pub fn encode_compressed_into_opt(
    batch: &ColumnarBatch,
    out: &mut Vec<u8>,
    c: Compression,
) -> Result<()> {
    let mut codec_ws = MathldbtV1EncodeWorkspace::default();
    enable_opt_encodings(&mut codec_ws);
    let mut ws = MathldbtV1CompressedEncodeWorkspace::default();
    encode_mathldbt_v1_compressed_into_with_workspace(batch, out, c, &mut codec_ws, &mut ws)
}

pub fn encode_compressed_into_opt_with_workspace(
    batch: &ColumnarBatch,
    out: &mut Vec<u8>,
    c: Compression,
    codec_ws: &mut MathldbtV1EncodeWorkspace,
    ws: &mut MathldbtV1CompressedEncodeWorkspace,
) -> Result<()> {
    enable_opt_encodings(codec_ws);
    encode_mathldbt_v1_compressed_into_with_workspace(batch, out, c, codec_ws, ws)
}

// Fast-path compressed (compress(encode_v1_fast_path(...)))

pub fn encode_compressed_fast_path_into(
    view: &ColumnarBatchView<'_>,
    out: &mut Vec<u8>,
    c: Compression,
) -> Result<()> {
    encode_mathldbt_v1_compressed_fast_path_into(view, out, c)
}

pub fn encode_compressed_fast_path_into_with_workspace(
    view: &ColumnarBatchView<'_>,
    out: &mut Vec<u8>,
    c: Compression,
    codec_ws: &mut MathldbtV1EncodeWorkspace,
    ws: &mut MathldbtV1CompressedEncodeWorkspace,
) -> Result<()> {
    encode_mathldbt_v1_compressed_fast_path_into_with_workspace(view, out, c, codec_ws, ws)
}

pub fn encode_compressed_fast_path_into_opt(
    view: &ColumnarBatchView<'_>,
    out: &mut Vec<u8>,
    c: Compression,
) -> Result<()> {
    let mut codec_ws = MathldbtV1EncodeWorkspace::default();
    codec_ws
        .set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);
    let mut ws = MathldbtV1CompressedEncodeWorkspace::default();
    encode_mathldbt_v1_compressed_fast_path_into_with_workspace(view, out, c, &mut codec_ws, &mut ws)
}

pub fn encode_compressed_fast_path_into_opt_with_workspace(
    view: &ColumnarBatchView<'_>,
    out: &mut Vec<u8>,
    c: Compression,
    codec_ws: &mut MathldbtV1EncodeWorkspace,
    ws: &mut MathldbtV1CompressedEncodeWorkspace,
) -> Result<()> {
    encode_mathldbt_v1_compressed_fast_path_into_opt_with_workspace(view, out, c, codec_ws, ws)
}

pub fn decode_compressed(
    bytes: &[u8],
    c: Compression,
    max_uncompressed_len: usize,
) -> Result<ColumnarBatch> {
    decode_mathldbt_v1_compressed(bytes, c, max_uncompressed_len)
}

pub fn decode_compressed_with_workspace(
    bytes: &[u8],
    c: Compression,
    max_uncompressed_len: usize,
    codec_ws: &mut MathldbtV1DecodeWorkspace,
    ws: &mut MathldbtV1CompressedDecodeWorkspace,
) -> Result<ColumnarBatch> {
    decode_mathldbt_v1_compressed_with_workspace(bytes, c, max_uncompressed_len, codec_ws, ws)
}

pub fn decode_compressed_into(
    bytes: &[u8],
    c: Compression,
    max_uncompressed_len: usize,
    out: &mut ColumnarBatch,
) -> Result<()> {
    let mut codec_ws = MathldbtV1DecodeWorkspace::default();
    let mut ws = MathldbtV1CompressedDecodeWorkspace::default();
    decode_mathldbt_v1_compressed_into_with_workspace(
        bytes,
        c,
        max_uncompressed_len,
        out,
        &mut codec_ws,
        &mut ws,
    )
}

pub fn decode_compressed_into_with_workspace(
    bytes: &[u8],
    c: Compression,
    max_uncompressed_len: usize,
    out: &mut ColumnarBatch,
    codec_ws: &mut MathldbtV1DecodeWorkspace,
    ws: &mut MathldbtV1CompressedDecodeWorkspace,
) -> Result<()> {
    decode_mathldbt_v1_compressed_into_with_workspace(
        bytes,
        c,
        max_uncompressed_len,
        out,
        codec_ws,
        ws,
    )
}
