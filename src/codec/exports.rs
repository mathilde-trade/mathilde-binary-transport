use crate::Result;
use crate::batch::ColumnarBatch;
use crate::codec::mathldbt_v1::{
    MathldbtV1EncodeWorkspace, encode_mathldbt_v1_into, encode_mathldbt_v1_into_with_workspace,
};
use crate::codec::mathldbt_v1_compressed::{
    Compression, MathldbtV1CompressedEncodeWorkspace, encode_mathldbt_v1_compressed_into,
    encode_mathldbt_v1_compressed_into_with_workspace,
};

pub fn encode_into(batch: &ColumnarBatch, out: &mut Vec<u8>) -> Result<()> {
    encode_mathldbt_v1_into(batch, out)
}

pub fn encode_into_workspace(
    batch: &ColumnarBatch,
    out: &mut Vec<u8>,
    ws: &mut MathldbtV1EncodeWorkspace,
) -> Result<()> {
    encode_mathldbt_v1_into_with_workspace(batch, out, ws)
}

pub fn encode_compressed_into(
    batch: &ColumnarBatch,
    out: &mut Vec<u8>,
    c: Compression,
) -> Result<()> {
    encode_mathldbt_v1_compressed_into(batch, out, c)
}

pub fn encode_compressed_into_workspace(
    batch: &ColumnarBatch,
    out: &mut Vec<u8>,
    c: Compression,
    codec_ws: &mut MathldbtV1EncodeWorkspace,
    ws: &mut MathldbtV1CompressedEncodeWorkspace,
) -> Result<()> {
    encode_mathldbt_v1_compressed_into_with_workspace(batch, out, c, codec_ws, ws)
}
