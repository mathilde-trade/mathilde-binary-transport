use crate::batch::ColumnarBatch;
use crate::codec::mathldbt_v1::{
    MathldbtV1DecodeWorkspace, MathldbtV1EncodeWorkspace, decode_mathldbt_v1_into_with_workspace,
    decode_mathldbt_v1_with_workspace, encode_mathldbt_v1_into_with_workspace,
};
use crate::{Error, Result};

#[cfg(any(feature = "compression-zstd", feature = "compression-gzip"))]
use std::io::{Read, Write};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Zstd { level: i32 },
    Gzip { level: u32 },
}

#[derive(Debug, Default, Clone)]
pub struct MathldbtV1CompressedEncodeWorkspace {
    plain: Vec<u8>,
}

#[derive(Debug, Default, Clone)]
pub struct MathldbtV1CompressedDecodeWorkspace {
    plain: Vec<u8>,
}

fn compress_none_into(out: &mut Vec<u8>, plain: &[u8]) {
    out.clear();
    out.extend_from_slice(plain);
}

#[cfg(feature = "compression-zstd")]
fn compress_zstd_into(out: &mut Vec<u8>, plain: &[u8], level: i32) -> Result<()> {
    if !(-7..=22).contains(&level) {
        return Err(Error::Other("invalid zstd level".to_string()));
    }
    out.clear();
    let mut enc =
        zstd::stream::write::Encoder::new(out, level).map_err(|e| Error::Other(e.to_string()))?;
    enc.write_all(plain)
        .map_err(|e| Error::Other(e.to_string()))?;
    let _ = enc.finish().map_err(|e| Error::Other(e.to_string()))?;
    Ok(())
}

#[cfg(not(feature = "compression-zstd"))]
fn compress_zstd_into(_out: &mut Vec<u8>, _plain: &[u8], _level: i32) -> Result<()> {
    Err(Error::Other(
        "zstd compression feature not enabled".to_string(),
    ))
}

#[cfg(feature = "compression-gzip")]
fn compress_gzip_into(out: &mut Vec<u8>, plain: &[u8], level: u32) -> Result<()> {
    if level > 9 {
        return Err(Error::Other("invalid gzip level".to_string()));
    }
    out.clear();
    let mut enc = flate2::GzBuilder::new()
        .mtime(0)
        .write(out, flate2::Compression::new(level));
    enc.write_all(plain)
        .map_err(|e| Error::Other(e.to_string()))?;
    let _ = enc.finish().map_err(|e| Error::Other(e.to_string()))?;
    Ok(())
}

#[cfg(not(feature = "compression-gzip"))]
fn compress_gzip_into(_out: &mut Vec<u8>, _plain: &[u8], _level: u32) -> Result<()> {
    Err(Error::Other(
        "gzip compression feature not enabled".to_string(),
    ))
}

#[cfg(any(feature = "compression-zstd", feature = "compression-gzip"))]
fn decode_with_max_bound<R: Read>(
    mut reader: R,
    max_uncompressed_len: usize,
    out: &mut Vec<u8>,
) -> Result<()> {
    out.clear();
    if max_uncompressed_len == 0 {
        let mut tmp = [0u8; 1];
        match reader.read(&mut tmp) {
            Ok(0) => return Ok(()),
            Ok(_) => {
                return Err(Error::Other(
                    "decompressed payload exceeds max_uncompressed_len".to_string(),
                ));
            }
            Err(e) => return Err(Error::Other(e.to_string())),
        }
    }
    let mut limited = reader.take((max_uncompressed_len as u64) + 1);
    limited
        .read_to_end(out)
        .map_err(|e| Error::Other(e.to_string()))?;
    if out.len() > max_uncompressed_len {
        return Err(Error::Other(
            "decompressed payload exceeds max_uncompressed_len".to_string(),
        ));
    }
    Ok(())
}

#[cfg(feature = "compression-zstd")]
fn decompress_zstd_into(
    bytes: &[u8],
    max_uncompressed_len: usize,
    out: &mut Vec<u8>,
) -> Result<()> {
    let dec = zstd::stream::read::Decoder::new(bytes).map_err(|e| Error::Other(e.to_string()))?;
    decode_with_max_bound(dec, max_uncompressed_len, out)
}

#[cfg(not(feature = "compression-zstd"))]
fn decompress_zstd_into(
    _bytes: &[u8],
    _max_uncompressed_len: usize,
    _out: &mut Vec<u8>,
) -> Result<()> {
    Err(Error::Other(
        "zstd compression feature not enabled".to_string(),
    ))
}

#[cfg(feature = "compression-gzip")]
fn decompress_gzip_into(
    bytes: &[u8],
    max_uncompressed_len: usize,
    out: &mut Vec<u8>,
) -> Result<()> {
    let dec = flate2::read::GzDecoder::new(bytes);
    decode_with_max_bound(dec, max_uncompressed_len, out)
}

#[cfg(not(feature = "compression-gzip"))]
fn decompress_gzip_into(
    _bytes: &[u8],
    _max_uncompressed_len: usize,
    _out: &mut Vec<u8>,
) -> Result<()> {
    Err(Error::Other(
        "gzip compression feature not enabled".to_string(),
    ))
}

pub fn encode_mathldbt_v1_compressed_into(
    batch: &ColumnarBatch,
    out: &mut Vec<u8>,
    c: Compression,
) -> Result<()> {
    let mut codec_ws = MathldbtV1EncodeWorkspace::default();
    let mut ws = MathldbtV1CompressedEncodeWorkspace::default();
    encode_mathldbt_v1_compressed_into_with_workspace(batch, out, c, &mut codec_ws, &mut ws)
}

pub fn encode_mathldbt_v1_compressed_into_with_workspace(
    batch: &ColumnarBatch,
    out: &mut Vec<u8>,
    c: Compression,
    codec_ws: &mut MathldbtV1EncodeWorkspace,
    ws: &mut MathldbtV1CompressedEncodeWorkspace,
) -> Result<()> {
    ws.plain.clear();
    encode_mathldbt_v1_into_with_workspace(batch, &mut ws.plain, codec_ws)?;

    match c {
        Compression::None => {
            compress_none_into(out, ws.plain.as_slice());
            Ok(())
        }
        Compression::Zstd { level } => compress_zstd_into(out, ws.plain.as_slice(), level),
        Compression::Gzip { level } => compress_gzip_into(out, ws.plain.as_slice(), level),
    }
}

pub fn decode_mathldbt_v1_compressed(
    bytes: &[u8],
    c: Compression,
    max_uncompressed_len: usize,
) -> Result<ColumnarBatch> {
    let mut codec_ws = MathldbtV1DecodeWorkspace::default();
    let mut ws = MathldbtV1CompressedDecodeWorkspace::default();
    decode_mathldbt_v1_compressed_with_workspace(
        bytes,
        c,
        max_uncompressed_len,
        &mut codec_ws,
        &mut ws,
    )
}

pub fn decode_mathldbt_v1_compressed_with_workspace(
    bytes: &[u8],
    c: Compression,
    max_uncompressed_len: usize,
    codec_ws: &mut MathldbtV1DecodeWorkspace,
    ws: &mut MathldbtV1CompressedDecodeWorkspace,
) -> Result<ColumnarBatch> {
    match c {
        Compression::None => decode_mathldbt_v1_with_workspace(bytes, codec_ws),
        Compression::Zstd { .. } if max_uncompressed_len == 0 => Err(Error::Other(
            "decompressed payload exceeds max_uncompressed_len".to_string(),
        )),
        Compression::Gzip { .. } if max_uncompressed_len == 0 => Err(Error::Other(
            "decompressed payload exceeds max_uncompressed_len".to_string(),
        )),
        Compression::Zstd { .. } => {
            decompress_zstd_into(bytes, max_uncompressed_len, &mut ws.plain)?;
            decode_mathldbt_v1_with_workspace(ws.plain.as_slice(), codec_ws)
        }
        Compression::Gzip { .. } => {
            decompress_gzip_into(bytes, max_uncompressed_len, &mut ws.plain)?;
            decode_mathldbt_v1_with_workspace(ws.plain.as_slice(), codec_ws)
        }
    }
}

pub fn decode_mathldbt_v1_compressed_into(
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

pub fn decode_mathldbt_v1_compressed_into_with_workspace(
    bytes: &[u8],
    c: Compression,
    max_uncompressed_len: usize,
    out: &mut ColumnarBatch,
    codec_ws: &mut MathldbtV1DecodeWorkspace,
    ws: &mut MathldbtV1CompressedDecodeWorkspace,
) -> Result<()> {
    match c {
        Compression::None => decode_mathldbt_v1_into_with_workspace(bytes, out, codec_ws),
        Compression::Zstd { .. } if max_uncompressed_len == 0 => Err(Error::Other(
            "decompressed payload exceeds max_uncompressed_len".to_string(),
        )),
        Compression::Gzip { .. } if max_uncompressed_len == 0 => Err(Error::Other(
            "decompressed payload exceeds max_uncompressed_len".to_string(),
        )),
        Compression::Zstd { .. } => {
            decompress_zstd_into(bytes, max_uncompressed_len, &mut ws.plain)?;
            decode_mathldbt_v1_into_with_workspace(ws.plain.as_slice(), out, codec_ws)
        }
        Compression::Gzip { .. } => {
            decompress_gzip_into(bytes, max_uncompressed_len, &mut ws.plain)?;
            decode_mathldbt_v1_into_with_workspace(ws.plain.as_slice(), out, codec_ws)
        }
    }
}
