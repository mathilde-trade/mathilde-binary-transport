use crate::batch::ColumnarBatch;
use crate::codec::mathldbt_v1::{
    MathldbtV1DecodeWorkspace, MathldbtV1EncodeWorkspace, decode_mathldbt_v1_into_with_workspace,
    decode_mathldbt_v1_with_workspace, encode_mathldbt_v1_into_with_workspace,
};
use crate::{Error, Result};

#[cfg(any(feature = "compression-zstd", feature = "compression-gzip"))]
use std::io::Read;
#[cfg(feature = "compression-gzip")]
use std::io::Write;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Zstd { level: i32 },
    Gzip { level: u32 },
}

#[cfg(feature = "compression-zstd")]
#[derive(Default)]
struct ZstdBulkEncodeCtx {
    compressor: Option<zstd::bulk::Compressor<'static>>,
    level: Option<i32>,
}

#[cfg(feature = "compression-zstd")]
impl fmt::Debug for ZstdBulkEncodeCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZstdBulkEncodeCtx")
            .field("has_compressor", &self.compressor.is_some())
            .field("level", &self.level)
            .finish()
    }
}

#[cfg(feature = "compression-zstd")]
#[derive(Default)]
struct ZstdBulkDecodeCtx {
    decompressor: Option<zstd::bulk::Decompressor<'static>>,
}

#[cfg(feature = "compression-zstd")]
impl fmt::Debug for ZstdBulkDecodeCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZstdBulkDecodeCtx")
            .field("has_decompressor", &self.decompressor.is_some())
            .finish()
    }
}

pub struct MathldbtV1CompressedEncodeWorkspace {
    plain: Vec<u8>,
    #[cfg(feature = "compression-zstd")]
    zstd: ZstdBulkEncodeCtx,
}

impl Default for MathldbtV1CompressedEncodeWorkspace {
    fn default() -> Self {
        Self {
            plain: Vec::new(),
            #[cfg(feature = "compression-zstd")]
            zstd: ZstdBulkEncodeCtx::default(),
        }
    }
}

impl Clone for MathldbtV1CompressedEncodeWorkspace {
    fn clone(&self) -> Self {
        Self {
            plain: self.plain.clone(),
            #[cfg(feature = "compression-zstd")]
            zstd: ZstdBulkEncodeCtx::default(),
        }
    }
}

impl fmt::Debug for MathldbtV1CompressedEncodeWorkspace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("MathldbtV1CompressedEncodeWorkspace");
        d.field("plain_len", &self.plain.len())
            .field("plain_cap", &self.plain.capacity());
        #[cfg(feature = "compression-zstd")]
        d.field("zstd", &self.zstd);
        d.finish()
    }
}

pub struct MathldbtV1CompressedDecodeWorkspace {
    plain: Vec<u8>,
    #[cfg(feature = "compression-zstd")]
    zstd: ZstdBulkDecodeCtx,
}

impl Default for MathldbtV1CompressedDecodeWorkspace {
    fn default() -> Self {
        Self {
            plain: Vec::new(),
            #[cfg(feature = "compression-zstd")]
            zstd: ZstdBulkDecodeCtx::default(),
        }
    }
}

impl Clone for MathldbtV1CompressedDecodeWorkspace {
    fn clone(&self) -> Self {
        Self {
            plain: self.plain.clone(),
            #[cfg(feature = "compression-zstd")]
            zstd: ZstdBulkDecodeCtx::default(),
        }
    }
}

impl fmt::Debug for MathldbtV1CompressedDecodeWorkspace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("MathldbtV1CompressedDecodeWorkspace");
        d.field("plain_len", &self.plain.len())
            .field("plain_cap", &self.plain.capacity());
        #[cfg(feature = "compression-zstd")]
        d.field("zstd", &self.zstd);
        d.finish()
    }
}

fn compress_none_into(out: &mut Vec<u8>, plain: &[u8]) {
    out.clear();
    out.extend_from_slice(plain);
}

#[cfg(feature = "compression-zstd")]
fn compress_zstd_into(
    out: &mut Vec<u8>,
    plain: &[u8],
    level: i32,
    ws: &mut MathldbtV1CompressedEncodeWorkspace,
) -> Result<()> {
    if !(-7..=22).contains(&level) {
        return Err(Error::Other("invalid zstd level".to_string()));
    }
    out.clear();
    let bound = zstd::zstd_safe::compress_bound(plain.len());
    out.try_reserve(bound)
        .map_err(|e| Error::Other(e.to_string()))?;

    let level_changed = ws.zstd.level != Some(level);
    if ws.zstd.compressor.is_none() {
        ws.zstd.compressor = Some(
            zstd::bulk::Compressor::new(level).map_err(|e| Error::Other(e.to_string()))?,
        );
        ws.zstd.level = Some(level);
    } else if level_changed {
        let mut compressor = match ws.zstd.compressor.take() {
            Some(c) => c,
            None => {
                return Err(Error::Other(
                    "internal error: zstd compressor not initialized".to_string(),
                ));
            }
        };
        compressor
            .set_compression_level(level)
            .map_err(|e| Error::Other(e.to_string()))?;
        ws.zstd.compressor = Some(compressor);
        ws.zstd.level = Some(level);
    }

    let compressor = match ws.zstd.compressor.as_mut() {
        Some(c) => c,
        None => {
            return Err(Error::Other(
                "internal error: zstd compressor not initialized".to_string(),
            ));
        }
    };
    let _written = compressor
        .compress_to_buffer(plain, out)
        .map_err(|e| Error::Other(e.to_string()))?;
    Ok(())
}

#[cfg(not(feature = "compression-zstd"))]
fn compress_zstd_into(
    _out: &mut Vec<u8>,
    _plain: &[u8],
    _level: i32,
    _ws: &mut MathldbtV1CompressedEncodeWorkspace,
) -> Result<()> {
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
    ws: &mut MathldbtV1CompressedDecodeWorkspace,
) -> Result<()> {
    let size_u64 = match zstd::zstd_safe::get_frame_content_size(bytes) {
        Ok(Some(v)) => Some(v),
        Ok(None) => None,
        Err(_) => None,
    };

    if let Some(size_u64) = size_u64 {
        let size_usize = usize::try_from(size_u64).map_err(|_| {
            Error::Other("decompressed payload exceeds max_uncompressed_len".to_string())
        })?;
        if size_usize > max_uncompressed_len {
            return Err(Error::Other(
                "decompressed payload exceeds max_uncompressed_len".to_string(),
            ));
        }

        out.clear();
        out.try_reserve(size_usize)
            .map_err(|e| Error::Other(e.to_string()))?;

        if ws.zstd.decompressor.is_none() {
            ws.zstd.decompressor =
                Some(zstd::bulk::Decompressor::new().map_err(|e| Error::Other(e.to_string()))?);
        }
        let decompressor = match ws.zstd.decompressor.as_mut() {
            Some(d) => d,
            None => {
                return Err(Error::Other(
                    "internal error: zstd decompressor not initialized".to_string(),
                ));
            }
        };

        let _written = decompressor
            .decompress_to_buffer(bytes, out)
            .map_err(|e| Error::Other(e.to_string()))?;
        if out.len() != size_usize {
            return Err(Error::Other(
                "zstd decompressed length mismatch".to_string(),
            ));
        }
        return Ok(());
    }

    let dec = zstd::stream::read::Decoder::new(bytes).map_err(|e| Error::Other(e.to_string()))?;
    decode_with_max_bound(dec, max_uncompressed_len, out)
}

#[cfg(not(feature = "compression-zstd"))]
fn decompress_zstd_into(
    _bytes: &[u8],
    _max_uncompressed_len: usize,
    _out: &mut Vec<u8>,
    _ws: &mut MathldbtV1CompressedDecodeWorkspace,
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
        Compression::Zstd { level } => compress_zstd_into(out, ws.plain.as_slice(), level, ws),
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
            decompress_zstd_into(bytes, max_uncompressed_len, &mut ws.plain, ws)?;
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
            decompress_zstd_into(bytes, max_uncompressed_len, &mut ws.plain, ws)?;
            decode_mathldbt_v1_into_with_workspace(ws.plain.as_slice(), out, codec_ws)
        }
        Compression::Gzip { .. } => {
            decompress_gzip_into(bytes, max_uncompressed_len, &mut ws.plain)?;
            decode_mathldbt_v1_into_with_workspace(ws.plain.as_slice(), out, codec_ws)
        }
    }
}
