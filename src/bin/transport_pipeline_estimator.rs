use mathilde_binary_transport::batch::{ColumnData, ColumnarBatch, ValidityBitmap};
use mathilde_binary_transport::codec::mathldbt_v1::{
    MathldbtV1DecodeWorkspace, MathldbtV1EncodeWorkspace, decode_mathldbt_v1_with_workspace,
    encode_mathldbt_v1_into_with_workspace,
};
use mathilde_binary_transport::schema::{ColumnarField, ColumnarSchema, ColumnarType};
use std::time::Instant;

#[cfg(any(feature = "compression-zstd", feature = "compression-gzip"))]
use mathilde_binary_transport::codec::mathldbt_v1_compressed::{
    Compression, MathldbtV1CompressedDecodeWorkspace, MathldbtV1CompressedEncodeWorkspace,
    decode_mathldbt_v1_compressed_with_workspace,
    encode_mathldbt_v1_compressed_into_with_workspace,
};

#[cfg(all(feature = "tools-json", feature = "compression-zstd"))]
use std::io::{Read, Write};

#[cfg(all(
    feature = "tools-json",
    feature = "compression-gzip",
    not(feature = "compression-zstd")
))]
use std::io::{Read, Write};

#[cfg(feature = "tools-json")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "tools-json")]
#[derive(Debug, Clone, Copy, Serialize)]
struct BarRowBorrowed<'a> {
    pair: &'a str,
    tf: &'a str,
    e_ms: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

#[cfg(feature = "tools-json")]
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct BarRowOwned {
    pair: String,
    tf: String,
    e_ms: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

#[cfg(all(feature = "tools-json", feature = "compression-zstd"))]
fn zstd_compress_bytes(bytes: &[u8], level: i32) -> Vec<u8> {
    let mut out = Vec::new();
    let mut enc = zstd::stream::write::Encoder::new(&mut out, level).unwrap();
    enc.write_all(bytes).unwrap();
    let _ = enc.finish().unwrap();
    out
}

#[cfg(all(feature = "tools-json", feature = "compression-zstd"))]
fn zstd_decompress_bytes(bytes: &[u8], max_uncompressed_len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let dec = zstd::stream::read::Decoder::new(bytes).unwrap();
    let mut limited = dec.take((max_uncompressed_len as u64) + 1);
    limited.read_to_end(&mut out).unwrap();
    assert!(out.len() <= max_uncompressed_len);
    out
}

#[cfg(all(feature = "tools-json", feature = "compression-gzip"))]
fn gzip_compress_bytes(bytes: &[u8], level: u32) -> Vec<u8> {
    let mut out = Vec::new();
    let mut enc = flate2::GzBuilder::new()
        .mtime(0)
        .write(&mut out, flate2::Compression::new(level));
    enc.write_all(bytes).unwrap();
    let _ = enc.finish().unwrap();
    out
}

#[cfg(all(feature = "tools-json", feature = "compression-gzip"))]
fn gzip_decompress_bytes(bytes: &[u8], max_uncompressed_len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let dec = flate2::read::GzDecoder::new(bytes);
    let mut limited = dec.take((max_uncompressed_len as u64) + 1);
    limited.read_to_end(&mut out).unwrap();
    assert!(out.len() <= max_uncompressed_len);
    out
}

fn parse_arg(args: &[String], name: &str) -> Option<String> {
    for (i, a) in args.iter().enumerate() {
        if let Some(v) = a.strip_prefix(&(name.to_string() + "=")) {
            return Some(v.to_string());
        }
        if a == name {
            return args.get(i + 1).cloned();
        }
    }
    None
}

fn usage() -> &'static str {
    "\
transport_pipeline_estimator\n\
\n\
Deterministic WAN estimate:\n\
  t_transfer = rtt_ms/1000 + (bytes*8)/(mbit_per_s*1e6)\n\
\n\
Args:\n\
  --rows N            (default: 2000)\n\
  --rtt-ms N          (default: 30)\n\
  --mbit N            (default: 100)\n\
  --max-uncompressed N (default: 1073741824)\n\
\n\
Notes:\n\
  - This tool prints byte sizes and a WAN estimate.\n\
  - It also prints local encode/decode timings from the current run; these are not stable benchmarks.\n\
  - If built with `--features tools-json`, it also reports a JSON baseline.\n\
"
}

fn make_bars_like_batch(rows: usize) -> ColumnarBatch {
    let schema = ColumnarSchema::new(vec![
        ColumnarField {
            name: Some("pair".to_string()),
            ty: ColumnarType::Utf8,
        },
        ColumnarField {
            name: Some("tf".to_string()),
            ty: ColumnarType::Utf8,
        },
        ColumnarField {
            name: Some("e_ms".to_string()),
            ty: ColumnarType::I64,
        },
        ColumnarField {
            name: Some("open".to_string()),
            ty: ColumnarType::F64,
        },
        ColumnarField {
            name: Some("high".to_string()),
            ty: ColumnarType::F64,
        },
        ColumnarField {
            name: Some("low".to_string()),
            ty: ColumnarType::F64,
        },
        ColumnarField {
            name: Some("close".to_string()),
            ty: ColumnarType::F64,
        },
        ColumnarField {
            name: Some("volume".to_string()),
            ty: ColumnarType::F64,
        },
    ])
    .unwrap();

    let validity_all = ValidityBitmap::new_all_valid(rows).unwrap();

    fn make_repeated_utf8(
        rows: usize,
        a: &'static [u8],
        b: &'static [u8],
    ) -> (ValidityBitmap, Vec<u32>, Vec<u8>) {
        let validity = ValidityBitmap::new_all_valid(rows).unwrap();
        let mut offsets = vec![0u32; rows + 1];
        let mut data = Vec::new();
        for i in 0..rows {
            let v = if (i & 1) == 0 { a } else { b };
            data.extend_from_slice(v);
            offsets[i + 1] = data.len() as u32;
        }
        (validity, offsets, data)
    }

    let (pair_validity, pair_offsets, pair_data) = make_repeated_utf8(rows, b"BTCUSDT", b"ETHUSDT");
    let (tf_validity, tf_offsets, tf_data) = make_repeated_utf8(rows, b"1m", b"1m");

    let mut e_vals = Vec::with_capacity(rows);
    for i in 0..rows {
        e_vals.push(1_700_000_000_000i64 + i as i64 * 60_000);
    }

    let mut open_bits = Vec::with_capacity(rows);
    let mut high_bits = Vec::with_capacity(rows);
    let mut low_bits = Vec::with_capacity(rows);
    let mut close_bits = Vec::with_capacity(rows);
    let mut vol_bits = Vec::with_capacity(rows);

    for i in 0..rows {
        let base = 10_000.0 + i as f64 * 0.25;
        open_bits.push((base + 0.10).to_bits());
        high_bits.push((base + 0.20).to_bits());
        low_bits.push((base + 0.05).to_bits());
        close_bits.push((base + 0.15).to_bits());
        vol_bits.push((100.0 + (i % 10) as f64).to_bits());
    }

    ColumnarBatch::new(
        schema,
        rows,
        vec![
            ColumnData::Var {
                ty: ColumnarType::Utf8,
                validity: pair_validity,
                offsets: pair_offsets,
                data: pair_data,
            },
            ColumnData::Var {
                ty: ColumnarType::Utf8,
                validity: tf_validity,
                offsets: tf_offsets,
                data: tf_data,
            },
            ColumnData::FixedI64 {
                validity: validity_all.clone(),
                values: e_vals,
            },
            ColumnData::FixedF64Bits {
                validity: validity_all.clone(),
                values: open_bits,
            },
            ColumnData::FixedF64Bits {
                validity: validity_all.clone(),
                values: high_bits,
            },
            ColumnData::FixedF64Bits {
                validity: validity_all.clone(),
                values: low_bits,
            },
            ColumnData::FixedF64Bits {
                validity: validity_all.clone(),
                values: close_bits,
            },
            ColumnData::FixedF64Bits {
                validity: validity_all,
                values: vol_bits,
            },
        ],
    )
    .unwrap()
}

#[cfg(feature = "tools-json")]
fn make_bars_like_rows(rows: usize) -> Vec<BarRowBorrowed<'static>> {
    let mut out = Vec::with_capacity(rows);
    for i in 0..rows {
        let pair = if (i & 1) == 0 { "BTCUSDT" } else { "ETHUSDT" };
        let tf = "1m";
        let e_ms = 1_700_000_000_000i64 + i as i64 * 60_000;
        let base = 10_000.0 + i as f64 * 0.25;
        out.push(BarRowBorrowed {
            pair,
            tf,
            e_ms,
            open: base + 0.10,
            high: base + 0.20,
            low: base + 0.05,
            close: base + 0.15,
            volume: 100.0 + (i % 10) as f64,
        });
    }
    out
}

fn transfer_time_seconds(bytes: usize, rtt_ms: f64, mbit_per_s: f64) -> f64 {
    let rtt_s = rtt_ms / 1000.0;
    let bits = (bytes as f64) * 8.0;
    let bw = mbit_per_s * 1_000_000.0;
    rtt_s + (bits / bw)
}

fn measure_plain(batch: &ColumnarBatch) -> (Vec<u8>, f64, f64) {
    let mut codec_enc_ws = MathldbtV1EncodeWorkspace::default();
    codec_enc_ws
        .set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);
    let mut out = Vec::new();

    let t0 = Instant::now();
    encode_mathldbt_v1_into_with_workspace(batch, &mut out, &mut codec_enc_ws).unwrap();
    let enc_s = t0.elapsed().as_secs_f64();

    let mut codec_dec_ws = MathldbtV1DecodeWorkspace::default();
    let t1 = Instant::now();
    let decoded = decode_mathldbt_v1_with_workspace(out.as_slice(), &mut codec_dec_ws).unwrap();
    let dec_s = t1.elapsed().as_secs_f64();
    assert_eq!(decoded.row_count, batch.row_count);

    (out, enc_s, dec_s)
}

#[cfg(feature = "tools-json")]
fn measure_json(rows: &[BarRowBorrowed<'static>]) -> (Vec<u8>, f64, f64) {
    let t0 = Instant::now();
    let bytes = serde_json::to_vec(rows).unwrap();
    let enc_s = t0.elapsed().as_secs_f64();

    let t1 = Instant::now();
    let decoded: Vec<BarRowOwned> = serde_json::from_slice(bytes.as_slice()).unwrap();
    let dec_s = t1.elapsed().as_secs_f64();
    assert_eq!(decoded.len(), rows.len());

    (bytes, enc_s, dec_s)
}

#[cfg(any(feature = "compression-zstd", feature = "compression-gzip"))]
fn measure_compressed(
    batch: &ColumnarBatch,
    c: Compression,
    max_uncompressed_len: usize,
) -> (Vec<u8>, f64, f64) {
    let mut codec_enc_ws = MathldbtV1EncodeWorkspace::default();
    codec_enc_ws
        .set_enable_dict_utf8(true)
        .set_enable_delta_varint_i64(true);
    let mut compress_ws = MathldbtV1CompressedEncodeWorkspace::default();
    let mut out = Vec::new();

    let t0 = Instant::now();
    encode_mathldbt_v1_compressed_into_with_workspace(
        batch,
        &mut out,
        c,
        &mut codec_enc_ws,
        &mut compress_ws,
    )
    .unwrap();
    let enc_s = t0.elapsed().as_secs_f64();

    let mut codec_dec_ws = MathldbtV1DecodeWorkspace::default();
    let mut decompress_ws = MathldbtV1CompressedDecodeWorkspace::default();
    let t1 = Instant::now();
    let decoded = decode_mathldbt_v1_compressed_with_workspace(
        out.as_slice(),
        c,
        max_uncompressed_len,
        &mut codec_dec_ws,
        &mut decompress_ws,
    )
    .unwrap();
    let dec_s = t1.elapsed().as_secs_f64();
    assert_eq!(decoded.row_count, batch.row_count);

    (out, enc_s, dec_s)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprint!("{}", usage());
        std::process::exit(0);
    }

    let rows: usize = parse_arg(&args, "--rows")
        .as_deref()
        .unwrap_or("2000")
        .parse()
        .unwrap_or(2000);
    let rtt_ms: f64 = parse_arg(&args, "--rtt-ms")
        .as_deref()
        .unwrap_or("30")
        .parse()
        .unwrap_or(30.0);
    let mbit: f64 = parse_arg(&args, "--mbit")
        .as_deref()
        .unwrap_or("100")
        .parse()
        .unwrap_or(100.0);
    #[cfg(any(feature = "compression-zstd", feature = "compression-gzip"))]
    let max_uncompressed_len: usize = parse_arg(&args, "--max-uncompressed")
        .as_deref()
        .unwrap_or("1073741824")
        .parse()
        .unwrap_or(1024 * 1024 * 1024);

    let batch = make_bars_like_batch(rows);

    let (plain, enc_s, dec_s) = measure_plain(&batch);
    let t_transfer = transfer_time_seconds(plain.len(), rtt_ms, mbit);
    println!("rows={rows}");
    println!("net: rtt_ms={rtt_ms} mbit_per_s={mbit}");
    println!(
        "plain_mathldbt: bytes={} enc_s={:.6} dec_s={:.6} t_transfer_s={:.6} t_total_s={:.6}",
        plain.len(),
        enc_s,
        dec_s,
        t_transfer,
        enc_s + t_transfer + dec_s
    );

    #[cfg(feature = "tools-json")]
    {
        let rows_json = make_bars_like_rows(rows);
        let (bytes, enc_s, dec_s) = measure_json(rows_json.as_slice());
        let t_transfer = transfer_time_seconds(bytes.len(), rtt_ms, mbit);
        println!(
            "json: bytes={} enc_s={:.6} dec_s={:.6} t_transfer_s={:.6} t_total_s={:.6}",
            bytes.len(),
            enc_s,
            dec_s,
            t_transfer,
            enc_s + t_transfer + dec_s
        );

        #[cfg(feature = "compression-zstd")]
        {
            let level = 3;
            let compressed = zstd_compress_bytes(bytes.as_slice(), level);
            let roundtrip = zstd_decompress_bytes(compressed.as_slice(), max_uncompressed_len);
            let decoded: Vec<BarRowOwned> = serde_json::from_slice(roundtrip.as_slice()).unwrap();
            assert_eq!(decoded.len(), rows_json.len());

            let t_transfer = transfer_time_seconds(compressed.len(), rtt_ms, mbit);
            println!(
                "json_zstd(level=3): bytes={} t_transfer_s={:.6}",
                compressed.len(),
                t_transfer
            );
        }

        #[cfg(feature = "compression-gzip")]
        {
            let level = 6;
            let compressed = gzip_compress_bytes(bytes.as_slice(), level);
            let roundtrip = gzip_decompress_bytes(compressed.as_slice(), max_uncompressed_len);
            let decoded: Vec<BarRowOwned> = serde_json::from_slice(roundtrip.as_slice()).unwrap();
            assert_eq!(decoded.len(), rows_json.len());

            let t_transfer = transfer_time_seconds(compressed.len(), rtt_ms, mbit);
            println!(
                "json_gzip(level=6): bytes={} t_transfer_s={:.6}",
                compressed.len(),
                t_transfer
            );
        }
    }

    #[cfg(feature = "compression-zstd")]
    {
        let c = Compression::Zstd { level: 3 };
        let (bytes, enc_s, dec_s) = measure_compressed(&batch, c, max_uncompressed_len);
        let t_transfer = transfer_time_seconds(bytes.len(), rtt_ms, mbit);
        println!(
            "zstd(level=3): bytes={} enc_s={:.6} dec_s={:.6} t_transfer_s={:.6} t_total_s={:.6}",
            bytes.len(),
            enc_s,
            dec_s,
            t_transfer,
            enc_s + t_transfer + dec_s
        );
    }

    #[cfg(feature = "compression-gzip")]
    {
        let c = Compression::Gzip { level: 6 };
        let (bytes, enc_s, dec_s) = measure_compressed(&batch, c, max_uncompressed_len);
        let t_transfer = transfer_time_seconds(bytes.len(), rtt_ms, mbit);
        println!(
            "gzip(level=6): bytes={} enc_s={:.6} dec_s={:.6} t_transfer_s={:.6} t_total_s={:.6}",
            bytes.len(),
            enc_s,
            dec_s,
            t_transfer,
            enc_s + t_transfer + dec_s
        );
    }
}
