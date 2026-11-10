#![feature(portable_simd)]
include!(concat!(env!("OUT_DIR"), "/gigatoken_modules.rs"));
use bpe::Tokenizer;
use std::io::{self, Write};

// Cap retained UTF-8 and token arrays at a bounded work cohort.
const MAX_BATCH_RAW_BYTES: usize = 8 * 1024 * 1024;

fn write_batch<W: Write>(
    output: &mut W,
    workers: &batch::WorkerPool,
    tokenizer: &Tokenizer,
    raw_bodies: &[Vec<u8>],
) -> Result<(), Box<dyn std::error::Error>> {
    if raw_bodies.is_empty() {
        return Ok(());
    }
    // Keep each document whole, preserving the prior lossy UTF-8 semantics.
    let bodies: Vec<_> = raw_bodies
        .iter()
        .map(|body| String::from_utf8_lossy(body))
        .collect();
    let refs: Vec<&[u8]> = bodies.iter().map(|body| body.as_bytes()).collect();
    let (tokens, lengths) = batch::encode_docs_ragged(workers, tokenizer, &refs);
    if lengths.len() != raw_bodies.len() {
        return Err("GigaToken returned an invalid batch shape".into());
    }
    // Validate the complete cohort before exposing its individual frames.
    let mut offset = 0usize;
    for &length in &lengths {
        if length <= 0 || length > u32::MAX as i64 {
            return Err("GigaToken returned an invalid batch shape".into());
        }
        offset = offset
            .checked_add(length as usize)
            .ok_or("GigaToken token offset overflow")?;
        if offset > tokens.len() {
            return Err("GigaToken returned an invalid token count".into());
        }
    }
    if offset != tokens.len() {
        return Err("GigaToken returned an invalid token count".into());
    }
    let mut offset = 0usize;
    for length in lengths {
        let count = length as usize;
        output.write_all(&(count as u32).to_le_bytes())?;
        for token in &tokens[offset..offset + count] {
            output.write_all(&token.to_le_bytes())?;
        }
        offset += count;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let tokenizer_path = args.next().ok_or("usage: bonfyre-gigatoken-native TOKENIZER BODY...")?;
    let paths: Vec<_> = args.collect();
    if paths.is_empty() || paths.len() > u32::MAX as usize {
        return Err("at least one body path is required".into());
    }
    let tokenizer = load_tokenizer::hf::load_hf_bpe(tokenizer_path)?;
    let workers = batch::WorkerPool::new();
    let mut output = io::BufWriter::new(io::stdout().lock());
    // Stable existing frame: document count followed by (count, u32le tokens).
    output.write_all(&(paths.len() as u32).to_le_bytes())?;
    let mut batch_bodies: Vec<Vec<u8>> = Vec::new();
    let mut batch_bytes = 0usize;
    for path in paths {
        let body = std::fs::read(path)?;
        if !batch_bodies.is_empty()
            && batch_bytes.saturating_add(body.len()) > MAX_BATCH_RAW_BYTES
        {
            write_batch(&mut output, &workers, &tokenizer, &batch_bodies)?;
            batch_bodies.clear();
            batch_bytes = 0;
        }
        batch_bytes = batch_bytes.saturating_add(body.len());
        batch_bodies.push(body);
    }
    write_batch(&mut output, &workers, &tokenizer, &batch_bodies)?;
    output.flush()?;
    Ok(())
}
