// Drain a child pipe while keeping only a bounded tail in memory.
export async function boundedPipe(stream, maxBytes) {
  const chunks = []; let bytes = 0;
  for await (const chunk of stream) {
    const buffer = Buffer.from(chunk);
    chunks.push(buffer); bytes += buffer.length;
    while (bytes > maxBytes && chunks.length) {
      const excess = bytes - maxBytes;
      if (chunks[0].length <= excess) { bytes -= chunks.shift().length; }
      else { chunks[0] = chunks[0].subarray(excess); bytes -= excess; }
    }
  }
  return Buffer.concat(chunks, bytes).toString('utf8');
}
