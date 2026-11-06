// The shipment check's sample: eight real files (JPEG photos carrying EXIF
// text and dates, compressed PDFs with text) and the shipment list they are
// checked against, built byte for byte the same every time. The page runs
// the same rules on these as on a visitor's own files.
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { deflateSync } from 'node:zlib';

const CRATE = Buffer.from('/9j/4AAQSkZJRgABAQAASABIAAD/4QCARXhpZgAATU0AKgAAAAgABAEaAAUAAAABAAAAPgEbAAUAAAABAAAARgEoAAMAAAABAAIAAIdpAAQAAAABAAAATgAAAAAAAABIAAAAAQAAAEgAAAABAAOgAQADAAAAAQABAACgAgAEAAAAAQAAADCgAwAEAAAAAQAAACAAAAAA/+0AOFBob3Rvc2hvcCAzLjAAOEJJTQQEAAAAAAAAOEJJTQQlAAAAAAAQ1B2M2Y8AsgTpgAmY7PhCfv/AABEIACAAMAMBIgACEQEDEQH/xAAfAAABBQEBAQEBAQAAAAAAAAAAAQIDBAUGBwgJCgv/xAC1EAACAQMDAgQDBQUEBAAAAX0BAgMABBEFEiExQQYTUWEHInEUMoGRoQgjQrHBFVLR8CQzYnKCCQoWFxgZGiUmJygpKjQ1Njc4OTpDREVGR0hJSlNUVVZXWFlaY2RlZmdoaWpzdHV2d3h5eoOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4eLj5OXm5+jp6vHy8/T19vf4+fr/xAAfAQADAQEBAQEBAQEBAAAAAAAAAQIDBAUGBwgJCgv/xAC1EQACAQIEBAMEBwUEBAABAncAAQIDEQQFITEGEkFRB2FxEyIygQgUQpGhscEJIzNS8BVictEKFiQ04SXxFxgZGiYnKCkqNTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqCg4SFhoeIiYqSk5SVlpeYmZqio6Slpqeoqaqys7S1tre4ubrCw8TFxsfIycrS09TV1tfY2dri4+Tl5ufo6ery8/T19vf4+fr/2wBDAAQEBAQEBAYEBAYJBgYGCQwJCQkJDA8MDAwMDA8SDw8PDw8PEhISEhISEhIVFRUVFRUZGRkZGRwcHBwcHBwcHBz/2wBDAQQFBQcHBwwHBwwdFBAUHR0dHR0dHR0dHR0dHR0dHR0dHR0dHR0dHR0dHR0dHR0dHR0dHR0dHR0dHR0dHR0dHR3/3QAEAAP/2gAMAwEAAhEDEQA/APKFtPapltPaugW09qmW09q+awmKPjJYw59bT2qVbT2roFtPapVtPavsMJijCWM8zn1tPapltPaugW09qmW09q+wwmKOeWM8z//QqLae1Srae1dAtp7VMtp7V+X4TFH4vLGeZzy2ntUy2ntXQLae1TLae1fYYTFHPLGeZz62ntUq2ntXQLae1Srae1fYYTFHPLGeZ//Z', 'base64');

// A JPEG with one EXIF segment: ImageDescription and DateTimeOriginal.
export function sampleJpeg(description, taken) {
  const ascii = text => Buffer.concat([Buffer.from(text, 'latin1'), Buffer.from([0])]);
  const desc = ascii(description), date = ascii(taken);
  // TIFF big-endian: IFD0 with ImageDescription (0x010e) and an Exif IFD pointer (0x8769); Exif IFD with DateTimeOriginal (0x9003).
  const ifd0 = 8, ifd0Size = 2 + 2 * 12 + 4, exifIfd = ifd0 + ifd0Size, exifSize = 2 + 12 + 4, data = exifIfd + exifSize;
  const tiff = Buffer.alloc(data + desc.length + date.length);
  tiff.write('MM', 0, 'latin1'); tiff.writeUInt16BE(42, 2); tiff.writeUInt32BE(ifd0, 4);
  tiff.writeUInt16BE(2, ifd0);
  tiff.writeUInt16BE(0x010e, ifd0 + 2); tiff.writeUInt16BE(2, ifd0 + 4); tiff.writeUInt32BE(desc.length, ifd0 + 6); tiff.writeUInt32BE(data, ifd0 + 10);
  tiff.writeUInt16BE(0x8769, ifd0 + 14); tiff.writeUInt16BE(4, ifd0 + 16); tiff.writeUInt32BE(1, ifd0 + 18); tiff.writeUInt32BE(exifIfd, ifd0 + 22);
  tiff.writeUInt32BE(0, ifd0 + 26);
  tiff.writeUInt16BE(1, exifIfd);
  tiff.writeUInt16BE(0x9003, exifIfd + 2); tiff.writeUInt16BE(2, exifIfd + 4); tiff.writeUInt32BE(date.length, exifIfd + 6); tiff.writeUInt32BE(data + desc.length, exifIfd + 10);
  tiff.writeUInt32BE(0, exifIfd + 14);
  desc.copy(tiff, data); date.copy(tiff, data + desc.length);
  const body = Buffer.concat([Buffer.from('Exif\0\0', 'latin1'), tiff]);
  const app1 = Buffer.concat([Buffer.from([0xff, 0xe1, (body.length + 2) >> 8, (body.length + 2) & 255]), body]);
  // Keep the SOI and the image; drop any EXIF the encoder wrote.
  const parts = [CRATE.subarray(0, 2), app1];
  for (let i = 2; i < CRATE.length;) {
    const marker = CRATE[i + 1], length = (CRATE[i + 2] << 8) | CRATE[i + 3];
    if (marker === 0xda) { parts.push(CRATE.subarray(i)); break; }
    if (marker !== 0xe1) parts.push(CRATE.subarray(i, i + 2 + length));
    i += 2 + length;
  }
  return Buffer.concat(parts);
}

// A one-page PDF whose text is in a FlateDecode content stream.
export function samplePdf(lines) {
  const content = 'BT /F1 12 Tf 72 720 Td ' + lines.map((line, i) => (i ? '0 -18 Td ' : '') + '(' + line.replace(/[\\()]/g, m => '\\' + m) + ') Tj').join(' ') + ' ET';
  const stream = deflateSync(Buffer.from(content, 'latin1'));
  const objects = ['<< /Type /Catalog /Pages 2 0 R >>', '<< /Type /Pages /Kids [3 0 R] /Count 1 >>',
    '<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>', null,
    '<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>'];
  const chunks = [Buffer.from('%PDF-1.4\n', 'latin1')], offsets = [];
  let size = chunks[0].length;
  objects.forEach((body, i) => {
    offsets.push(size);
    const part = body === null
      ? Buffer.concat([Buffer.from(`${i + 1} 0 obj\n<< /Length ${stream.length} /Filter /FlateDecode >>\nstream\n`, 'latin1'), stream, Buffer.from('\nendstream\nendobj\n', 'latin1')])
      : Buffer.from(`${i + 1} 0 obj\n${body}\nendobj\n`, 'latin1');
    chunks.push(part); size += part.length;
  });
  const xref = 'xref\n0 6\n0000000000 65535 f \n' + offsets.map(o => String(o).padStart(10, '0') + ' 00000 n \n').join('') +
    `trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n${size}\n%%EOF\n`;
  chunks.push(Buffer.from(xref, 'latin1'));
  return Buffer.concat(chunks);
}

export const SAMPLE_SHIPMENTS = 'shipment_id,ship_date,origin,destination\nDEMO-1001,2026-10-01,"Dallas, TX","Tulsa, OK"\nDEMO-1002,2026-10-02,"Fort Worth, TX","Austin, TX"\nDEMO-1003,2026-10-03,"Dallas, TX","Houston, TX"\n';

export function sampleFiles() {
  const crate1 = sampleJpeg('Load DEMO-1001 dock 4', '2026:10:01 09:12:44');
  return {
    'IMG_4821.jpg': crate1,
    'IMG_4821-copy.jpg': crate1,
    'scan_001.pdf': samplePdf(['PROOF OF DELIVERY', 'Shipment DEMO-1001', 'Delivered Oct 1, 2026', 'Signed: R. Ortiz']),
    'SCAN002.pdf': samplePdf(['SHIPPING LABEL', 'Ref DEMO-1002', 'Ship date 10/02/2026']),
    'phone_294.jpg': sampleJpeg('DEMO-1002 pallets before loading', '2026:10:02 15:40:03'),
    'unknown.pdf': samplePdf(['Proof of delivery', 'Shipment DEMO-9999', '2026-10-03']),
    'label_unclear.pdf': samplePdf(['Shipping label', 'DEMO-1003', 'Ship date: smudged']),
    'IMG_992.jpg': sampleJpeg('DEMO-1003 trailer seal', '2026:10:03 07:58:21'),
  };
}

export function shipmentCheckFiles() {
  const hash = body => createHash('sha256').update(body).digest('hex');
  const files = {}, sample = sampleFiles();
  for (const [name, body] of Object.entries(sample)) files['sample/' + name] = body;
  files['sample/shipments.csv'] = Buffer.from(SAMPLE_SHIPMENTS);
  for (const name of ['shipment-check.mjs', 'shipment-client.mjs', 'shipment-check.css']) files[name] = readFileSync(join(import.meta.dir, name));
  const descriptor = { files: Object.entries(sample).map(([name, body]) => ({ name, bytes: body.length, sha256: hash(body) })), shipments: 'sample/shipments.csv' };
  files['sample.json'] = Buffer.from(JSON.stringify(descriptor) + '\n');
  const identity = Object.fromEntries(Object.keys(files).sort().map(name => [name, hash(files[name])]));
  const asset_prefix = `v-${hash(JSON.stringify(identity))}/`;
  return { sample: descriptor, asset_prefix, files: Object.fromEntries(Object.entries(files).map(([name, body]) => [asset_prefix + name, body])) };
}
