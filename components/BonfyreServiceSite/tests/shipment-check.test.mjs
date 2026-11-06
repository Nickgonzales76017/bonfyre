// The shipment check's rules on real file bytes: the sample's eight files are
// generated JPEGs and compressed PDFs, read the way the page reads them.
import { test, expect } from 'bun:test';
import { inflateSync } from 'node:zlib';
import { createHash } from 'node:crypto';
import { readShipments, checkBatch, checkedCsv, missingCsv, normalDate, findShipment, documentType, pdfText, jpegText, jpegPhotoDate, storedZip, crc32 } from '../shipment-check.mjs';
import { sampleFiles, SAMPLE_SHIPMENTS, shipmentCheckFiles } from '../shipment-material.mjs';

const facts = async files => Promise.all(Object.entries(files).map(async ([name, body]) => {
  const bytes = new Uint8Array(body), sha256 = createHash('sha256').update(body).digest('hex'), pdf = name.endsWith('.pdf');
  return { key: sha256 + ':' + name, name, size: bytes.length, sha256, text: pdf ? await pdfText(bytes, x => inflateSync(x)) : jpegText(bytes), photo_date: pdf ? '' : jpegPhotoDate(bytes) };
}));

test('the sample reproduces the published check: 5 ready, 1 duplicate, 1 unknown shipment, 1 unreadable date, 2 shipments without proof', async () => {
  const result = checkBatch(readShipments(SAMPLE_SHIPMENTS), await facts(sampleFiles()));
  expect(result.counts).toEqual({ ready: 5, duplicate: 1, unmatched: 1, 'date-needs-review': 1, 'type-needs-review': 0 });
  const by = Object.fromEntries(result.rows.map(row => [row.file_name, row]));
  expect(by['IMG_4821-copy.jpg']).toMatchObject({ disposition: 'duplicate', reasons: ['same content as IMG_4821.jpg'] });
  expect(by['scan_001.pdf']).toMatchObject({ shipment_id: 'DEMO-1001', document_type: 'delivery-proof', date: '2026-10-01', date_from: 'document text', disposition: 'ready' });
  expect(by['SCAN002.pdf']).toMatchObject({ shipment_id: 'DEMO-1002', document_type: 'shipping-label', date: '2026-10-02' });
  expect(by['phone_294.jpg']).toMatchObject({ shipment_id: 'DEMO-1002', document_type: 'cargo-photo', date: '2026-10-02', date_from: 'photo' });
  expect(by['unknown.pdf']).toMatchObject({ disposition: 'unmatched', reasons: ['shipment DEMO-9999 is not in the list'] });
  expect(by['label_unclear.pdf']).toMatchObject({ shipment_id: 'DEMO-1003', disposition: 'date-needs-review', proposed_name: '' });
  expect(by['IMG_992.jpg'].proposed_name).toMatch(/^DEMO-1003_2026-10-03_cargo-photo_[0-9a-f]{8}\.jpg$/);
  expect(result.missing.map(row => row.shipment_id)).toEqual(['DEMO-1002', 'DEMO-1003']);
  expect(checkedCsv(result).split('\n')[0]).toBe('file_name,shipment_id,document_type,date,date_from,disposition,reasons,proposed_name,sha256,bytes');
  expect(missingCsv(result)).toContain('DEMO-1002,2026-10-02,"Fort Worth, TX","Austin, TX",No delivery proof in this batch');
});

test("a person's correction wins over what was read, and fixes the row", async () => {
  const files = await facts(sampleFiles()), unclear = files.find(file => file.name === 'label_unclear.pdf');
  const result = checkBatch(readShipments(SAMPLE_SHIPMENTS), files, { [unclear.key]: { date: '2026-10-03' } });
  expect(result.rows.find(row => row.file_name === 'label_unclear.pdf')).toMatchObject({ disposition: 'ready', date_from: 'person' });
  expect(result.counts.ready).toBe(6);
});

test('dates, references and types are read the way documents write them', () => {
  expect(['2026-10-01', '10/02/2026', 'Oct 3, 2026', '20261004', 'shipped 13/45/2026', 'smudged'].map(normalDate)).toEqual(['2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04', '', '']);
  const list = readShipments('Load,Pickup Date\nTX-88123,2026-10-01\nTX-88124,2026-10-02\n');
  expect(findShipment(list, 'pod_tx88123.pdf')).toEqual({ shipment_id: 'TX-88123' });
  expect(findShipment(list, 'TX-88123 and TX-88124 combined.pdf')).toEqual({ ambiguous: ['TX-88123', 'TX-88124'] });
  expect(['POD-123.pdf', 'BOL scan.pdf', 'label.png', 'IMG_1.HEIC', 'notes.txt'].map(name => documentType(name))).toEqual(['delivery-proof', 'bill-of-lading', 'shipping-label', 'cargo-photo', '']);
  expect(() => readShipments('Name,City\nA,B\n')).toThrow('needs a shipment');
  expect(() => readShipments('shipment\nA1\na1\n')).toThrow('listed twice');
});

test('the renamed-files ZIP is a valid stored archive with the exact bytes', async () => {
  const one = new TextEncoder().encode('hello'), two = new Uint8Array([1, 2, 3, 255]);
  expect(crc32(one)).toBe(0x3610a686);
  const zip = new Uint8Array(await storedZip([{ name: 'a.txt', bytes: one }, { name: 'b.bin', bytes: two }]).arrayBuffer());
  const dir = `${import.meta.dir}/../../../.bonfyre-runtime`, path = `/tmp/shipment-zip-${process.pid}.zip`;
  await Bun.write(path, zip);
  const listed = Bun.spawnSync(['unzip', '-l', path]).stdout.toString(), a = Bun.spawnSync(['unzip', '-p', path, 'a.txt']).stdout.toString();
  expect(listed).toContain('a.txt'); expect(listed).toContain('b.bin'); expect(a).toBe('hello');
  expect(Bun.spawnSync(['unzip', '-t', path]).exitCode).toBe(0);
  await Bun.file(path).delete?.();
});

test('the published package is versioned by content and carries the sample and the client', () => {
  const { asset_prefix, files, sample } = shipmentCheckFiles();
  expect(asset_prefix).toMatch(/^v-[0-9a-f]{64}\/$/);
  for (const name of ['shipment-client.mjs', 'shipment-check.mjs', 'sample.json', 'sample/shipments.csv', 'sample/scan_001.pdf']) expect(files[asset_prefix + name]).toBeDefined();
  expect(sample.files).toHaveLength(8);
  expect(shipmentCheckFiles().asset_prefix).toBe(asset_prefix);
});
