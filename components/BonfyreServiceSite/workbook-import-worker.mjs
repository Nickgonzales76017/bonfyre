import { importWorkbook } from './workbook-import.mjs';
self.onmessage = async ({ data }) => {
  try { self.postMessage({ result: await importWorkbook(data.bytes, { name: data.name }) }); }
  catch (error) { self.postMessage({ error: error.message }); }
};
