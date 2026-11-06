import { readFile } from 'node:fs/promises';
export const WORKBOOK_APP_URI = 'ui://aurekai/financial-review/v1.html';
export const WORKBOOK_APP_RESOURCE = {
  uri: WORKBOOK_APP_URI, name: 'Financial review',
  description: 'Transactions, source cells and exact workbook changes.',
  mimeType: 'text/html;profile=mcp-app',
  _meta: { ui: { csp: { connectDomains: [], resourceDomains: [], frameDomains: [], baseUriDomains: [] } } }
};
export async function readWorkbookApp(uri, file = process.env.BONFYRE_WORKBOOK_MCP_APP_FILE || new URL('./dist/workbook-review.html', import.meta.url)) {
  if (uri !== WORKBOOK_APP_URI) return null;
  const text = await readFile(file, 'utf8');
  if (Buffer.byteLength(text) > 4 * 1024 * 1024 || !text.startsWith('<!doctype html>')) throw Error('Invalid financial review application bundle.');
  return { contents: [{ ...WORKBOOK_APP_RESOURCE, text }] };
}
