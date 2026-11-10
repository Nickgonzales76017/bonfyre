/* Presentation metadata only. No transport, data or effect authority lives here.
 * MCP icon URIs remain on the public server origin, with no credentials. */
export const KNIGHT_PATH = '/assets/aurekai-knight.png';
export const KNIGHT_TOOL_PATH = '/assets/aurekai-knight-48.png';
export function knightIcons(origin) {
  try {
    const url = new URL(origin);
    if (url.protocol !== 'https:') return [];
    return [
      {src: `${url.origin}${KNIGHT_TOOL_PATH}`, mimeType: 'image/png', sizes: ['48x48']},
      {src: `${url.origin}${KNIGHT_PATH}`, mimeType: 'image/png', sizes: ['168x168']},
    ];
  } catch { return []; }
}
export function withMcpIcons(items, icons) {
  return items.map(item => item.icons?.length || !icons.length ? item : {...item, icons});
}
