import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';

export const REPO = resolve(import.meta.dir,'../..');
export const SURFACE = 'https://aurekai.life';
export const DESK_PATTERN = /<h3>([^<]*)<\/h3><span class="desk-n">(\d+)<\/span>/g;

export function run(argv,timeoutSeconds=4) {
  try { return spawnSync(argv[0],argv.slice(1),{
    cwd:REPO,encoding:'utf8',timeout:timeoutSeconds*1000,maxBuffer:2*1024*1024,
  }).stdout?.trim() || ''; }
  catch { return ''; }
}
export function deskQueues(html) { return [...html.matchAll(DESK_PATTERN)].map(([,title,count]) => [title,count]); }
export function atlasSummary(raw) {
  const heading = raw.match(/atlas: (\d+) architectures, (\d+) views, (\d+) interactions/);
  const maturity = raw.match(/maturity: ([^\n]+)/);
  return { heading, maturity:maturity?.[1]?.trim() || '' };
}
export function diskFree(raw) {
  const lines = raw.split('\n');
  return lines.length > 1 ? lines.at(-1).split(/\s+/)[3] || '?' : '?';
}
