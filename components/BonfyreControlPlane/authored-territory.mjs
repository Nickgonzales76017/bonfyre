import { resolve } from 'node:path';

const repo = resolve(import.meta.dir, '../..');
const result = Bun.spawnSync(['cue', 'export', 'schemas/control/standing-address.cue', '--out', 'json'], { cwd: repo });
if (result.exitCode !== 0) throw new Error(result.stderr.toString());
const authored = JSON.parse(result.stdout.toString());
export const territoryActions = Object.freeze(Object.fromEntries(Object.entries(authored.territoryActions).map(([territory, actions]) => [territory, Object.freeze(actions)])));
export const territoryAudience = Object.freeze(authored.territoryAudience);
export const attentionSignals = Object.freeze(authored.attentionSignals);
