import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { VERSION } from "./version.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = dirname(__dirname);
const sourcePath = join(repoRoot, "aurekai.manifest.json");
const outPath = process.argv[2] || join(repoRoot, "dist", "aurekai.manifest.json");
const target = process.env.HYPER_TARGET || "bun-darwin-arm64";

const manifest = JSON.parse(readFileSync(sourcePath, "utf8"));
manifest.release = VERSION.release;
manifest.target = target;

mkdirSync(dirname(outPath), { recursive: true });
writeFileSync(outPath, `${JSON.stringify(manifest, null, 2)}\n`);

process.stdout.write(`${outPath}\n`);