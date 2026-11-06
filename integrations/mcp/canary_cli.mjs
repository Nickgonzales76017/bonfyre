// Retired to cmd/BonfyreMcp ('bonfyre-mcp canary'); kept only while installed wrappers in ~/.local/bin still name this path.
const child = Bun.spawnSync([new URL('../../cmd/BonfyreMcp/bonfyre-mcp', import.meta.url).pathname, 'canary', ...process.argv.slice(2)], { stdio: ['inherit', 'inherit', 'inherit'] });
process.exit(child.exitCode ?? 1);
