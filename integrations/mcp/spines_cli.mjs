// Retired to cmd/BonfyreMcp ('bonfyre-mcp spines'); kept only while installed wrappers in ~/.local/bin still name this path.
const child = Bun.spawnSync([new URL('../../cmd/BonfyreMcp/bonfyre-mcp', import.meta.url).pathname, 'spines', ...process.argv.slice(2)], { stdio: ['inherit', 'inherit', 'inherit'] });
process.exit(child.exitCode ?? 1);
