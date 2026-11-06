#!/usr/bin/env bun
// Build a relocatable, Python-free runtime from explicitly supplied native
// Arrow/Parquet/Flight libraries. Never overwrites an existing bundle.
import { createHash } from "node:crypto";
import { chmodSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, realpathSync, renameSync,
  rmSync, statSync } from "node:fs";
import { dirname, join, resolve, basename } from "node:path";

const root = resolve(import.meta.dir, "../..");
const outAt = process.argv.indexOf("--out");
const out = outAt >= 0 ? resolve(process.argv[outAt + 1] ?? "") : null;
const source = {
  arrow: process.env.ARROW_CPP_LIBARROW,
  parquet: process.env.ARROW_CPP_LIBPARQUET,
  flight: process.env.ARROW_CPP_LIBFLIGHT,
  simdjson: process.env.SIMDJSON_DYLIB,
};
const licenses = {
  "APACHE-ARROW-LICENSE.txt": process.env.ARROW_CPP_LICENSE,
  "APACHE-ARROW-NOTICE.txt": process.env.ARROW_CPP_NOTICE,
  "SIMDJSON-LICENSE.txt": process.env.SIMDJSON_LICENSE,
};
const include = process.env.ARROW_CPP_INCLUDE;
const simdjsonInclude = process.env.SIMDJSON_INCLUDE ?? "/opt/homebrew/include";
if (!out || !include || Object.values({ ...source, ...licenses }).some((path) => !path || !existsSync(path))) {
  console.error("usage: ARROW_CPP_INCLUDE=... ARROW_CPP_LIBARROW=... ARROW_CPP_LIBPARQUET=... ARROW_CPP_LIBFLIGHT=... SIMDJSON_DYLIB=... ARROW_CPP_LICENSE=... ARROW_CPP_NOTICE=... SIMDJSON_LICENSE=... bun build.mjs --out NEW_DIRECTORY");
  process.exit(2);
}
if (existsSync(out)) throw new Error(`refusing to overwrite native bundle: ${out}`);
mkdirSync(dirname(out), { recursive: true });

function run(args) {
  const result = Bun.spawnSync(args, { cwd: root, stderr: "pipe", stdout: "pipe" });
  if (result.exitCode !== 0) throw new Error(`${args[0]} failed: ${result.stderr}`);
  return result.stdout.toString();
}

async function sha256(path) {
  const hash = createHash("sha256");
  for await (const chunk of Bun.file(path).stream()) hash.update(chunk);
  return hash.digest("hex");
}

const stage = mkdtempSync(join(dirname(out), ".bonfyre-arrow-native-"));
try {
  const lib = join(stage, "lib");
  mkdirSync(lib);
  const copied = {};
  for (const [name, path] of Object.entries(source)) {
    const target = join(lib, basename(resolve(path)));
    copyFileSync(path, target);
    // A package manager installs its libraries read-only; the bundle's copy is
    // the bundle's own file and carries the bundle's mode, not the package's.
    chmodSync(target, 0o644);
    copied[name] = { file: basename(target), bytes: statSync(target).size,
      sha256: await sha256(target) };
  }
  const legal = join(stage, "licenses");
  mkdirSync(legal);
  for (const [name, path] of Object.entries(licenses)) copyFileSync(path, join(legal, name));
  const headers = join(stage, "include");
  mkdirSync(join(headers, "arrow", "c"), { recursive: true });
  copyFileSync(join(include, "arrow", "c", "abi.h"), join(headers, "arrow", "c", "abi.h"));
  copyFileSync(join(root, "cmd/BonfyreArrowFlight/include/bonfyre_arrow_c.h"),
    join(headers, "bonfyre_arrow_c.h"));
  const common = ["clang++", "-std=c++20", "-O2", "-Wall", "-Wextra", "-Werror",
    "-isystem", include, "-Wl,-rpath,@executable_path/lib"];
  run([...common, join(root, "cmd/BonfyreArrowStream/src/main.cpp"),
    source.arrow, source.parquet, "-o", join(stage, "bonfyre-arrow-stream")]);
  run([...common, "-isystem", simdjsonInclude,
    join(root, "cmd/BonfyreArrowFlight/src/main.cpp"),
    source.flight, source.parquet, source.arrow, source.simdjson,
    "-o", join(stage, "bonfyre-arrow-flight")]);
  run([...common, "-dynamiclib", "-fvisibility=hidden", "-I",
    join(root, "cmd/BonfyreArrowFlight/include"),
    "-Wl,-rpath,@loader_path/lib",
    "-Wl,-install_name,@rpath/libbonfyre_arrow_c.dylib",
    join(root, "cmd/BonfyreArrowFlight/src/c_data.cpp"),
    source.arrow, source.parquet, "-o", join(stage, "libbonfyre_arrow_c.dylib")]);
  const simdjsonRefs = run(["otool", "-L", join(stage, "bonfyre-arrow-flight")])
    .split("\n").map((line) => line.trim().split(" ")[0])
    .filter((path) => path.includes("libsimdjson") && path !== join(stage, "bonfyre-arrow-flight"));
  if (simdjsonRefs.length !== 1) throw new Error("cannot resolve native simdjson linkage");
  run(["install_name_tool", "-change", simdjsonRefs[0],
    `@rpath/${copied.simdjson.file}`, join(stage, "bonfyre-arrow-flight")]);
  // A bundle that still loads libraries from where a package manager put them
  // is not a bundle: it runs only while that package stays installed. Every
  // library the binaries reach, directly or through another library, is
  // carried in lib/ and every reference is rewritten to find it there.
  const closure = {};
  {
    const system = (path) => path.startsWith("/usr/lib/") || path.startsWith("/System/");
    const deps = (file) => run(["otool", "-L", file]).split("\n").slice(1)
      .map((line) => line.trim().split(" ")[0]).filter((path) => path && !system(path));
    const idOf = (file) => run(["otool", "-D", file]).split("\n")[1]?.trim() ?? "";
    // The libraries copied in by name answer for the install names they were built with.
    const carried = new Map();
    for (const [name, path] of Object.entries(source)) {
      carried.set(idOf(path), copied[name].file);
      carried.set(realpathSync(path), copied[name].file);
    }
    const top = ["bonfyre-arrow-stream", "bonfyre-arrow-flight", "libbonfyre_arrow_c.dylib"].map((name) => join(stage, name));
    const queue = [...top, ...Object.values(copied).map((row) => join(lib, row.file))];
    const seen = new Set(queue);
    // Where each carried library came from, so a sibling it names relatively
    // (@rpath/x, @loader_path/x) can be found beside the original.
    const origin = new Map(Object.entries(source).map(([name, path]) => [join(lib, copied[name].file), realpathSync(path)]));
    const carry = (dep, from) => {
      const name = basename(dep), target = join(lib, name);
      copyFileSync(realpathSync(from), target);
      chmodSync(target, 0o644);
      carried.set(from, name); carried.set(realpathSync(from), name);
      origin.set(target, realpathSync(from));
      closure[name] = { from };
      if (!seen.has(target)) { seen.add(target); queue.push(target); }
      return name;
    };
    while (queue.length) {
      const file = queue.shift();
      const inLib = file.startsWith(lib + "/");
      if (inLib) {
        run(["install_name_tool", "-id", `@rpath/${basename(file)}`, file]);
        // A library in lib/ finds its neighbours beside itself.
        try { run(["install_name_tool", "-add_rpath", "@loader_path", file]); } catch { /* already has it */ }
      }
      for (const dep of deps(file)) {
        if (dep.startsWith("@")) {
          const base = basename(dep);
          if (base === basename(file)) continue;                   // a library's own name
          // An @rpath name already carried under another file name (libarrow.2500.dylib -> libarrow.dylib).
          const known = [...carried.entries()].find(([from]) => basename(from) === base)?.[1];
          if (known) { if (known !== base || !dep.startsWith("@rpath/")) run(["install_name_tool", "-change", dep, `@rpath/${known}`, file]); continue; }
          if (existsSync(join(lib, base))) { if (!dep.startsWith("@rpath/")) run(["install_name_tool", "-change", dep, `@rpath/${base}`, file]); continue; }
          // A sibling named relatively: it sits beside the original, or in the package manager's lib.
          const beside = origin.get(file) ? dirname(origin.get(file)) : null;
          const found = [beside && join(beside, base), "/opt/homebrew/lib/" + base, "/usr/local/lib/" + base, "/opt/local/lib/" + base].find((path) => path && existsSync(path));
          if (!found) { if (top.includes(file)) continue; throw new Error(`cannot find ${dep}, needed by ${basename(file)}`); }
          carry(base, found);
          if (!dep.startsWith("@rpath/")) run(["install_name_tool", "-change", dep, `@rpath/${base}`, file]);
          continue;
        }
        let name = carried.get(dep) ?? (existsSync(dep) ? carried.get(realpathSync(dep)) : undefined);
        if (!name) {
          if (!existsSync(dep)) throw new Error(`cannot carry ${dep}, needed by ${basename(file)}`);
          name = carry(dep, dep);
        }
        run(["install_name_tool", "-change", dep, `@rpath/${name}`, file]);
      }
    }
    for (const file of seen) run(["codesign", "--force", "--sign", "-", file]);
    // Proof, not hope: nothing in the bundle may still name a path outside it.
    for (const file of seen) {
      const outside = deps(file).filter((path) => !path.startsWith("@"));
      if (outside.length) throw new Error(`${basename(file)} still loads ${outside[0]} from outside the bundle`);
      // And every relative name must be answered by a file that is here.
      for (const dep of deps(file).filter((path) => path.startsWith("@rpath/")))
        if (!existsSync(join(lib, basename(dep))) && !existsSync(join(stage, basename(dep))))
          throw new Error(`${basename(file)} names ${dep}, which the bundle does not carry`);
    }
    for (const [name, row] of Object.entries(closure)) {
      const path = join(lib, name);
      Object.assign(row, { bytes: statSync(path).size, sha256: await sha256(path) });
    }
    // Signing and renaming changed the named libraries; the manifest records them as they now are.
    for (const row of Object.values(copied)) Object.assign(row, { bytes: statSync(join(lib, row.file)).size, sha256: await sha256(join(lib, row.file)) });
  }
  const binaries = {};
  for (const name of ["bonfyre-arrow-stream", "bonfyre-arrow-flight", "libbonfyre_arrow_c.dylib"]) {
    const path = join(stage, name);
    binaries[name] = { bytes: statSync(path).size, sha256: await sha256(path) };
  }
  const manifest = { kind: "bonfyre-arrow-native-runtime", format: 1,
    source: "explicit native-library inputs", libraries: copied,
    // Every other library the binaries reach, carried so the bundle runs
    // where the package it was built from is not installed.
    closure, self_contained: true,
    binaries };
  await Bun.write(join(stage, "manifest.json"), JSON.stringify(manifest, null, 2) + "\n");
  renameSync(stage, out);
  console.log(out);
} catch (error) {
  rmSync(stage, { recursive: true, force: true });
  throw error;
}
