import { spawn } from "node:child_process";
import { readdir, readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const directory = fileURLToPath(new URL("../tests/fixtures/workflow-histories/", import.meta.url));
const existing = await readdir(directory).catch((error) => {
  if (error.code === "ENOENT") return [];
  throw error;
});
if (existing.some((name) => name.endsWith(".json")) && !process.argv.includes("--patched-change")) {
  throw new Error("Histories already exist. Re-record only for a reviewed patched() change; use --patched-change after reviewing the existing replay failure.");
}
const manifestPath = require.resolve("vitest/package.json");
const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
const executable = resolve(dirname(manifestPath), manifest.bin.vitest);
const child = spawn(process.execPath, [executable, "run", "--maxWorkers=1",
  "tests/temporal-turns-sandbox.test.ts", "tests/temporal-supervisor-sandbox.test.ts",
  "tests/temporal-memory-sandbox.test.ts", "tests/temporal-legacy-supervisor-sandbox.test.ts"
], {
  cwd: fileURLToPath(new URL("../", import.meta.url)),
  env: { ...process.env, SPACE_RECORD_TEMPORAL_HISTORIES: "1" }, stdio: "inherit"
});
child.on("error", (error) => { console.error(error); process.exitCode = 1; });
child.on("exit", (code, signal) => { process.exitCode = code ?? (signal ? 1 : 0); });
