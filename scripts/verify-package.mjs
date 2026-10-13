#!/usr/bin/env node
/**
 * Verifies the packed artifact the way a consumer receives it:
 *  1. The tarball contains only the intended files.
 *  2. It installs into an empty project.
 *  3. ESM import, CommonJS require, and TypeScript types all resolve.
 * Requires `npm run build` first. Nothing is published.
 */
import { execFileSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const work = mkdtempSync(join(tmpdir(), 'fluxbase-pack-'));
const run = (cmd, args, cwd) => execFileSync(cmd, args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], shell: process.platform === 'win32' });
const fail = (message) => {
  console.error(`verify-package: ${message}`);
  rmSync(work, { recursive: true, force: true });
  process.exit(1);
};

// 1. Pack and inspect file list.
const packed = JSON.parse(run('npm', ['pack', '--json', '--pack-destination', work], root));
const entry = packed[0];
const files = entry.files.map((f) => f.path);
const allowed = /^(dist\/|README\.md$|LICENSE$|CHANGELOG\.md$|package\.json$)/;
const unexpected = files.filter((f) => !allowed.test(f));
if (unexpected.length) fail(`unexpected files in tarball: ${unexpected.join(', ')}`);
for (const required of ['dist/index.js', 'dist/index.cjs', 'dist/index.d.ts', 'dist/index.d.cts', 'LICENSE', 'README.md']) {
  if (!files.includes(required)) fail(`missing ${required} in tarball`);
}
console.log(`tarball ${entry.filename}: ${files.length} files, all within the allowed set`);

// 2. Install into a clean consumer project.
const consumer = join(work, 'consumer');
mkdirSync(consumer);
writeFileSync(join(consumer, 'package.json'), JSON.stringify({ name: 'consumer', private: true, type: 'module' }));
run('npm', ['install', '--no-audit', '--no-fund', '--silent', join(work, entry.filename), 'typescript@5', '@types/node@22'], consumer);

// 3a. ESM import.
writeFileSync(
  join(consumer, 'esm-check.mjs'),
  `import { Fluxbase, createMemoryCache, FluxbaseError } from 'fluxbase';
const fb = new Fluxbase({ mode: 'memory', logging: { level: 'silent' } });
await fb.start();
await fb.kv.set('k', { v: 1 }, { ttl: 60 });
if ((await fb.kv.get('k'))?.v !== 1) throw new Error('kv round trip failed');
await fb.close();
if (typeof createMemoryCache !== 'function' || !(new FluxbaseError('X', 'm') instanceof Error)) throw new Error('exports missing');
console.log('esm ok');
`,
);
console.log(run('node', ['esm-check.mjs'], consumer).trim());

// 3b. CommonJS require.
writeFileSync(
  join(consumer, 'cjs-check.cjs'),
  `const { Fluxbase } = require('fluxbase');
const fb = new Fluxbase({ mode: 'memory', logging: { level: 'silent' } });
fb.start().then(() => fb.close()).then(() => console.log('cjs ok'));
`,
);
console.log(run('node', ['cjs-check.cjs'], consumer).trim());

// 3c. TypeScript types resolve under NodeNext and reject misuse.
writeFileSync(
  join(consumer, 'types-check.ts'),
  `import { Fluxbase, type Job } from 'fluxbase';
const fb = new Fluxbase<{ 'a.b': { n: number } }>({ mode: 'memory' });
const q = fb.queue<{ to: string }>('mail');
const j: Promise<Job<{ to: string }>> = q.add('x', { to: 'a' });
void j;
fb.events.subscribe('a.b', (e) => { void e.data.n.toFixed(); });
// @ts-expect-error unknown topic must be rejected by the typed event map
fb.events.subscribe('a.c', () => undefined);
`,
);
writeFileSync(
  join(consumer, 'tsconfig.json'),
  JSON.stringify({ compilerOptions: { module: 'NodeNext', moduleResolution: 'NodeNext', strict: true, noEmit: true, skipLibCheck: false, types: ['node'] }, files: ['types-check.ts'] }),
);
run(join(consumer, 'node_modules', '.bin', 'tsc'), ['-p', 'tsconfig.json'], consumer);
console.log('types ok');

rmSync(work, { recursive: true, force: true });
console.log('verify-package: all checks passed');
