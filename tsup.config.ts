import { defineConfig } from 'tsup';

export default defineConfig({
  entry: { index: 'src/index.ts' },
  format: ['esm', 'cjs'],
  dts: true,
  sourcemap: true,
  clean: true,
  target: 'node22',
  platform: 'node',
  outDir: 'dist',
  splitting: false,
  treeshake: true,
  // Runtime dependencies stay external. They are installed by the consumer.
  external: ['better-sqlite3', 'ioredis', 'cron-parser'],
});
