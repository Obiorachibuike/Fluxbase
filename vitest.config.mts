import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/**/*.test.ts'],
    testTimeout: 30_000,
    hookTimeout: 30_000,
    pool: 'forks',
    // Integration tests share one Redis; isolation comes from unique key prefixes.
    fileParallelism: true,
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      reporter: ['text', 'json-summary'],
    },
  },
});
