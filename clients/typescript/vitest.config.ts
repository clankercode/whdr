import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Integration tests boot a real whdr-server; give them room and keep the
    // shared host calm (repo norm: modest parallelism).
    testTimeout: 30_000,
    hookTimeout: 30_000,
    include: ["test/**/*.test.ts"],
  },
});
