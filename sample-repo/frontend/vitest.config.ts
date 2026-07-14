import { defineConfig } from "vitest/config";

// Bug real confirmado en una corrida real del testing-agent (Docker-outside-
// of-Docker, KAN-15): sin este config, "vitest run" usa su glob por defecto
// (**/*.{test,spec}.ts) y agarra tests/login.spec.ts -- que es un test de
// Playwright, no de vitest (importa "test" desde "@playwright/test", que
// solo puede correr dentro del test-runner de Playwright, ver
// playwright.config.ts: testDir: "./tests"). Eso hace que "npm test" falle
// siempre, incluso sin ningun cambio real que lo rompa. Se limita vitest a
// src/ (donde estan los tests reales de vitest, ej. authClient.test.ts) y
// se excluye tests/ explicitamente.
export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    exclude: ["tests/**", "node_modules/**"],
  },
});
