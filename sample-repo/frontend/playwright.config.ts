import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  webServer: {
    command: "npx serve public -l 4173",
    port: 4173,
    reuseExistingServer: false,
  },
  use: {
    baseURL: "http://localhost:4173",
  },
});
