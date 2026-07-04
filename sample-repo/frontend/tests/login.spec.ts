import { test, expect } from "@playwright/test";

// Testing agent para UI real: renderiza public/index.html en un navegador
// headless de verdad y verifica el comportamiento visible, no solo la
// lógica en memoria (eso ya lo cubre authClient.test.ts con Vitest).
// AuthService no corre en este contenedor, asi que se intercepta la llamada
// real a /auth/login — el objetivo es probar la UI, no la red.

test("muestra el token cuando el login es exitoso", async ({ page }) => {
  await page.route("**/auth/login", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ token: "abc123", expiresAt: "2026-01-01T00:00:00Z" }),
    })
  );

  await page.goto("/index.html");
  await page.fill("#username", "demo");
  await page.fill("#password", "demo");
  await page.click("button[type=submit]");

  await expect(page.locator("#result")).toHaveText(/abc123/);
  await expect(page.locator("#result")).toHaveAttribute("data-state", "success");
});

test("muestra un error visible cuando el login falla", async ({ page }) => {
  await page.route("**/auth/login", (route) => route.fulfill({ status: 401 }));

  await page.goto("/index.html");
  await page.fill("#username", "demo");
  await page.fill("#password", "contraseña-incorrecta");
  await page.click("button[type=submit]");

  await expect(page.locator("#result")).toHaveText(/401/);
  await expect(page.locator("#result")).toHaveAttribute("data-state", "error");
});
