import { beforeEach, describe, expect, it, vi } from "vitest";
import { AuthClient } from "./authClient";

// Minimal real test suite — this is what the testing agent runs
// (`npm test`) against the branch a coding agent produces, before the
// judge ever sees it.
describe("AuthClient.login", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("returns the token on a successful login", async () => {
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ token: "abc123", expiresAt: "2026-01-01T00:00:00Z" }),
    }) as unknown as typeof fetch;

    const client = new AuthClient("https://auth.example.com");
    const result = await client.login("demo", "demo");

    expect(result.token).toBe("abc123");
  });

  it("throws when AuthService responds with an error status", async () => {
    global.fetch = vi.fn().mockResolvedValue({ ok: false, status: 401 }) as unknown as typeof fetch;

    const client = new AuthClient("https://auth.example.com");

    await expect(client.login("demo", "wrong-password")).rejects.toThrow("AuthService login failed: 401");
  });
});
