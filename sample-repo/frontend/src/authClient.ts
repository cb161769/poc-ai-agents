/**
 * Thin client that Frontend uses to talk to AuthService.
 * Real dependency edge Frontend -> AuthService, mirrored in the Neo4j graph.
 */
export interface LoginResponse {
  token: string;
  expiresAt: string;
}

export class AuthClient {
  constructor(private readonly baseUrl: string) {}

  async login(username: string, password: string): Promise<LoginResponse> {
    const res = await fetch(`${this.baseUrl}/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });

    if (!res.ok) {
      throw new Error(`AuthService login failed: ${res.status}`);
    }

    return (await res.json()) as LoginResponse;
  }
}
