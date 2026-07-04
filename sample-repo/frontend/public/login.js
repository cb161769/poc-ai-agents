// Plain JS on purpose — no bundler needed to serve this for the Playwright
// tests. Mirrors the same call AuthClient.login() makes in authClient.ts.
document.getElementById("login-form").addEventListener("submit", async (event) => {
  event.preventDefault();

  const username = document.getElementById("username").value;
  const password = document.getElementById("password").value;
  const result = document.getElementById("result");

  try {
    const res = await fetch("/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });

    if (!res.ok) {
      throw new Error(`AuthService login failed: ${res.status}`);
    }

    const data = await res.json();
    result.textContent = `Bienvenido, token: ${data.token}`;
    result.dataset.state = "success";
  } catch (err) {
    result.textContent = err.message;
    result.dataset.state = "error";
  }
});
