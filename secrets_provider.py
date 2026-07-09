"""Thin indirection for reading credentials, additive on top of the plain
.env/environment-variable approach every module already uses.

get_secret(name) first looks for a "<name>_FILE" environment variable
pointing at a mounted file (the convention Docker secrets and Kubernetes
secrets both use: a file whose contents are the secret, refreshable without
restarting the process that reads it) and falls back to the plain
environment variable if that isn't set. Nothing breaks for anyone still
using a plain .env -- this only adds a real path to a proper secrets
manager for anyone deploying this beyond a laptop.
"""
import os


def get_secret(name: str, default: str = "") -> str:
    file_path = os.environ.get(f"{name}_FILE")
    if file_path:
        with open(file_path, encoding="utf-8") as f:
            return f.read().strip()
    return os.environ.get(name, default)


def require_secret(name: str) -> str:
    """Same as get_secret(), but raises KeyError(name) when missing -- for
    callers that already catch KeyError to report missing_env_var (every
    *_client.py's main() does this today for os.environ[...] lookups).
    """
    value = get_secret(name)
    if not value:
        raise KeyError(name)
    return value
