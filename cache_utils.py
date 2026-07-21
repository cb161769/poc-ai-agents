"""File-based TTL cache for outbound calls to Jira/SonarQube.

Keeps the PoC from re-hitting external APIs on every pipeline run while
staying transparent about what was served from cache vs. live.
"""
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Callable

# Bug real confirmado en vivo (epica KAN-4, PR real #243): "./cache" es
# relativo al cwd del proceso que lo importa -- cuando orchestration.py
# corre parado en el repo OBJETIVO (Docker-outside-of-Docker, -w /target-repo),
# "./cache" resolvia DENTRO del repo objetivo, no de poc-ai-agents. Los
# archivos de cache (Jira/Sonar/Figma/graph-query) terminaban commiteados
# en la rama del coding agent junto con el codigo real -- confirmado real:
# "cache/f05611a....json" aparecio commiteado en un PR real. Ancla siempre
# al directorio de ESTE modulo, sin importar desde donde se importe.
CACHE_DIR = Path(os.environ.get("CACHE_DIR") or (Path(__file__).resolve().parent / "cache"))
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "300"))


def _cache_key(namespace: str, params: dict) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(
        json.dumps({"ns": namespace, "params": params}, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return CACHE_DIR / f"{digest}.json"


def cached_call(namespace: str, params: dict, fetch_fn: Callable[[], Any], ttl_seconds: int = None) -> Any:
    """Return fetch_fn() result, cached on disk for ttl_seconds.

    The returned dict always carries a "_cache" key describing whether this
    call was served from cache and how old the entry is, so callers (and the
    orchestrator script) can surface that to the user.
    """
    ttl = ttl_seconds if ttl_seconds is not None else CACHE_TTL_SECONDS
    path = _cache_key(namespace, params)

    if path.exists():
        age = time.time() - path.stat().st_mtime
        if age <= ttl:
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["_cache"] = {"hit": True, "age_seconds": round(age, 1)}
            return payload

    result = fetch_fn()
    path.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    result = dict(result)
    result["_cache"] = {"hit": False, "age_seconds": 0.0}
    return result
