"""Populates Qdrant with the three RAG collections used by the qdrant-rag MCP:

  - sample_repo_code:       source files under sample-repo/
  - jira_history:           real past tickets/comments from the Jira project
  - sonar_history_resolved: real, already-resolved SonarQube findings

Runs once (or on demand) as the rag-indexer container / manually via
`python scripts/index_rag_corpus.py`. Embeddings are computed locally with
sentence-transformers — no external embedding API calls.
"""
import base64
import os
import sys
import uuid
from pathlib import Path

import httpx
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sonar_client import fetch_resolved_history  # noqa: E402

load_dotenv()

QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
SAMPLE_REPO_DIR = Path(os.environ.get("SAMPLE_REPO_DIR", "./sample-repo"))
EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
VECTOR_SIZE = 384  # all-MiniLM-L6-v2 output dimension
# mcp-server-qdrant (.vscode/mcp.json, EMBEDDING_MODEL=sentence-transformers/
# all-MiniLM-L6-v2) SIEMPRE consulta un vector NOMBRADO con este slug interno
# de FastEmbed -- si esta coleccion se crea con un vector sin nombre (como
# antes), toda query real del MCP falla con 400 "Vector with name
# fast-all-minilm-l6-v2 is not configured in this collection".
MCP_VECTOR_NAME = "fast-all-minilm-l6-v2"

SOURCE_EXTENSIONS = {".java", ".ts", ".py"}


def get_client() -> QdrantClient:
    return QdrantClient(url=QDRANT_URL)


def ensure_collection(client: QdrantClient, name: str):
    existing = {c.name for c in client.get_collections().collections}
    if name not in existing:
        client.create_collection(
            collection_name=name,
            vectors_config={MCP_VECTOR_NAME: VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE)},
        )


def index_sample_repo_code(client: QdrantClient, model: SentenceTransformer):
    ensure_collection(client, "sample_repo_code")
    points = []
    for path in SAMPLE_REPO_DIR.rglob("*"):
        if path.is_file() and path.suffix in SOURCE_EXTENSIONS:
            text = path.read_text(encoding="utf-8", errors="ignore")
            vector = model.encode(text).tolist()
            points.append(
                PointStruct(
                    id=str(uuid.uuid5(uuid.NAMESPACE_URL, str(path))),
                    vector={MCP_VECTOR_NAME: vector},
                    payload={"path": str(path.relative_to(SAMPLE_REPO_DIR)), "text": text},
                )
            )
    if points:
        client.upsert(collection_name="sample_repo_code", points=points)
    print(f"[index_rag_corpus] sample_repo_code: {len(points)} archivos indexados")


def index_jira_history(client: QdrantClient, model: SentenceTransformer):
    ensure_collection(client, "jira_history")

    jira_url = os.environ.get("JIRA_URL")
    email = os.environ.get("JIRA_EMAIL")
    token = os.environ.get("JIRA_API_TOKEN")
    project_key = os.environ.get("JIRA_PROJECT_KEY")

    if not all([jira_url, email, token, project_key]):
        print("[index_rag_corpus] jira_history: faltan variables JIRA_* / JIRA_PROJECT_KEY, se omite")
        return

    auth = base64.b64encode(f"{email}:{token}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {auth}", "Accept": "application/json"}

    # GET /rest/api/3/search fue dado de baja por Atlassian (410 Gone) -- el
    # reemplazo es POST /rest/api/3/search/jql, con el JQL/campos en el body.
    resp = httpx.post(
        f"{jira_url.rstrip('/')}/rest/api/3/search/jql",
        headers=headers,
        json={"jql": f"project = {project_key} ORDER BY updated DESC", "maxResults": 50, "fields": ["summary", "description"]},
        timeout=20.0,
    )
    resp.raise_for_status()
    issues = resp.json().get("issues", [])

    points = []
    for issue in issues:
        summary = (issue.get("fields") or {}).get("summary", "")
        text = f"{issue.get('key')}: {summary}"
        vector = model.encode(text).tolist()
        points.append(
            PointStruct(
                id=str(uuid.uuid5(uuid.NAMESPACE_URL, issue.get("key", str(uuid.uuid4())))),
                vector={MCP_VECTOR_NAME: vector},
                payload={"ticket_id": issue.get("key"), "summary": summary},
            )
        )
    if points:
        client.upsert(collection_name="jira_history", points=points)
    print(f"[index_rag_corpus] jira_history: {len(points)} tickets indexados")


def index_sonar_history(client: QdrantClient, model: SentenceTransformer):
    ensure_collection(client, "sonar_history_resolved")

    if not os.environ.get("SONAR_TOKEN"):
        print("[index_rag_corpus] sonar_history_resolved: falta SONAR_TOKEN, se omite")
        return

    try:
        issues = fetch_resolved_history()
    except httpx.HTTPStatusError as exc:
        print(f"[index_rag_corpus] sonar_history_resolved: error HTTP {exc.response.status_code}, se omite")
        return

    points = []
    for issue in issues:
        text = f"{issue.get('rule')}: {issue.get('message')}"
        vector = model.encode(text).tolist()
        points.append(
            PointStruct(
                id=str(uuid.uuid4()),
                vector={MCP_VECTOR_NAME: vector},
                payload={"rule": issue.get("rule"), "message": issue.get("message"), "component": issue.get("component")},
            )
        )
    if points:
        client.upsert(collection_name="sonar_history_resolved", points=points)
    print(f"[index_rag_corpus] sonar_history_resolved: {len(points)} hallazgos indexados")


def main():
    client = get_client()
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)

    index_sample_repo_code(client, model)
    index_jira_history(client, model)
    index_sonar_history(client, model)


if __name__ == "__main__":
    main()
