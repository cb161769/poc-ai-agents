"""Batch worker that pulls a token from AuthService — real dependency edge
DataWorker -> AuthService, mirrored in the Neo4j graph."""
import requests


class DataWorker:
    def __init__(self, auth_service_url: str):
        self.auth_service_url = auth_service_url

    def fetch_token(self, username: str, password: str) -> str:
        resp = requests.post(
            f"{self.auth_service_url}/auth/login",
            json={"username": username, "password": password},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()["token"]

    def run_batch(self, token: str) -> int:
        resp = requests.get(
            f"{self.auth_service_url}/data/pending",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        resp.raise_for_status()
        return len(resp.json())
