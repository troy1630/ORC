import httpx


class PortainerClient:
    def __init__(self, base_url: str, api_token: str) -> None:
        self._base = base_url.rstrip("/")
        self._headers = {"X-API-Key": api_token}

    def _get(self, path: str, **params) -> httpx.Response:
        return httpx.get(
            f"{self._base}{path}", headers=self._headers, params=params, timeout=15
        )

    def health_check(self) -> bool:
        try:
            r = httpx.get(f"{self._base}/api/status", headers=self._headers, timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def get_endpoints(self) -> list[dict]:
        r = self._get("/api/endpoints")
        r.raise_for_status()
        return r.json()

    def get_containers(self, endpoint_id: int) -> list[dict]:
        r = self._get(
            f"/api/endpoints/{endpoint_id}/docker/containers/json", all=True
        )
        r.raise_for_status()
        return r.json()

    def get_container_logs(
        self, endpoint_id: int, container_id: str, since: int = 0
    ) -> str:
        params: dict = {
            "stdout": True,
            "stderr": True,
            "timestamps": True,
            "tail": 200,
        }
        if since:
            params["since"] = since + 1  # avoid re-ingesting the last seen line
        r = httpx.get(
            f"{self._base}/api/endpoints/{endpoint_id}/docker/containers/{container_id}/logs",
            headers=self._headers,
            params=params,
            timeout=30,
        )
        r.raise_for_status()
        return r.text
