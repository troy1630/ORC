import httpx

# SSL verification disabled — Portainer commonly uses self-signed certs on internal networks.
_CLIENT_KWARGS = {"verify": False, "timeout": 15}
_HEALTH_KWARGS = {"verify": False, "timeout": 5}
_LOG_KWARGS = {"verify": False, "timeout": 30}


def _decode_docker_stream(data: bytes) -> str:
    """
    Decode Docker's multiplexed log stream.

    Non-TTY containers use a framing format: each frame is an 8-byte header
    [stream_type (1), padding (3), payload_size (4 big-endian)] followed by
    payload_size bytes of UTF-8 text.  TTY containers emit raw text with no
    headers.  We detect which format we have from the first byte.
    """
    if not data:
        return ""

    # If the first byte is 0, 1, or 2 and bytes 1-3 are null, it's multiplexed.
    if len(data) >= 8 and data[0] in (0, 1, 2) and data[1:4] == b"\x00\x00\x00":
        parts: list[str] = []
        i = 0
        while i + 8 <= len(data):
            size = int.from_bytes(data[i + 4 : i + 8], "big")
            end = i + 8 + size
            if end > len(data):
                break
            parts.append(data[i + 8 : end].decode("utf-8", errors="replace"))
            i = end
        return "".join(parts)

    # TTY or plain-text stream — decode as-is.
    return data.decode("utf-8", errors="replace")


class PortainerClient:
    def __init__(self, base_url: str, api_token: str) -> None:
        self._base = base_url.rstrip("/")
        self._headers = {"X-API-Key": api_token}

    def _get(self, path: str, **params) -> httpx.Response:
        return httpx.get(
            f"{self._base}{path}", headers=self._headers, params=params, **_CLIENT_KWARGS
        )

    def health_check(self) -> bool:
        try:
            r = httpx.get(f"{self._base}/api/status", headers=self._headers, **_HEALTH_KWARGS)
            return r.status_code == 200
        except Exception:
            return False

    def get_endpoints(self) -> list[dict]:
        r = self._get("/api/endpoints")
        r.raise_for_status()
        return r.json()

    def get_containers(self, endpoint_id: int, all_containers: bool = True) -> list[dict]:
        r = self._get(
            f"/api/endpoints/{endpoint_id}/docker/containers/json",
            all=all_containers,
        )
        r.raise_for_status()
        return r.json()

    def get_running_containers(self, endpoint_id: int) -> list[dict]:
        return [
            c for c in self.get_containers(endpoint_id, all_containers=False)
            if c.get("State", "running") == "running"
        ]

    def get_container_logs(self, endpoint_id: int, container_id: str, since: int = 0) -> str:
        params: dict = {"stdout": True, "stderr": True, "timestamps": True, "tail": 200}
        if since:
            params["since"] = since + 1
        r = httpx.get(
            f"{self._base}/api/endpoints/{endpoint_id}/docker/containers/{container_id}/logs",
            headers=self._headers,
            params=params,
            **_LOG_KWARGS,
        )
        r.raise_for_status()
        return _decode_docker_stream(r.content)  # r.content (bytes), not r.text
