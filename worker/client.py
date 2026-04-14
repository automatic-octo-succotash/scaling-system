import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator

log = logging.getLogger(__name__)

_BASE = "https://api.rd.services"
_TOKEN_URL = f"{_BASE}/auth/token"


class RDClient:
    def __init__(
        self,
        access_token: str,
        refresh_token: str,
        client_id: str,
        client_secret: str,
        on_refresh: Callable[[str, str, datetime], None] | None = None,
    ):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self._client_id = client_id
        self._client_secret = client_secret
        self._on_refresh = on_refresh

    # ── token management ──────────────────────────────────────────────────────

    def refresh(self) -> None:
        body = json.dumps({
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "refresh_token": self.refresh_token,
        }).encode()
        req = urllib.request.Request(
            _TOKEN_URL,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())

        if "errors" in data:
            raise RuntimeError(f"Token refresh failed: {data['errors']}")

        self.access_token = data["access_token"]
        self.refresh_token = data.get("refresh_token", self.refresh_token)
        expires_in = int(data.get("expires_in", 86400))
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        if self._on_refresh:
            self._on_refresh(self.access_token, self.refresh_token, expires_at)

        log.info("Token refreshed, expires at %s", expires_at.isoformat())

    # ── HTTP ──────────────────────────────────────────────────────────────────

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def get(self, path: str, params: dict | None = None) -> tuple[dict | list, dict]:
        """Single GET request. Returns (body, response_headers). Retries on 401 and 429."""
        url = f"{_BASE}{path}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"

        refreshed = False
        for attempt in range(5):
            req = urllib.request.Request(url, headers=self._headers())
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return json.loads(resp.read()), dict(resp.headers)
            except urllib.error.HTTPError as exc:
                if exc.code == 401 and not refreshed:
                    log.warning("401 on %s — refreshing token and retrying", path)
                    self.refresh()
                    refreshed = True
                elif exc.code == 429:
                    wait = 60 * (2 ** attempt)
                    log.warning("429 rate limited on %s — sleeping %ds", path, wait)
                    time.sleep(wait)
                else:
                    body = exc.read().decode(errors="replace")
                    raise RuntimeError(f"GET {path} → HTTP {exc.code}: {body}") from exc

        raise RuntimeError(f"GET {path} failed after retries")

    def paginate(
        self,
        path: str,
        params: dict | None = None,
        items_key: str | None = None,
    ) -> Iterator[dict]:
        """Yield every item across all pages of a list endpoint."""
        params = dict(params or {})
        params.setdefault("page_size", 125)
        page = 1

        while True:
            params["page"] = page
            body, headers = self.get(path, params)

            if items_key and isinstance(body, dict):
                items = body.get(items_key, [])
            elif isinstance(body, list):
                items = body
            else:
                # try common envelope keys
                items = next(
                    (body[k] for k in ("deals", "users", "products", "stages", "pipelines") if k in body),
                    [],
                )

            yield from items

            total = int(
                headers.get("pagination-total-rows")
                or headers.get("Pagination-Total-Rows")
                or 0
            )
            page_size = int(params["page_size"])

            if not items or (total > 0 and page * page_size >= total):
                break
            page += 1
