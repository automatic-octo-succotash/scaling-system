import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from worker import db, sync
from worker.client import RDClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

_REFRESH_BUFFER = timedelta(minutes=5)


def _require(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        log.error("Required environment variable %s is not set", key)
        sys.exit(1)
    return val


def main() -> None:
    database_url  = _require("DATABASE_URL")
    client_id     = _require("RD_CLIENT_ID")
    client_secret = _require("RD_CLIENT_SECRET")

    conn = db.connect(database_url)
    state = db.get_oauth_state(conn)

    expires_at: datetime = state["expires_at"]
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    client = RDClient(
        access_token=state["access_token"],
        refresh_token=state["refresh_token"],
        client_id=client_id,
        client_secret=client_secret,
        on_refresh=lambda at, rt, exp: db.update_oauth_state(conn, at, rt, exp),
    )

    # Proactively refresh the access token if it's close to expiry.
    if expires_at - datetime.now(timezone.utc) < _REFRESH_BUFFER:
        log.info("Access token expires at %s — refreshing proactively", expires_at.isoformat())
        client.refresh()

    sync.run(client, conn)
    conn.close()


if __name__ == "__main__":
    main()
