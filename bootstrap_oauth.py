#!/usr/bin/env python3
"""
RD Station CRM OAuth2 bootstrap — one-time token seeding.

Usage:
    python bootstrap_oauth.py

What it does:
  1. Prints the authorization URL — open it in a browser and grant access.
  2. Starts a local HTTP server on port 3000 to catch the callback with the code.
  3. Exchanges the code for access_token + refresh_token via RD Station's API.
  4. Inserts the tokens into crm.oauth_state using kubectl exec into the postgres pod.

Requirements:
  - Python 3.8+  (no third-party packages needed)
  - kubectl configured against your k3s cluster (KUBECONFIG env or ~/.kube/config)

Environment variables (all optional — script will prompt if missing):
    RD_CLIENT_ID        RD Station app client ID
    RD_CLIENT_SECRET    RD Station app client secret
    KUBECONFIG          path to kubeconfig  (default: ~/.kube/config)
    KUBE_NAMESPACE      k8s namespace       (default: app)
    POSTGRES_POD        postgres pod name   (default: postgres-0)
    POSTGRES_USER       postgres role       (default: hcubasd)
    POSTGRES_DB         database name       (default: postgres)
"""

import getpass
import http.server
import json
import os
import subprocess
import sys
import threading
import urllib.parse
import urllib.request
import webbrowser
from datetime import datetime, timezone, timedelta

# ── config ────────────────────────────────────────────────────────────────────

CALLBACK_PORT = 3000
CALLBACK_PATH = "/callback"
REDIRECT_URI  = f"http://localhost:{CALLBACK_PORT}{CALLBACK_PATH}"
AUTH_URL_BASE = "https://api.rd.services/auth/dialog"
TOKEN_URL     = "https://api.rd.services/auth/token?token_by=code"

KUBE_NAMESPACE  = os.environ.get("KUBE_NAMESPACE", "app")
POSTGRES_POD    = os.environ.get("POSTGRES_POD",   "postgres-0")
POSTGRES_USER   = os.environ.get("POSTGRES_USER",  "hcubasd")
POSTGRES_DB     = os.environ.get("POSTGRES_DB",    "postgres")

# ── helpers ───────────────────────────────────────────────────────────────────

def prompt_value(label: str, env_key: str, secret: bool = False) -> str:
    value = os.environ.get(env_key, "").strip()
    if value:
        display = f"{'*' * min(len(value), 8)}  (from env {env_key})"
        print(f"  {label}: {display}")
        return value
    fn = getpass.getpass if secret else input
    return fn(f"  {label}: ").strip()


def exchange_code(client_id: str, client_secret: str, code: str) -> dict:
    payload = json.dumps({
        "client_id":     client_id,
        "client_secret": client_secret,
        "code":          code,
        "redirect_uri":  REDIRECT_URI,
    }).encode()
    req = urllib.request.Request(
        TOKEN_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def insert_tokens_kubectl(access_token: str, refresh_token: str, expires_in: int):
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()

    sql = (
        "INSERT INTO crm.oauth_state (id, access_token, refresh_token, expires_at, updated_at) "
        f"VALUES (1, '{access_token}', '{refresh_token}', '{expires_at}', NOW()) "
        "ON CONFLICT (id) DO UPDATE "
        "SET access_token = EXCLUDED.access_token, "
        "    refresh_token = EXCLUDED.refresh_token, "
        "    expires_at = EXCLUDED.expires_at, "
        "    updated_at = NOW();"
    )

    kubeconfig = os.environ.get("KUBECONFIG", "")
    cmd = ["kubectl"]
    if kubeconfig:
        cmd += ["--kubeconfig", kubeconfig]
    cmd += [
        "exec", "-n", KUBE_NAMESPACE, POSTGRES_POD,
        "--", "psql", "-U", POSTGRES_USER, "-d", POSTGRES_DB, "-c", sql,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  kubectl exec failed:\n{result.stderr}")
        print("\n  Run this SQL manually instead:\n")
        _print_sql(access_token, refresh_token, expires_in)
        sys.exit(1)

    print("  crm.oauth_state updated successfully.")


def _print_sql(access_token: str, refresh_token: str, expires_in: int):
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()
    print(f"""
INSERT INTO crm.oauth_state (id, access_token, refresh_token, expires_at, updated_at)
VALUES (
  1,
  '{access_token}',
  '{refresh_token}',
  '{expires_at}',
  NOW()
)
ON CONFLICT (id) DO UPDATE
    SET access_token  = EXCLUDED.access_token,
        refresh_token = EXCLUDED.refresh_token,
        expires_at    = EXCLUDED.expires_at,
        updated_at    = NOW();
""")

# ── local callback server ─────────────────────────────────────────────────────

_code_queue: list[str] = []


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == CALLBACK_PATH:
            params = urllib.parse.parse_qs(parsed.query)
            code = params.get("code", [None])[0]
            if code:
                _code_queue.append(code)
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"<h2>Authorization complete. You can close this tab.</h2>")
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing code parameter.")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):
        pass


def _listen_for_code(event: threading.Event):
    server = http.server.HTTPServer(("localhost", CALLBACK_PORT), _CallbackHandler)
    while not _code_queue:
        server.handle_request()
    server.server_close()
    event.set()

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n── RD Station OAuth2 Bootstrap ──\n")
    print("Credentials:\n")
    client_id     = prompt_value("RD_CLIENT_ID",     "RD_CLIENT_ID")
    client_secret = prompt_value("RD_CLIENT_SECRET", "RD_CLIENT_SECRET", secret=True)
    print()

    if not client_id or not client_secret:
        print("Error: RD_CLIENT_ID and RD_CLIENT_SECRET are required.")
        sys.exit(1)

    auth_url = (
        f"{AUTH_URL_BASE}"
        f"?client_id={urllib.parse.quote(client_id)}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
    )

    code_event = threading.Event()
    listener = threading.Thread(target=_listen_for_code, args=(code_event,), daemon=True)
    listener.start()

    print(f"Opening browser for RD Station authorization...")
    print(f"  {auth_url}\n")
    print(f"Listening for callback on {REDIRECT_URI}  (timeout: 2 min)\n")
    webbrowser.open(auth_url)

    code_event.wait(timeout=130)

    if not _code_queue:
        print("Timed out waiting for callback. Re-run the script and complete the flow within 2 minutes.")
        sys.exit(1)

    code = _code_queue[0]
    print("  Callback received.\n")

    print("Exchanging code for tokens...")
    try:
        data = exchange_code(client_id, client_secret, code)
    except Exception as exc:
        print(f"  Request failed: {exc}")
        sys.exit(1)

    if "errors" in data:
        print(f"  API returned errors: {data['errors']}")
        sys.exit(1)

    access_token  = data.get("access_token")
    refresh_token = data.get("refresh_token")
    expires_in    = int(data.get("expires_in", 86400))

    if not access_token or not refresh_token:
        print(f"  Unexpected response shape: {data}")
        sys.exit(1)

    print(f"  access_token:  {access_token[:12]}...  (expires in {expires_in}s / {expires_in // 3600}h)")
    print(f"  refresh_token: {refresh_token[:12]}...\n")

    print("Writing tokens to crm.oauth_state via kubectl exec...")
    insert_tokens_kubectl(access_token, refresh_token, expires_in)

    print("\nDone. Token rotation is seeded — the worker will take it from here.\n")


if __name__ == "__main__":
    main()
