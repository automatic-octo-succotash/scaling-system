#!/usr/bin/env python3
"""
RD Station CRM OAuth2 bootstrap — one-time token seeding.

Usage:
    python bootstrap_oauth.py

What it does:
  1. Prints the RD Station authorization URL — open it in your browser.
  2. After you grant access, RD Station redirects to your redirect_uri with
     ?code=XXXX in the URL. Paste that code here.
  3. Immediately exchanges the code for access_token + refresh_token.
  4. Inserts the tokens into crm.oauth_state via kubectl exec into the postgres pod.

Requirements:
  - Python 3.8+  (no third-party packages needed)
  - kubectl configured against your k3s cluster

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
import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta

# ── config ────────────────────────────────────────────────────────────────────

REDIRECT_URI  = "http://localhost:3000/callback"
AUTH_URL_BASE = "https://api.rd.services/auth/dialog"
TOKEN_URL     = "https://api.rd.services/auth/token?token_by=code"

KUBE_NAMESPACE = os.environ.get("KUBE_NAMESPACE", "app")
POSTGRES_POD   = os.environ.get("POSTGRES_POD",   "postgres-0")
POSTGRES_USER  = os.environ.get("POSTGRES_USER",  "hcubasd")
POSTGRES_DB    = os.environ.get("POSTGRES_DB",    "postgres")

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


def insert_tokens(access_token: str, refresh_token: str, expires_in: int):
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()

    # Escape single quotes in token values just in case
    at  = access_token.replace("'", "''")
    rt  = refresh_token.replace("'", "''")

    sql = (
        "INSERT INTO crm.oauth_state (id, access_token, refresh_token, expires_at, updated_at) "
        f"VALUES (1, '{at}', '{rt}', '{expires_at}', NOW()) "
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
        print(f"\n  kubectl exec failed:\n{result.stderr.strip()}")
        print("\n  Run this SQL manually instead:\n")
        print(f"    kubectl exec -n {KUBE_NAMESPACE} {POSTGRES_POD} -- psql -U {POSTGRES_USER} -d {POSTGRES_DB} -c \"<SQL below>\"")
        print(sql)
        sys.exit(1)

    print("  crm.oauth_state updated.")

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n── RD Station OAuth2 Bootstrap ──\n")

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

    print("1. Open this URL in your browser and grant access:\n")
    print(f"   {auth_url}\n")
    print(f"2. After granting access you'll be redirected to {REDIRECT_URI}?code=XXXX")
    print("   The page will show an error (nothing is listening there) — that's fine.")
    print("   Copy the value of the 'code' parameter from the URL bar.\n")

    code = input("Paste the code here: ").strip()
    if not code:
        print("No code provided.")
        sys.exit(1)

    print("\nExchanging code for tokens...")
    try:
        data = exchange_code(client_id, client_secret, code)
    except Exception as exc:
        print(f"  Request failed: {exc}")
        sys.exit(1)

    if "errors" in data:
        print(f"  API error: {data['errors']}")
        sys.exit(1)

    access_token  = data.get("access_token")
    refresh_token = data.get("refresh_token")
    expires_in    = int(data.get("expires_in", 86400))

    if not access_token or not refresh_token:
        print(f"  Unexpected response: {data}")
        sys.exit(1)

    print(f"  access_token:  {access_token[:12]}...  (expires in {expires_in // 3600}h)")
    print(f"  refresh_token: {refresh_token[:12]}...\n")

    print("Writing tokens to crm.oauth_state...")
    insert_tokens(access_token, refresh_token, expires_in)

    print("\nDone. Token rotation is seeded — the worker will handle refreshes from here.\n")


if __name__ == "__main__":
    main()
