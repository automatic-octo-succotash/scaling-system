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
  4. Decodes the JWT and warns if the token looks like a global/partner token
     (which the CRM v2 API will reject with "global credentials" errors).
  5. Inserts the tokens into crm.oauth_state via kubectl exec into the postgres pod.

IMPORTANT — app type:
  The CRM v2 API requires a *company-scoped* token, not a global one.
  If you see "The access token is global, but the current plugin is configured
  without 'global_credentials'" from the API, the app in the RD Station
  developer portal was registered as a global/partner integration.

  Fix: in the developer portal, create a new app *without* "global access"
  (or uncheck that option on the existing app), then re-run this script with
  the new client_id / client_secret.

  The CRM-specific authorization URL is:
    https://app.rdstation.com.br/api/v2/auth/dialog

  Set AUTH_URL=crm to use it instead of the default.

Environment variables (all optional — script will prompt if missing):
    RD_CLIENT_ID        RD Station app client ID
    RD_CLIENT_SECRET    RD Station app client secret
    AUTH_URL            "crm"  → https://app.rdstation.com.br/api/v2/auth/dialog
                        "default" or unset → https://api.rd.services/auth/dialog
    KUBECONFIG          path to kubeconfig  (default: ~/.kube/config)
    KUBE_NAMESPACE      k8s namespace       (default: app)
    POSTGRES_POD        postgres pod name   (default: postgres-0)
    POSTGRES_USER       postgres role       (default: hcubasd)
    POSTGRES_DB         database name       (default: postgres)
"""

import base64
import getpass
import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta

# ── config ────────────────────────────────────────────────────────────────────

REDIRECT_URI = "http://localhost:3000/callback"
TOKEN_URL    = "https://api.rd.services/auth/token?token_by=code"

# CRM-specific auth dialog vs the generic one — try AUTH_URL=crm if the default
# produces a global token that the CRM API rejects.
_AUTH_URLS = {
    "crm":     "https://app.rdstation.com.br/api/v2/auth/dialog",
    "default": "https://api.rd.services/auth/dialog",
}

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


def decode_jwt_payload(token: str) -> dict:
    try:
        payload_b64 = token.split(".")[1]
        payload_b64 += "==" * (-len(payload_b64) % 4)
        return json.loads(base64.b64decode(payload_b64))
    except Exception:
        return {}


def warn_if_global(token: str) -> None:
    payload = decode_jwt_payload(token)
    sub = payload.get("sub", "")
    scope = payload.get("scope", "")

    if sub.endswith("@clients") or scope == "":
        print()
        print("  ⚠  WARNING: this token looks like a global/partner token.")
        print("     sub:   ", sub)
        print("     scope: ", scope or "(empty)")
        print()
        print("     The CRM v2 API will reject it with:")
        print("       'The access token is global, but the current plugin")
        print("        is configured without global_credentials'")
        print()
        print("     To fix:")
        print("       1. In the RD Station developer portal, create a new app")
        print("          WITHOUT 'global access' enabled.")
        print("       2. Update RD_CLIENT_ID / RD_CLIENT_SECRET in your secret.")
        print("       3. Re-run this script with AUTH_URL=crm")
        print()


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


def insert_tokens(access_token: str, refresh_token: str, expires_in: int) -> None:
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat()
    at = access_token.replace("'", "''")
    rt = refresh_token.replace("'", "''")

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
        print("\n  Run this SQL manually:\n")
        print(f"  kubectl exec -n {KUBE_NAMESPACE} {POSTGRES_POD} -- psql -U {POSTGRES_USER} -d {POSTGRES_DB} -c \"...\"")
        print(sql)
        sys.exit(1)

    print("  crm.oauth_state updated.")

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n── RD Station OAuth2 Bootstrap ──\n")

    auth_url_key = os.environ.get("AUTH_URL", "default").lower()
    auth_base    = _AUTH_URLS.get(auth_url_key, _AUTH_URLS["default"])
    print(f"  Auth URL: {auth_base}  (set AUTH_URL=crm to use the CRM-specific dialog)\n")

    client_id     = prompt_value("RD_CLIENT_ID",     "RD_CLIENT_ID")
    client_secret = prompt_value("RD_CLIENT_SECRET", "RD_CLIENT_SECRET", secret=True)
    print()

    if not client_id or not client_secret:
        print("Error: RD_CLIENT_ID and RD_CLIENT_SECRET are required.")
        sys.exit(1)

    auth_url = (
        f"{auth_base}"
        f"?client_id={urllib.parse.quote(client_id)}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
    )

    print("1. Open this URL in your browser and grant access:\n")
    print(f"   {auth_url}\n")
    print(f"2. After granting access you'll be redirected to {REDIRECT_URI}?code=XXXX")
    print("   The page will show a connection error (nothing is listening) — that's fine.")
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
    print(f"  refresh_token: {refresh_token[:12]}...")

    warn_if_global(access_token)

    print("Writing tokens to crm.oauth_state...")
    insert_tokens(access_token, refresh_token, expires_in)

    print("\nDone. Token rotation is seeded — the worker will handle refreshes from here.\n")


if __name__ == "__main__":
    main()
