#!/usr/bin/env python3
"""Blogger publication adapter.

OAuth client configuration is read from ``~/Downloads/credentials.json``.
``~/.bonfyre/secrets/cloud.refs.env`` supplies local secret references without
copying those values into the repository.
Tokens remain local under ``~/.bonfyre/secrets`` and API output is reduced to
the non-secret fields needed for an external-effect receipt.
"""

from __future__ import annotations

import argparse
import http.server
import json
import os
import pathlib
import secrets
import sys
import time
import urllib.parse
import urllib.request
import webbrowser

HOME = pathlib.Path.home()
REFS = HOME / ".bonfyre/secrets/cloud.refs.env"
CREDS = HOME / "Downloads/credentials.json"
TOKEN = HOME / ".bonfyre/secrets/blogger-token.json"
SCOPE = "https://www.googleapis.com/auth/blogger"
AUTH_TIMEOUT_SECONDS = 300


def load_env_refs() -> dict[str, str]:
    values: dict[str, str] = {}
    if REFS.exists():
        for line in REFS.read_text(errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key.startswith("export "):
                key = key.removeprefix("export ").strip()
            values[key] = value.strip().strip("'\"")
    return values


def client_config() -> tuple[str, str, str]:
    refs = load_env_refs()
    reference_id = refs.get("BLOGGER_CLIENT_ID", "")
    reference_secret = refs.get("BLOGGER_CLIENT_SECRET", "")
    client_id = ""
    client_secret = ""
    redirect = "http://localhost"
    if CREDS.exists():
        data = json.loads(CREDS.read_text())
        installed = data.get("installed") or {}
        web = data.get("web") or {}
        config = installed or web
        configured_id = config.get("client_id", "")
        if reference_id and configured_id and reference_id != configured_id:
            raise SystemExit(
                "Blogger client reference does not match credentials.json; "
                "refusing to combine unrelated OAuth configuration."
            )
        client_id = reference_id or configured_id
        client_secret = reference_secret or config.get("client_secret", "")
        uris = config.get("redirect_uris") or []
        if uris:
            redirect = uris[0]
        elif web:
            raise SystemExit(
                "The OAuth web client in ~/Downloads/credentials.json has no "
                "registered redirect URI. Register a loopback redirect in Google "
                "Cloud or replace it with an installed-app client configuration."
            )
    else:
        client_id = reference_id
        client_secret = reference_secret
    if not client_id or not client_secret:
        raise SystemExit(
            "Blogger OAuth client configuration not found. Expected "
            "~/Downloads/credentials.json and/or local secret references."
        )
    return client_id, client_secret, redirect


def load_token() -> dict | None:
    if not TOKEN.exists():
        return None
    return json.loads(TOKEN.read_text())


def save_token(token: dict) -> None:
    TOKEN.parent.mkdir(parents=True, exist_ok=True)
    TOKEN.write_text(json.dumps(token, indent=2, sort_keys=True))
    os.chmod(TOKEN, 0o600)


def post_form(url: str, data: dict) -> dict:
    request = urllib.request.Request(
        url,
        data=urllib.parse.urlencode(data).encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read())


def refresh(token: dict, client_id: str, client_secret: str) -> dict:
    if token.get("expires_at", 0) > time.time() + 120:
        return token
    refresh_token = token.get("refresh_token")
    if not refresh_token:
        raise SystemExit(
            "OAuth token expired and has no refresh token. "
            "Run `bin/blogger_publish.py auth`."
        )
    output = post_form(
        "https://oauth2.googleapis.com/token",
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
    )
    token.update(output)
    token["expires_at"] = time.time() + int(output.get("expires_in", 3600))
    save_token(token)
    return token


def loopback_endpoint(redirect: str) -> tuple[str, int, str]:
    parsed = urllib.parse.urlparse(redirect)
    if (
        parsed.scheme != "http"
        or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}
        or parsed.port is None
    ):
        raise SystemExit(
            "Blogger OAuth requires an explicit loopback redirect such as "
            "http://localhost:8765/callback. Register that exact URI for a "
            "web client, or use an installed-app client that supplies one."
        )
    bind_host = "127.0.0.1" if parsed.hostname == "localhost" else parsed.hostname
    return bind_host, parsed.port, parsed.path or "/"


def receive_authorization_code(redirect: str, auth_url: str, state: str) -> str:
    bind_host, port, expected_path = loopback_endpoint(redirect)
    result: dict[str, str] = {}

    class CallbackHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
            parsed = urllib.parse.urlparse(self.path)
            query = urllib.parse.parse_qs(parsed.query)
            if parsed.path != expected_path:
                self.send_error(404)
                return
            if query.get("state", [""])[0] != state:
                result["error"] = "OAuth callback state mismatch"
                self.send_error(400)
                return
            if query.get("error"):
                result["error"] = "Google authorization was declined or failed"
                self.send_error(400)
                return
            code = query.get("code", [""])[0]
            if not code:
                result["error"] = "OAuth callback did not include a code"
                self.send_error(400)
                return
            result["code"] = code
            body = b"Blogger authorization received. You may close this tab."
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    with http.server.HTTPServer((bind_host, port), CallbackHandler) as server:
        server.timeout = AUTH_TIMEOUT_SECONDS
        if not webbrowser.open(auth_url):
            raise SystemExit("Could not open the Google authorization page.")
        print("Waiting for Google authorization in the browser...")
        server.handle_request()
    if result.get("error"):
        raise SystemExit(result["error"])
    if not result.get("code"):
        raise SystemExit("Timed out waiting for Google authorization.")
    return result["code"]


def auth() -> int:
    client_id, client_secret, redirect = client_config()
    state = secrets.token_urlsafe(32)
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect,
            "response_type": "code",
            "scope": SCOPE,
            "access_type": "offline",
            "prompt": "consent",
            "state": state,
        }
    )
    code = receive_authorization_code(redirect, auth_url, state)
    token = post_form(
        "https://oauth2.googleapis.com/token",
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect,
        },
    )
    token["expires_at"] = time.time() + int(token.get("expires_in", 3600))
    save_token(token)
    print(f"Stored Blogger OAuth token at {TOKEN} with mode 0600.")
    return 0


def api(method: str, path: str, body: dict | None = None) -> dict:
    client_id, client_secret, _ = client_config()
    token = load_token()
    if not token:
        raise SystemExit("No Blogger OAuth token. Run `bin/blogger_publish.py auth` once.")
    token = refresh(token, client_id, client_secret)
    headers = {
        "Authorization": "Bearer " + token["access_token"],
        "Content-Type": "application/json; charset=UTF-8",
    }
    request = urllib.request.Request(
        "https://www.googleapis.com/blogger/v3" + path,
        data=None if body is None else json.dumps(body).encode(),
        headers=headers,
        method=method,
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        return json.loads(response.read())


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd", required=True)
    subparsers.add_parser("auth")
    subparsers.add_parser("blogs")
    post_parser = subparsers.add_parser("post")
    post_parser.add_argument("--blog-id", required=True)
    post_parser.add_argument("--title", required=True)
    post_parser.add_argument("--html-file", required=True)
    post_parser.add_argument("--labels", default="")
    post_parser.add_argument("--draft", action="store_true")
    post_parser.add_argument("--receipt-dir", default="")
    args = parser.parse_args()

    if args.cmd == "auth":
        return auth()
    if args.cmd == "blogs":
        output = api("GET", "/users/self/blogs")
        safe = [
            {"id": item.get("id"), "name": item.get("name"), "url": item.get("url")}
            for item in output.get("items", [])
        ]
        print(json.dumps(safe, indent=2))
        return 0

    content = pathlib.Path(args.html_file).read_text()
    body = {"kind": "blogger#post", "title": args.title, "content": content}
    labels = [item.strip() for item in args.labels.split(",") if item.strip()]
    if labels:
        body["labels"] = labels
    path = (
        f"/blogs/{urllib.parse.quote(args.blog_id)}/posts/"
        f"?isDraft={'true' if args.draft else 'false'}"
    )
    output = api("POST", path, body)
    safe = {
        key: output.get(key)
        for key in ("id", "blog", "published", "updated", "url", "title", "status")
        if key in output
    }
    if args.receipt_dir:
        receipt_dir = pathlib.Path(args.receipt_dir)
        receipt_dir.mkdir(parents=True, exist_ok=True)
        receipt_id = str(output.get("id", "post"))
        (receipt_dir / f"blogger-{receipt_id}.json").write_text(
            json.dumps(safe, indent=2, sort_keys=True)
        )
    print(json.dumps(safe, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
