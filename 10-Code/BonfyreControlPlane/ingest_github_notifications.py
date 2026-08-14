#!/usr/bin/env python3
"""Turn GitHub notifications into occurrences, and bot noise into nothing.

Import tooling. The Run 6 pathology this replaces: nine notifications arrive,
all nine are dumped into the next frontier prompt, and the model re-derives
which ones actually mattered. Most of them are bots.

The split is made by looking at who last spoke, not by guessing from the
subject line. A notification whose most recent activity is a deployment bot or a
usage-limit notice is not a state change, and produces no occurrence.

    python3 ingest_github_notifications.py --dry-run
    python3 ingest_github_notifications.py
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import external_events as ee

UTC = dt.timezone.utc
DEFAULT_DB = Path(__file__).resolve().parent / "control_plane.db"

# Accounts whose activity is never a relationship transition.
BOT_LOGINS = {
    "vercel",
    "vercel[bot]",
    "github-actions",
    "github-actions[bot]",
    "codecov",
    "codecov[bot]",
    "dependabot",
    "dependabot[bot]",
    "netlify",
    "netlify[bot]",
    "chatgpt-codex-connector",
    "chatgpt-codex-connector[bot]",
}


def gh_json(endpoint: str) -> object:
    completed = subprocess.run(
        ["gh", "api", endpoint],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip()[:300])
    return json.loads(completed.stdout)


@dataclass
class Notification:
    repo: str
    subject_type: str
    title: str
    reason: str
    updated_at: str
    api_url: str

    @property
    def number(self) -> str:
        return self.api_url.rsplit("/", 1)[-1] if self.api_url else ""

    @property
    def ref(self) -> str:
        return f"{self.repo}#{self.number}"


def fetch_notifications(limit: int) -> list[Notification]:
    raw = gh_json(f"notifications?all=true&per_page={limit}")
    found = []
    for item in raw:  # type: ignore[union-attr]
        subject = item.get("subject") or {}
        found.append(
            Notification(
                repo=(item.get("repository") or {}).get("full_name", ""),
                subject_type=subject.get("type", ""),
                title=subject.get("title", ""),
                reason=item.get("reason", ""),
                updated_at=item.get("updated_at", ""),
                api_url=subject.get("url") or "",
            )
        )
    return found


def last_human_actor(notification: Notification) -> tuple[str, str, str]:
    """Who last spoke, and how. Returns (login, kind, evidence_ref).

    kind is 'human', 'bot', or 'unknown'. The evidence ref is the comment or
    subject URL, so the occurrence points at something a person can open.
    """
    if not notification.api_url:
        return "", "unknown", ""
    try:
        comments = gh_json(f"{notification.api_url}/comments?per_page=20")
    except RuntimeError:
        return "", "unknown", notification.api_url

    if not isinstance(comments, list) or not comments:
        try:
            subject = gh_json(notification.api_url)
        except RuntimeError:
            return "", "unknown", notification.api_url
        login = ((subject or {}).get("user") or {}).get("login", "")  # type: ignore[union-attr]
        kind = "bot" if login.lower() in BOT_LOGINS else "human"
        return login, kind, (subject or {}).get("html_url", notification.api_url)  # type: ignore[union-attr]

    latest = comments[-1]
    login = (latest.get("user") or {}).get("login", "")
    kind = "bot" if login.lower() in BOT_LOGINS else "human"
    return login, kind, latest.get("html_url", notification.api_url)


def classify(
    notification: Notification, actor_kind: str, login: str, self_login: str
) -> str | None:
    """Map a notification to an occurrence kind, or None for no-op.

    Three things produce nothing: a bot spoke, *we* spoke last, or the reason
    carries no state. The second matters most in practice -- a thread whose most
    recent activity is our own comment is us waiting, not the world moving, and
    treating it as an event is how a queue fills with our own echo.
    """
    if actor_kind == "bot":
        return None
    if self_login and login.lower() == self_login.lower():
        return None
    if notification.reason == "assign":
        return ee.STATE_CHANGED
    if notification.reason in {"mention", "comment", "review_requested"}:
        return ee.INBOUND_REPLY
    if notification.reason in {"author", "state_change", "subscribed"}:
        return ee.STATE_CHANGED
    return None


def current_login() -> str:
    try:
        user = gh_json("user")
        return (user or {}).get("login", "")  # type: ignore[union-attr]
    except RuntimeError:
        return ""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    self_login = current_login()
    notifications = fetch_notifications(args.limit)

    db = None
    if not args.dry_run:
        db = __import__("sqlite3").connect(args.db)
        ee.ensure_schema(db)

    committed = suppressed = duplicate = 0
    rows = []

    for notification in notifications:
        login, actor_kind, evidence = last_human_actor(notification)
        kind = classify(notification, actor_kind, login, self_login)
        if kind is None:
            suppressed += 1
            why = "bot" if actor_kind == "bot" else ("self" if login and self_login and login.lower() == self_login.lower() else "no-op")
            rows.append((notification.ref, f"suppressed/{why}", login or actor_kind, ""))
            continue

        observed = dt.datetime.fromisoformat(
            notification.updated_at.replace("Z", "+00:00")
        )
        if args.dry_run:
            committed += 1
            rows.append((notification.ref, kind, login, evidence))
            continue

        event_id = ee.observe(
            db,
            source="github",
            actor=notification.repo,
            event_kind=kind,
            subject_ref=notification.ref,
            observed_at=observed,
            payload={
                "title": notification.title,
                "reason": notification.reason,
                "last_actor": login,
                "subject_type": notification.subject_type,
            },
            evidence_ref=evidence,
        )
        if event_id is None:
            duplicate += 1
            rows.append((notification.ref, "already-recorded", login, evidence))
        else:
            committed += 1
            rows.append((notification.ref, kind, login, evidence))

    width = max((len(row[0]) for row in rows), default=10)
    for ref, kind, actor, evidence in rows:
        marker = "· " if kind.startswith("suppressed") else "  "
        print(f"{marker}{ref:<{width}}  {kind:<16}  {actor}")
        if evidence and not kind.startswith("suppressed") and kind != "already-recorded":
            print(f"    {evidence}")

    print(
        f"\n{len(notifications)} notifications -> {committed} occurrences, "
        f"{suppressed} suppressed as bot/no-op, {duplicate} already recorded"
    )
    if db is not None:
        db.close()


if __name__ == "__main__":
    main()
