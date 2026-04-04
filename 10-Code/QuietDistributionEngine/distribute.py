"""Quiet Distribution Engine — outreach tracker and copy generator."""

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "_shared"))

from bonfyre_toolkit import data_path, now_stamp, write_json_report


DB_PATH = data_path(__file__, "distribution.db")
VAULT_ROOT = Path(__file__).resolve().parents[2]
GENERATED_OFFERS_PATH = VAULT_ROOT / "05-Monetization" / "_generated-offers.json"
DISTRIBUTION_SNAPSHOT_NOTE = VAULT_ROOT / "05-Monetization" / "_Distribution Pipeline Snapshot.md"
DISTRIBUTION_SNAPSHOT_JSON = VAULT_ROOT / "05-Monetization" / "_distribution-pipeline-snapshot.json"

CHANNELS = {
    "dm": "Direct messages (Twitter, LinkedIn, email)",
    "listing": "Marketplace listings (Fiverr, Upwork, etc.)",
    "post": "Content posts (Twitter, Reddit, Indie Hackers)",
    "landing": "Landing page / link-in-bio",
}

TEMPLATES = {
    "dm": {
        "founder-cold": (
            "Hey {name} — I run a small transcription service built on local AI. "
            "I turn interview recordings into clean transcripts + summaries + action items, "
            "usually same-day. Would you want to try one for free so you can see the quality?"
        ),
        "operator-cold": (
            "Hi {name} — I noticed you're doing a lot of calls/interviews. "
            "I offer fast AI transcription with human QA — transcripts, summaries, and action items "
            "delivered same-day. Want me to run a sample on one of your recordings?"
        ),
    },
    "listing": {
        "service-listing": (
            "# Professional AI Transcription Service\n\n"
            "Fast, accurate transcription powered by local AI with human quality checks.\n\n"
            "## What You Get\n"
            "- Clean, formatted transcript\n"
            "- Executive summary with key points\n"
            "- Action items extracted automatically\n\n"
            "## Pricing\n"
            "- Standard (< 30 min): $12\n"
            "- Extended (30-60 min): $20\n"
            "- Deep (60+ min, multi-speaker): $35\n\n"
            "Same-day delivery. Satisfaction guaranteed."
        ),
    },
    "post": {
        "value-post": (
            "I built a local AI transcription pipeline that turns messy recordings into "
            "clean transcripts + summaries + action items.\n\n"
            "No cloud. No subscriptions. Just send a file, get a deliverable.\n\n"
            "Happy to run a free sample if anyone wants to test it."
        ),
    },
}


def load_live_offers():
    if not GENERATED_OFFERS_PATH.exists():
        return []
    payload = json.loads(GENERATED_OFFERS_PATH.read_text(encoding="utf-8"))
    offers = payload.get("offers", [])
    return offers if isinstance(offers, list) else []


def _best_offer(offer_name: str = ""):
    offers = load_live_offers()
    if not offers:
        return None
    if offer_name:
        for offer in offers:
            if str(offer.get("offer_name")) == offer_name:
                return offer
    return max(offers, key=lambda item: int(item.get("review_score", 0) or 0))


def _live_template(channel: str, offer: dict) -> str:
    offer_name = str(offer.get("offer_name", "Offer"))
    buyer_segment = str(offer.get("buyer_segment", "buyers"))
    price = str(offer.get("price", "$0"))
    proof_asset = str(offer.get("proof_asset", "proof asset"))
    if channel == "dm":
        return (
            f"Hi [NAME] — I put together a done-for-you offer for {buyer_segment}. "
            f"It turns messy recordings into a clean transcript, summary, and action items. "
            f"The current offer is {price}, and I already have proof from {proof_asset}. "
            "Want me to run one sample for you?"
        )
    if channel == "listing":
        return (
            f"# {offer_name}\n\n"
            f"Built for {buyer_segment}.\n\n"
            "## What You Get\n"
            "- Clean transcript\n"
            "- Executive summary\n"
            "- Action items\n\n"
            f"## Price\n- {price}\n\n"
            f"## Proof\n- Backed by {proof_asset}\n"
        )
    if channel == "post":
        return (
            f"I turned a proof asset from {proof_asset} into a repeatable offer for {buyer_segment}. "
            f"Current price is {price}. "
            "If you have a messy call, memo, or interview, I can turn it into a clean deliverable fast."
        )
    return f"{offer_name} for {buyer_segment} at {price}."


def _followup_move(age_days: int) -> str:
    if age_days <= 1:
        return "wait"
    if age_days <= 3:
        return "soft follow-up"
    if age_days <= 7:
        return "send proof angle"
    return "close loop or archive"


def _followup_message(send_row, offer, age_days: int) -> str:
    buyer_segment = str((offer or {}).get("buyer_segment") or send_row["buyer_segment"] or "buyers")
    proof_asset = str((offer or {}).get("proof_asset") or send_row["proof_asset"] or "a recent proof sample")
    price = str((offer or {}).get("price") or "$18")
    move = _followup_move(age_days)

    if move == "wait":
        return "No follow-up yet. Give this send a little more time."
    if move == "soft follow-up":
        return (
            f"Hi [NAME] — following up in case this got buried. "
            f"I put together this offer for {buyer_segment} and can still run a quick sample if helpful."
        )
    if move == "send proof angle":
        return (
            f"Hi [NAME] — one more nudge. I already have a proof-backed example from {proof_asset}, "
            f"and the current offer is {price}. If you want, I can run one file and let the output speak for itself."
        )
    return (
        "Hi [NAME] — closing the loop for now. "
        "If transcript + summary + action-item cleanup becomes useful later, I can pick this back up quickly."
    )


def _parse_timestamp(value: str):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _pending_rows(db):
    return db.execute(
        "SELECT * FROM sends WHERE reply_type IS NULL ORDER BY sent_at DESC"
    ).fetchall()


def _status_payload(db):
    rows = db.execute("SELECT * FROM sends ORDER BY sent_at DESC").fetchall()
    pending_rows = _pending_rows(db)
    channel_stats = {}
    replied_count = 0
    positive_count = 0
    for row in rows:
        channel = row["channel"]
        stats = channel_stats.setdefault(channel, {"sent": 0, "positive": 0, "negative": 0, "neutral": 0, "waiting": 0})
        stats["sent"] += 1
        reply_type = row["reply_type"] or "waiting"
        stats[reply_type] = stats.get(reply_type, 0) + 1
        if row["reply_type"] is not None:
            replied_count += 1
        if row["reply_type"] == "positive":
            positive_count += 1

    best_channel = None
    best_rate = -1.0
    for channel, stats in channel_stats.items():
        rate = (stats["positive"] / stats["sent"]) if stats["sent"] else 0.0
        if rate > best_rate:
            best_rate = rate
            best_channel = channel

    payload = {
        "project": "QuietDistributionEngine",
        "generated_at": now_stamp(),
        "total_sends": len(rows),
        "channels": channel_stats,
        "live_offer_count": len(load_live_offers()),
        "linked_offer_count": len({row['offer_name'] for row in rows if row['offer_name']}),
        "replied_count": replied_count,
        "pending_count": len(pending_rows),
        "followup_due_count": 0,
        "response_rate": round((replied_count / len(rows)), 2) if rows else 0.0,
        "positive_reply_count": positive_count,
        "positive_reply_rate": round((positive_count / replied_count), 2) if replied_count else 0.0,
        "best_channel": best_channel,
        "best_positive_rate": round(best_rate, 2) if best_channel else 0.0,
        "recent_sends": [],
    }
    now = datetime.now(timezone.utc)
    for row in pending_rows:
        sent_at = _parse_timestamp(row["sent_at"])
        if sent_at and (now - sent_at.astimezone(timezone.utc)).days >= 2:
            payload["followup_due_count"] += 1
    for row in rows[:10]:
        payload["recent_sends"].append({
            "id": row["id"],
            "channel": row["channel"],
            "target": row["target"],
            "offer_name": row["offer_name"],
            "reply_type": row["reply_type"] or "waiting",
            "sent_at": row["sent_at"],
        })
    return payload


def _write_distribution_snapshot(payload):
    DISTRIBUTION_SNAPSHOT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = [
        "---",
        "title: Distribution Pipeline Snapshot",
        "type: monetization_snapshot",
        f"created: {payload['generated_at']}",
        f"updated: {payload['generated_at']}",
        "tags:",
        "  - monetization",
        "  - distribution",
        "  - generated",
        "---",
        "",
        "# Distribution Pipeline Snapshot",
        "",
        f"- Total sends: `{payload['total_sends']}`",
        f"- Linked offers: `{payload['linked_offer_count']}`",
        f"- Replies: `{payload['replied_count']}`",
        f"- Pending: `{payload['pending_count']}`",
        f"- Follow-up due: `{payload['followup_due_count']}`",
        f"- Response rate: `{payload['response_rate']:.0%}`",
        f"- Positive reply rate: `{payload['positive_reply_rate']:.0%}`",
        f"- Best channel: `{payload['best_channel'] or '-'}`",
        "",
        "## Recent Sends",
        "",
    ]
    if payload["recent_sends"]:
        for row in payload["recent_sends"]:
            offer = row["offer_name"] or "unlinked"
            lines.append(
                f"- `#{row['id']}` `{row['channel']}` -> `{row['target']}` "
                f"[{row['reply_type']}] [{offer}] `{str(row['sent_at'])[:10]}`"
            )
    else:
        lines.append("- No sends logged yet.")
    DISTRIBUTION_SNAPSHOT_NOTE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def get_db():
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("""CREATE TABLE IF NOT EXISTS sends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel TEXT NOT NULL,
        target TEXT NOT NULL,
        offer_name TEXT,
        buyer_segment TEXT,
        proof_asset TEXT,
        template TEXT,
        message_preview TEXT,
        sent_at TEXT NOT NULL,
        reply_type TEXT,
        reply_notes TEXT,
        replied_at TEXT
    )""")
    columns = {row["name"] for row in db.execute("PRAGMA table_info(sends)").fetchall()}
    for column_name in ("offer_name", "buyer_segment", "proof_asset"):
        if column_name in columns:
            continue
        try:
            db.execute(f"ALTER TABLE sends ADD COLUMN {column_name} TEXT")
        except sqlite3.OperationalError as error:
            if "duplicate column name" not in str(error).lower():
                raise
    db.commit()
    return db


def cmd_channels(args):
    print("## Channels\n")
    for ch, desc in CHANNELS.items():
        print(f"  {ch:<10} {desc}")
        if ch in TEMPLATES:
            for tpl_name in TEMPLATES[ch]:
                print(f"    └─ {tpl_name}")
    offers = load_live_offers()
    if offers:
        print("\n## Live Offers\n")
        for offer in offers:
            print(f"  - {offer.get('offer_name')} ({offer.get('buyer_segment')}, {offer.get('price')})")
    print()


def cmd_generate(args):
    channel = args.channel
    if channel not in TEMPLATES:
        print(f"No templates for channel: {channel}")
        print(f"Available: {', '.join(TEMPLATES.keys())}")
        return

    # Read offer file for context if provided
    offer_context = ""
    if args.offer:
        offer_path = Path(args.offer)
        if offer_path.exists():
            offer_context = offer_path.read_text(errors="replace")[:500]
            print(f"Loaded offer context from: {offer_path.name}\n")

    live_offer = _best_offer(args.offer_name or "")
    if live_offer:
        print(f"Loaded live offer context from: {live_offer.get('offer_name')}\n")

    print(f"## {channel.upper()} Templates\n")
    for name, template in TEMPLATES[channel].items():
        print(f"### {name}")
        # Replace placeholders with example values
        filled = template.replace("{name}", "[NAME]")
        print(filled)
        print()
    if live_offer:
        print("### live-offer")
        print(_live_template(channel, live_offer))
        print()


def cmd_send(args):
    db = get_db()
    now = now_stamp()
    live_offer = _best_offer(args.offer_name or "")

    # Get template preview if specified
    preview = ""
    if args.message and args.channel in TEMPLATES:
        if args.message in TEMPLATES[args.channel]:
            preview = TEMPLATES[args.channel][args.message][:80] + "..."
    elif live_offer:
        preview = _live_template(args.channel, live_offer)[:140] + "..."

    db.execute(
        """INSERT INTO sends (
            channel, target, offer_name, buyer_segment, proof_asset, template, message_preview, sent_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            args.channel,
            args.target,
            str(live_offer.get("offer_name")) if live_offer else None,
            str(live_offer.get("buyer_segment")) if live_offer else None,
            str(live_offer.get("proof_asset")) if live_offer else None,
            args.message or ("live-offer" if live_offer else ""),
            preview,
            now,
        ),
    )
    db.commit()
    send_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    print(f"Logged send #{send_id}: {args.channel} → {args.target}")
    if live_offer:
        print(f"  Offer: {live_offer.get('offer_name')}")


def cmd_reply(args):
    db = get_db()
    send = db.execute("SELECT * FROM sends WHERE id = ?", (args.send_id,)).fetchone()
    if not send:
        print(f"Send #{args.send_id} not found.")
        return

    now = now_stamp()
    db.execute(
        "UPDATE sends SET reply_type = ?, reply_notes = ?, replied_at = ? WHERE id = ?",
        (args.type, args.notes or "", now, args.send_id),
    )
    db.commit()
    print(f"Send #{args.send_id}: reply logged ({args.type})")


def cmd_report(args):
    db = get_db()
    rows = db.execute("SELECT * FROM sends ORDER BY sent_at DESC").fetchall()

    if not rows:
        print("No sends logged yet.")
        return

    # Channel summary
    channels = {}
    for r in rows:
        ch = r["channel"]
        if ch not in channels:
            channels[ch] = {"sent": 0, "positive": 0, "negative": 0, "silent": 0}
        channels[ch]["sent"] += 1
        rt = r["reply_type"]
        if rt == "positive":
            channels[ch]["positive"] += 1
        elif rt == "negative":
            channels[ch]["negative"] += 1
        elif rt is None:
            channels[ch]["silent"] += 1

    print("## Channel Performance\n")
    print(f"{'Channel':<12} {'Sent':<6} {'+':<6} {'-':<6} {'Silent':<8} {'Rate'}")
    print("-" * 48)
    for ch, stats in channels.items():
        rate = f"{stats['positive'] / stats['sent'] * 100:.0f}%" if stats["sent"] > 0 else "-"
        print(f"{ch:<12} {stats['sent']:<6} {stats['positive']:<6} {stats['negative']:<6} {stats['silent']:<8} {rate}")

    # Recent sends
    print(f"\n## Recent Sends\n")
    for r in rows[:10]:
        reply = r["reply_type"] or "waiting"
        offer_tag = f" [{r['offer_name']}]" if r["offer_name"] else ""
        print(f"  #{r['id']} {r['channel']:<10} → {r['target']:<25} [{reply}]{offer_tag}  {r['sent_at'][:10]}")

    offers = {}
    for r in rows:
        name = r["offer_name"] or "unlinked"
        stats = offers.setdefault(name, {"sent": 0, "positive": 0, "negative": 0, "waiting": 0})
        stats["sent"] += 1
        reply_type = r["reply_type"] or "waiting"
        if reply_type in stats:
            stats[reply_type] += 1
    print(f"\n## Offer Performance\n")
    print(f"{'Offer':<42} {'Sent':<6} {'+':<6} {'-':<6} {'Waiting'}")
    print("-" * 72)
    for name, stats in offers.items():
        print(f"{name:<42} {stats['sent']:<6} {stats['positive']:<6} {stats['negative']:<6} {stats['waiting']}")


def cmd_pending(args):
    db = get_db()
    rows = _pending_rows(db)

    if not rows:
        print("No pending sends.")
        return

    print(f"{'ID':<5} {'Channel':<10} {'Target':<25} {'Offer':<36} {'Age'}")
    print("-" * 100)
    for row in rows:
        offer = row["offer_name"] or "-"
        sent_at = _parse_timestamp(row["sent_at"])
        age_days = (datetime.now(timezone.utc) - sent_at.astimezone(timezone.utc)).days if sent_at else 0
        print(f"{row['id']:<5} {row['channel']:<10} {row['target']:<25} {offer:<36} {str(age_days) + 'd'}")


def cmd_followup(args):
    db = get_db()
    rows = _pending_rows(db)

    if not rows:
        print("No follow-ups needed.")
        return

    print(f"{'ID':<5} {'Target':<25} {'Offer':<36} {'Age':<6} {'Next Move'}")
    print("-" * 120)
    now = datetime.now(timezone.utc)
    for row in rows:
        sent_at = _parse_timestamp(row["sent_at"])
        age_days = (now - sent_at.astimezone(timezone.utc)).days if sent_at else 0
        next_move = _followup_move(age_days)
        offer = row["offer_name"] or "-"
        print(f"{row['id']:<5} {row['target']:<25} {offer:<36} {str(age_days) + 'd':<6} {next_move}")


def cmd_followup_copy(args):
    db = get_db()
    row = db.execute("SELECT * FROM sends WHERE id = ?", (args.send_id,)).fetchone()
    if not row:
        print(f"Send #{args.send_id} not found.")
        return
    if row["reply_type"] is not None:
        print(f"Send #{args.send_id} already has reply type: {row['reply_type']}")
        return

    sent_at = _parse_timestamp(row["sent_at"])
    age_days = (datetime.now(timezone.utc) - sent_at.astimezone(timezone.utc)).days if sent_at else 0
    offer = _best_offer(str(row["offer_name"] or ""))
    next_move = _followup_move(age_days)
    print(f"## Follow-Up For Send #{row['id']}\n")
    print(f"- Target: {row['target']}")
    print(f"- Offer: {row['offer_name'] or '-'}")
    print(f"- Age: {age_days}d")
    print(f"- Recommended move: {next_move}\n")
    print(_followup_message(row, offer, age_days))


def cmd_status(args):
    db = get_db()
    payload = _status_payload(db)
    _write_distribution_snapshot(payload)
    report = write_json_report(__file__, "status.json", payload)
    print(f"Project: {payload['project']}")
    print(
        f"Total sends: {payload['total_sends']} | "
        f"Live offers: {payload['live_offer_count']} | "
        f"Linked offers: {payload['linked_offer_count']}"
    )
    print(
        f"Replies: {payload['replied_count']} | Pending: {payload['pending_count']} | "
        f"Response rate: {payload['response_rate']:.0%}"
    )
    print(
        f"Follow-up due: {payload['followup_due_count']} | "
        f"Best channel: {payload['best_channel'] or '-'} | "
        f"Positive rate: {payload['best_positive_rate']:.0%} | "
        f"Positive reply rate: {payload['positive_reply_rate']:.0%}"
    )
    print(f"Report: {report}")


def main():
    parser = argparse.ArgumentParser(description="Quiet Distribution Engine")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("channels", help="List channels and templates")

    gen_p = sub.add_parser("generate", help="Generate outreach copy")
    gen_p.add_argument("--offer", help="Path to offer markdown file")
    gen_p.add_argument("--offer-name", help="Exact live offer name from 05-Monetization/_generated-offers.json")
    gen_p.add_argument("--channel", required=True, choices=CHANNELS.keys())

    send_p = sub.add_parser("send", help="Log an outreach send")
    send_p.add_argument("--channel", required=True, choices=CHANNELS.keys())
    send_p.add_argument("--target", required=True, help="Who was contacted")
    send_p.add_argument("--message", help="Template name used")
    send_p.add_argument("--offer-name", help="Exact live offer name to link this send to")

    reply_p = sub.add_parser("reply", help="Log a reply")
    reply_p.add_argument("--send-id", required=True, type=int)
    reply_p.add_argument("--type", required=True, choices=["positive", "negative", "neutral"])
    reply_p.add_argument("--notes", default="")

    sub.add_parser("pending", help="List sends with no reply yet")
    sub.add_parser("followup", help="List recommended next touches for pending sends")
    followup_copy_p = sub.add_parser("followup-copy", help="Generate follow-up copy for a pending send")
    followup_copy_p.add_argument("--send-id", required=True, type=int)
    sub.add_parser("report", help="Channel performance report")
    sub.add_parser("status", help="Project status snapshot")

    args = parser.parse_args()
    cmds = {
        "channels": cmd_channels,
        "generate": cmd_generate,
        "send": cmd_send,
        "reply": cmd_reply,
        "pending": cmd_pending,
        "followup": cmd_followup,
        "followup-copy": cmd_followup_copy,
        "report": cmd_report,
        "status": cmd_status,
    }
    if args.command in cmds:
        cmds[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
