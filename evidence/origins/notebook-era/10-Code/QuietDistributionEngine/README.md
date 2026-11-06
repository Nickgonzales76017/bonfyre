# Quiet Distribution Engine

Low-noise distribution system that turns vault outputs into outreach messages, listings, and landing pages. Tracks sends, replies, and channel performance.

## Usage

```bash
# Generate outreach copy from an offer file
python distribute.py generate --offer offers/transcription.md --channel dm

# List available channels and templates
python distribute.py channels

# Log an outreach send
python distribute.py send --channel dm --target "john@example.com" --message "founder-cold-v1"

# Log a reply or response
python distribute.py reply --send-id 1 --type positive --notes "Interested, wants pricing"

# View channel performance
python distribute.py report

# Write a lightweight status snapshot
python distribute.py status
```

## Channels

- `dm` — Direct messages (Twitter, LinkedIn, email)
- `listing` — Marketplace listings (Fiverr, Upwork, etc.)
- `post` — Content posts (Twitter, Reddit, Indie Hackers)
- `landing` — Landing page / link-in-bio

## Dependencies

- Python 3.10+
- SQLite (built-in)

## House Style

- persistent data lives in `data/`
- generated snapshots live in `reports/`
- `status` gives the fast project-level read
