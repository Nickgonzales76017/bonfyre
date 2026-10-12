# Personal Data Engine

Vault feedback engine that scans daily logs, project files, and execution data to generate weekly reviews, signal scorecards, and priority recommendations.

## Usage

```bash
# Scan vault logs and generate a weekly review
python engine.py review --vault ~/Documents/Bonfyre --week 2026-04-01

# Score projects by momentum (activity, proof, revenue signals)
python engine.py score --vault ~/Documents/Bonfyre

# Show signal summary (what's moving, what's stalled, what's dead)
python engine.py signals --vault ~/Documents/Bonfyre

# Write a lightweight status snapshot
python engine.py status --vault ~/Documents/Bonfyre

# Generate a daily log template for today
python engine.py daily
```

## How It Works

1. Scans `06-Logs/` for daily log entries
2. Scans `02-Projects/` for execution log updates, task completion, and proof signals
3. Scores each project on momentum, proof, and revenue proximity
4. Generates a structured weekly review with recommendations

## Dependencies

- Python 3.10+
- An Obsidian vault with the Bonfyre folder structure

## House Style

- generated snapshots live in `reports/`
- `status` gives the fast project-level read
