# Personal Data Engine

Historical-feedback engine that replays absorbed notebook logs and project files
alongside current execution data to generate reviews and signal scorecards.

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

1. Replays `evidence/origins/notebook-era/06-Logs/` daily log entries
2. Replays `evidence/origins/notebook-era/02-Projects/` project observations
3. Scores each project on momentum, proof, and revenue proximity
4. Generates a structured weekly review with recommendations

## Dependencies

- Python 3.10+
- The absorbed notebook evidence tree in this repository

## House Style

- generated snapshots live in `reports/`
- `status` gives the fast project-level read
