# Deliverable Formatter Engine

Turns transcripts and summaries into buyer-ready markdown, PDF, or email-friendly packages.

## Run

```bash
python3 formatter.py job-output/ --template executive-summary
python3 formatter.py job-output/ --format pdf
```

## Requires
- Python 3.10+
- Optional: weasyprint or markdown-pdf for PDF export
