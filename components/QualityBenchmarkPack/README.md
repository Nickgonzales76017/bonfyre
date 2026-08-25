# Quality Benchmark Pack

Curated set of transcripts with human quality ratings for calibrating scoring heuristics.

## Structure

```
QualityBenchmarkPack/
├── benchmarks/
│   └── <id>/
│       ├── audio.wav          # source audio (optional)
│       ├── transcript.txt     # raw transcript
│       ├── summary.md         # generated summary
│       └── rating.json        # human quality judgment
├── schema.json                # rating schema definition
├── evaluate.py                # compare heuristics vs human ratings
└── README.md
```

## Run

```bash
python3 evaluate.py benchmarks/
```
