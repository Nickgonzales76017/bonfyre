# Prediction Market Data Tool

Scrapes prediction market data, detects mispriced probabilities, and delivers alerts.

## Stack
- Python 3.10+
- httpx for API requests
- SQLite for market data storage
- Discord/Telegram bot for alert delivery

## Run

```bash
python3 scraper.py --help
python3 signals.py --help
```

## Structure
```
PredictionMarketDataTool/
├── scraper.py         # Market data collection
├── signals.py         # Mispricing detection + scoring
├── alerts.py          # Alert delivery (Discord/Telegram)
├── store.py           # SQLite persistence
└── README.md
```
