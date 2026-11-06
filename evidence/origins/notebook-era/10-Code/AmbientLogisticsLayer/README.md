# Ambient Logistics Layer

Local logistics coordination tool — package holding, errand batching, and micro-delivery routing.

## Stack
- Python 3.10+
- SQLite for task/request tracking
- Simple CLI + optional web form

## Run

```bash
python3 coordinator.py --help
```

## Structure
```
AmbientLogisticsLayer/
├── coordinator.py     # CLI entrypoint
├── models.py          # Task, Request, Node data models
├── router.py          # Batch routing logic
├── store.py           # SQLite persistence
└── README.md
```
