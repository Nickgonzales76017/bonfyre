# BonfyreFPQ Remote Pipeline

Compress models at scale on RunPod GPUs. Results push to GitHub so nothing is lost if a server dies.

## Quick Start

```bash
# 1. Fill in your tokens
vim pipeline/.env
#    RUNPOD_API_KEY=rpa_xxx    (already set)
#    GITHUB_TOKEN=ghp_xxx      (needed for pushing weights)
#    HF_TOKEN=hf_xxx           (needed for gated models)

# 2. Launch everything (H200 + all 5 GLM models)
cd pipeline && bash go.sh

# 3. Or do it step by step:
python3 deploy.py launch --gpu H200        # spin up pod
python3 deploy.py list                     # check status
python3 deploy.py ssh <pod_id>             # get SSH command
# SSH in, run pod_setup.sh, then worker.sh
```

## Architecture

```
Local (your Mac)                    RunPod Pod (H200/RTX)
─────────────────                   ─────────────────────
pipeline/
├── .env              ← creds      /workspace/
├── deploy.py         ← API mgr    ├── bonfyre/          (cloned repo)
├── go.sh             ← one-click  ├── jobs/
├── pod_setup.sh      ← init       │   ├── pending/      ← job queue
├── worker.sh         ← worker     │   ├── running/      ← in progress
└── jobs/                           │   ├── done/         ← complete
    └── pending/                    │   └── failed/       ← errors
        ├── 01_glm-4-9b.json       ├── models/
        ├── 02_glm-4-9b-chat.json  │   ├── original/     ← downloaded
        ├── ...                     │   └── fpq/          ← compressed
                                    └── logs/             ← worker logs
```

## Commands

| Command | What it does |
|---------|-------------|
| `python3 deploy.py launch --gpu H200` | Spin up a GPU pod |
| `python3 deploy.py launch --gpu RTX6000PRO` | Cheaper 96GB option |
| `python3 deploy.py list` | Show all running pods |
| `python3 deploy.py ssh <id>` | Get SSH connection info |
| `python3 deploy.py stop <id>` | Stop a specific pod |
| `python3 deploy.py stop-all` | Kill everything |
| `python3 deploy.py submit THUDM/glm-4-9b` | Create a job manifest |

## Worker Modes

```bash
# Queue mode (default) — processes jobs from /workspace/jobs/pending/
bash worker.sh

# Single job mode
bash worker.sh --once

# Direct model (skip queue)
bash worker.sh --model THUDM/glm-4-9b --bits 3
```

## GLM Models Queued

| Priority | Model | Size | Notes |
|----------|-------|------|-------|
| 1 | THUDM/glm-4-9b | ~18GB | Base model, compress first |
| 2 | THUDM/glm-4-9b-chat | ~18GB | Chat variant |
| 3 | THUDM/glm-z1-9b-0414 | ~18GB | Reasoning model |
| 4 | THUDM/glm-4-32b-0414 | ~64GB | Big model, needs H200 |
| 5 | THUDM/glm-z1-32b-0414 | ~64GB | Big reasoning model |

## GPU Recommendations

- **9B models**: RTX PRO 6000 ($1.69/hr, 96GB VRAM) — can run 2-3 in parallel
- **32B models**: H200 ($3.59/hr, 141GB VRAM) — needs the headroom
- Run RTX PRO 6000 for the 9B batch, H200 only for the 32B pair

## Where Results Go

1. **GitHub Releases**: Weight files uploaded as release assets to `bonfyre-fpq-models` repo
2. **Git branches**: Metadata/config/README pushed to `models/<name>` branches
3. **Pod local**: Everything stays on `/workspace/models/fpq/` until pod is stopped

## Cost Estimate

| Model batch | GPU | Est. time | Est. cost |
|------------|-----|-----------|-----------|
| 3x 9B GLM | RTX PRO 6000 | ~2-3 hrs | ~$3-5 |
| 2x 32B GLM | H200 | ~3-4 hrs | ~$11-14 |
| **Total** | | | **~$14-19** |
