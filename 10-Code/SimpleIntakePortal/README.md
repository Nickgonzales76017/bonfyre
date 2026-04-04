# Simple Intake Portal

Customer-facing upload portal for the transcription workflow. Built on top of WebWorkerSaaS.

## Run

```bash
python3 -m http.server 8081
```

Open `http://localhost:8081`

## Notes
- Customer-facing subset of WebWorkerSaaS functionality
- Strips operator features (manifests, packages, advanced exports)
- Adds clear status communication for customers
