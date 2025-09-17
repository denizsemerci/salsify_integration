# SharePoint → Salsify Image Integration Service

A production-ready Python daemon that transfers images from SharePoint (via Microsoft Graph) to the Salsify PIM.

## Features
- MSAL Client Credentials auth to Microsoft Graph
- Polls SharePoint folder every N seconds (default 300)
- Streams file content to Salsify (no disk writes)
- Duplicate tracking via `data/processed_files.json`
- Robust retries, exponential backoff, circuit breaker
- Prometheus metrics and `/health` endpoint
- Dockerized, non-root runtime

## Quick Start
1. Copy `.env.example` to `.env` and fill values
2. Build and run:
```bash
docker compose up --build
```

## Configuration
- `config/settings.yaml` and environment variables control runtime

## Development
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.main
```

## Testing
```bash
pytest -q
```

## License
MIT
