# IntelliWatch

Industrial scene understanding and safety monitoring. This is the Stage 1 monorepo foundation described in the supplied production specification: relational evidence and event model, a versioned FastAPI boundary, a separate observation worker, Redis realtime channels, S3-compatible evidence storage, and an industrial entry experience connected to the operational application.

## Architecture

- `frontend/`: React + TypeScript + Vite, Tailwind CSS with a shadcn-style shared primitive base, TanStack Query for API state, Zustand for small client UI state, and Framer Motion-ready styling. Landing and manufacturing pages use industrial imagery and editorial typography; `/app` is a dense operations console.
- `backend/`: FastAPI `/api/v1`, Pydantic input contracts, SQLAlchemy 2 models, Alembic baseline, Argon2 password hashes, short-lived role-bearing JWTs, incident lifecycle, audit records, Redis publication, and MinIO/S3 evidence upload and signed retrieval.
- `ai-worker/`: Redis Streams consumer for validated external perception results. It applies explainable zone intrusion, dwell, helmet/vest, fall-candidate, and machine-anomaly rules, then submits idempotent events. It never performs GPU inference in an HTTP request.
- `docker-compose.yml`: PostgreSQL, Redis, MinIO, bucket initialization, API, and worker for repeatable local development.

## Start locally

Requirements: Docker Desktop with Compose, Node.js 20+, Python 3.12+ (the container images use 3.12), and PowerShell or a POSIX shell.

1. Copy `.env.example` to `.env`. Change `JWT_SECRET` to a long random value. Keep real camera credentials in a secret manager; the camera record should contain a reference, not a credential-bearing RTSP URL.
2. Start infrastructure and services: `docker compose up --build`.
3. Create the first admin from another terminal: `docker compose exec api python -m app.cli create-user --email you@example.com --role admin`. Enter a password of at least 12 characters when prompted.
4. Generate a one-year worker token using the same `.env` secret: `docker compose run --rm --no-deps api python -m app.cli service-token`. Put the output in `AI_SERVICE_TOKEN` in `.env`, then restart with `docker compose up -d worker api`. Rotate this token yearly or after exposure.
5. Open `http://localhost:5173` after starting the frontend dev server. For local frontend development outside Compose, run `npm install` then `npm run dev` from the repository root. Set `VITE_API_URL` if the API is not at `http://localhost:8000`.
6. Sign in, open Configuration, create a factory and site, then register a real camera source. The UI only shows persisted API data; empty, unconfigured, unavailable, and no-playback states are explicit.

The API health endpoint is public at `GET /api/v1/health`; data routes require a bearer token. Viewer accounts can read; operator/admin accounts can mutate configuration and incident state. Event ingestion is restricted to the `ai_service` role. Create human users with the CLI; there is no public sign-up endpoint.

## Perception integration

Perception providers post a versioned JSON contract to `POST /api/v1/observations` with the AI service bearer token. The API stores detections, tracks, PPE, behaviour, and machine-state observations, then forwards the validated payload to the internal Redis stream `ai:observations`. Camera IDs and optional zone IDs must already exist. Camera-health samples use `POST /api/v1/cameras/{id}/health`; unhealthy conditions create incidents with reason codes. The worker records reason codes and model version in each generated event. A compatible GPU, selected/licensed model weights, camera calibration, and a DeepStream or model adapter are environment-specific integration work; this repository does not ship fabricated inference or claim GPU capability before it is configured.

Run a non-production Python worker locally with the repository `.venv`, after copying `.env.example` to `.env`, installing `backend/requirements.txt` and `ai-worker/requirements.txt`, and setting `API_URL`/`AI_SERVICE_TOKEN` for the local API. The Compose worker is the recommended route.

## Data and API

Core tables cover factory, site, camera, zone, tracked object, detection, track point, PPE and behaviour observations, machine state, scene relationships, event, incident, evidence, camera health, model version, audit log, and users. `backend/migrations/versions/0001_initial_core.py` is the initial metadata baseline. Subsequent schema changes should use explicit Alembic operations and must not modify a released migration.

Useful endpoints:

- `GET /api/v1/health`, `GET /api/v1/system/metrics`
- `POST/GET /api/v1/factories`, `POST/GET /api/v1/sites`
- `POST/GET /api/v1/cameras`, `GET /api/v1/cameras/{id}/events`
- `POST /api/v1/observations`, `GET /api/v1/cameras/{id}/scene`, `POST/GET /api/v1/cameras/{id}/health`
- `POST /api/v1/sites/{id}/floor-plan`, `GET /api/v1/sites/{id}/floor-plan/url`
- `POST/GET/PATCH /api/v1/zones`
- `POST /api/v1/events/ingest`, `GET /api/v1/events`
- `GET/PATCH /api/v1/incidents`, `GET /api/v1/incidents/{id}`
- `POST /api/v1/incidents/{id}/evidence`, signed retrieval via `/evidence/{evidence_id}/url`
- WebSockets: `/ws/events`, `/ws/incidents`, `/ws/cameras`, `/ws/scene/{camera_id}`

## Current Stage 1 boundary

This repository establishes the product core and real connected UI journey. Before an operational safety pilot, connect and qualify the actual camera/DeepStream or model adapter, calibration and zones, evidence clip/snapshot capture, GPU-specific models, facility data, deployment secrets, and production identity provider. The demo must be driven by a real camera or authorized recording plus genuine model outputs; no seed records are inserted by the application.
