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

### Stage 2: NVIDIA DeepStream capture-to-observation adapter

`ai-worker/deepstream_pipeline.py` is the opt-in video producer for a compatible NVIDIA DeepStream Linux host. It opens the configured camera or authorized recording with `nvurisrcbin`, batches sources with `nvstreammux`, runs the deployment's TensorRT model through `nvinfer`, assigns persistent-in-stream IDs through `nvtracker`, projects detections back into each source camera's calibrated image coordinates, and submits actual inference metadata to the existing `POST /api/v1/observations` contract. The API and UI are unchanged. The existing Redis observation stream, explainable rules worker, Postgres records, and realtime event channels remain downstream.

DeepStream must run on a supported NVIDIA GPU/Jetson system with its SDK, GStreamer plugins, and PyServiceMaker. PyServiceMaker is included with current DeepStream installations; NVIDIA recommends it over the deprecated `pyds` Python bindings. This repository does not bundle a camera recording, model weights, TensorRT engine, label map or tracker binary/config: PeopleNet and PPE models must be selected, licensed, configured and validated for the target camera conditions. The current development machine has no NVIDIA runtime tools or Docker, so this hardware pipeline cannot be started or qualified here. NVIDIA's [DeepStream installation guide](https://docs.nvidia.com/metropolis/deepstream/9.1/text/DS_Installation.html) lists supported platforms; the [PyServiceMaker quick start](https://docs.nvidia.com/metropolis/deepstream/9.1/text/DS_service_maker_python_quick_start.html) and [pipeline API guide](https://docs.nvidia.com/metropolis/deepstream/9.1/text/DS_service_maker_python_intro_to_pipeline_api.html) describe the application APIs used here.

After the API, database, Redis and rules worker are running, register real cameras and issue the AI service token as described above. On the DeepStream host, install `httpx` into the SDK's compatible Python environment and configure:

- `API_URL` and `AI_SERVICE_TOKEN` for the IntelliWatch API.
- `DEEPSTREAM_SOURCES_JSON` as a JSON array of `{ "camera_id", "uri", "model_version" }`. Each ID must already exist in IntelliWatch. Keep RTSP credentials in the host's secret manager or protected environment; never commit them.
- `DEEPSTREAM_PGIE_CONFIG` with the deployed `nvinfer` model configuration and `DEEPSTREAM_TRACKER_CONFIG` with the compatible `nvtracker` configuration. Tracker IDs are required; the adapter fails closed when either configuration is missing.
- `DEEPSTREAM_TRACKER_LIBRARY` with the installed DeepStream tracker shared library (commonly `libnvds_nvmultiobjecttracker.so`).
- Optionally, `DEEPSTREAM_OBSERVATION_INTERVAL_SECONDS` (default `1.0`) and `OBSERVATION_QUEUE_CAPACITY` (default `2048`) to set sampling and the bounded API handoff.

Run `python ai-worker/deepstream_pipeline.py` in the SDK runtime. It verifies the configured camera records and reads enabled zones from the API; zone polygons use the calibrated source-image coordinates collected by the existing configuration screen. It reports measured pipeline FPS, avoids logging source credentials, refreshes zone configuration periodically, and sends observations asynchronously so API latency does not block the metadata probe. Queue overflow and API delivery failures are logged; increase capacity or reduce the observation interval based on measured throughput. This adapter currently produces real detections, tracking IDs and calibrated zone occupancy/dwell events. It does not claim PPE, depth, pose/fall, re-identification, machine anomaly, scene-graph, event evidence clips or production-grade model-performance analytics until corresponding qualified models, calibration and metadata providers are configured. Qualify the actual detector and PPE models against facility footage and measure precision, recall, FPS and latency before operational safety use.

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

## Stage 2 operations

Stage 2 keeps camera-local tracker IDs separate from facility-level identity hypotheses. A re-identification provider can submit a proven match to `POST /api/v1/global-tracks/link`; the response and graph retain method, confidence, and provenance. Do not use that endpoint to guess identity matches.

Configure an image-to-floor-plane 3×3 homography for a camera with `PUT /api/v1/cameras/{camera_id}/calibration`. The matrix must map the camera's configured pixel dimensions to metres. The API projects the bottom-centre of each person bounding box into the floor plane when the provider has not supplied `world_position_m`. Coordinates remain unavailable when dimensions/calibration do not match. Spatial graph and trajectory endpoints return persisted observations only.

Event analysis, camera heatmaps, model/camera health, scene graphs, deterministic investigation queries, event correlation, and incident reports use persisted database records. Investigation queries state their supported filters and return linked incidents; exported HTML reports include the reason codes and lifecycle audit and can be printed to PDF in a browser.

To set up the explicitly labeled demo facility, run after the API migration has completed:

```powershell
docker compose exec api python -m app.cli seed-demo
```

This creates facility, site, camera, and zone configuration only. It does not create sample incidents or pretend detections. To exercise the normal Redis observation stream with synthetic data, set `DEMO_MODE=true` in the local `.env` and recreate the worker:

```powershell
docker compose up -d --build worker
```

Synthetic samples use `demo-simulator` model provenance and exist only while this explicit flag is enabled. Turn it off and recreate the worker to stop generation. The simulator requires the existing AI service token configuration; no token or user password is included here.

Stage 2 additive database changes are in Alembic revision `0002_stage2_global_identity`; the API image applies migrations during startup. NVIDIA DeepStream remains an opt-in provider and requires compatible NVIDIA hardware, drivers, and licensed model assets. The demo simulator validates downstream queue/rules/storage plumbing and is not a substitute for video inference.
