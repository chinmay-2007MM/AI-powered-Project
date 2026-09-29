import asyncio
import json
import logging
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from io import BytesIO
from uuid import uuid4
import boto3
from botocore.exceptions import ClientError
from fastapi import Depends, FastAPI, File, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from redis import Redis
from sqlalchemy import func, text
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr, Field
from .config import settings
from .db import get_db
from .models import Camera, Zone, Event, Incident, Evidence, AuditLog, Factory, Site, User, TrackedObject, GlobalTrack, GlobalTrackMembership, Detection, ModelVersion, PPEObservation, TrackPoint, BehaviourObservation, MachineState, CameraHealth, SceneRelationship
from .schemas import CameraCreate, CameraRead, CameraUpdate, CameraCalibration, CameraHealthIn, ZoneCreate, ZoneRead, ZoneUpdate, EventIngest, IncidentStatusUpdate, IncidentRead, FactoryCreate, SiteCreate, SiteRead, AIResult
from .services import create_event, ensure_camera, publish
from .auth import decode_token, make_token, verify_password
from .spatial import project_to_floor

class JsonLogFormatter(logging.Formatter):
    def format(self, record):
        payload = {"timestamp": datetime.now(timezone.utc).isoformat(), "level": record.levelname, "logger": record.name, "message": record.getMessage()}
        if record.exc_info: payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)

handler = logging.StreamHandler()
handler.setFormatter(JsonLogFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
log = logging.getLogger("intelliwatch")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(title="IntelliWatch API", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def authenticate_requests(request, call_next):
    path = request.url.path
    if path in {"/api/v1/health", "/api/v1/auth/token", "/docs", "/openapi.json", "/redoc"} or not path.startswith("/api/v1/"):
        return await call_next(request)
    authorization = request.headers.get("authorization", "")
    if not authorization.startswith("Bearer "):
        from starlette.responses import JSONResponse
        return JSONResponse({"detail": "Authentication required"}, status_code=401)
    try: claims = decode_token(authorization[7:])
    except HTTPException as exc:
        from starlette.responses import JSONResponse
        return JSONResponse({"detail": exc.detail}, status_code=401)
    role = claims.get("role")
    method = request.method.upper()
    if method in {"POST", "PUT", "PATCH", "DELETE"}:
        ai_path = path in {"/api/v1/events/ingest", "/api/v1/observations", "/api/v1/global-tracks/link"} or bool(re.fullmatch(r"/api/v1/cameras/[^/]+/health", path))
        query_path = path == "/api/v1/investigations/query"
        allowed = role == "ai_service" if ai_path else role in {"viewer", "operator", "admin"} if query_path else role in {"operator", "admin"}
        if not allowed:
            from starlette.responses import JSONResponse
            return JSONResponse({"detail": "Insufficient role"}, status_code=403)
    request.state.actor = claims.get("sub", "unknown")
    return await call_next(request)


# Register CORS after the custom auth middleware so it wraps it. This lets
# preflights through and adds CORS headers to authentication error responses.
app.add_middleware(CORSMiddleware, allow_origins=[v.strip() for v in settings.cors_origins.split(",")], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1)


@app.post("/api/v1/auth/token")
def login(body: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter_by(email=body.email.lower(), active=True).first()
    if not user or not verify_password(body.password, user.password_hash): raise HTTPException(401, "Incorrect email or password")
    return {"access_token": make_token(user.email, user.role), "token_type": "bearer", "expires_in": 2700, "role": user.role, "email": user.email}


def current_actor(request: Request) -> str:
    return getattr(request.state, "actor", "unknown")


def incident_read(db: Session, incident: Incident) -> IncidentRead:
    event = db.get(Event, incident.event_id)
    return IncidentRead(id=incident.id, event_id=event.id, status=incident.status, assigned_to=incident.assigned_to, opened_at=incident.opened_at, resolved_at=incident.resolved_at, event_type=event.event_type, severity=event.severity, confidence=event.confidence, timestamp=event.timestamp, camera_id=event.camera_id, object_ids=event.object_ids, reason_codes=event.reason_codes, evidence_ref=event.evidence_ref)


@app.get("/api/v1/health")
def health(db: Session = Depends(get_db)):
    checks = {"database": "ok", "redis": "unknown", "object_storage": "unknown", "ai_worker": "not_reporting"}
    try: db.execute(text("SELECT 1"))
    except Exception as exc: checks["database"] = "unavailable"; log.warning("database health failure: %s", exc)
    try:
        redis_client = Redis.from_url(settings.redis_url, socket_connect_timeout=1, decode_responses=True)
        redis_client.ping(); checks["redis"] = "ok"
        checks["ai_worker"] = "active" if redis_client.get("ai:worker:heartbeat") else "not_reporting"
        redis_client.close()
    except Exception: checks["redis"] = "unavailable"
    try:
        boto3.client("s3", endpoint_url=settings.s3_endpoint, aws_access_key_id=settings.s3_access_key, aws_secret_access_key=settings.s3_secret_key).head_bucket(Bucket=settings.s3_bucket)
        checks["object_storage"] = "ok"
    except Exception: checks["object_storage"] = "unavailable"
    return {"status": "ok" if all(checks[k] == "ok" for k in ("database", "redis", "object_storage")) else "degraded", "timestamp": datetime.now(timezone.utc), "components": checks}


@app.get("/api/v1/system/metrics")
def metrics(db: Session = Depends(get_db)):
    cameras_total = db.query(func.count(Camera.id)).scalar() or 0
    online = db.query(func.count(Camera.id)).filter(Camera.status == "online").scalar() or 0
    active_incidents = db.query(func.count(Incident.id)).filter(Incident.status != "RESOLVED").scalar() or 0
    events_today = db.query(func.count(Event.id)).filter(func.date(Event.timestamp) == datetime.now(timezone.utc).date()).scalar() or 0
    return {"cameras_total": cameras_total, "cameras_online": online, "active_incidents": active_incidents, "events_today": events_today}


@app.get("/api/v1/factories")
def list_factories(db: Session = Depends(get_db)):
    return db.query(Factory).order_by(Factory.name).all()


@app.post("/api/v1/factories", status_code=201)
def create_factory(body: FactoryCreate, request: Request, db: Session = Depends(get_db)):
    factory = Factory(id=body.id or str(uuid4()), **body.model_dump(exclude={"id"}))
    db.add(factory); db.add(AuditLog(id=str(uuid4()), actor=current_actor(request), action="factory.created", entity="factory", entity_id=factory.id, metadata_json={}))
    db.commit(); db.refresh(factory); return factory


@app.get("/api/v1/sites", response_model=list[SiteRead])
def list_sites(factory_id: str | None = None, db: Session = Depends(get_db)):
    q = db.query(Site)
    if factory_id: q = q.filter(Site.factory_id == factory_id)
    return q.order_by(Site.name).all()


@app.post("/api/v1/sites", response_model=SiteRead, status_code=201)
def create_site(body: SiteCreate, request: Request, db: Session = Depends(get_db)):
    if not db.get(Factory, body.factory_id): raise HTTPException(404, "Factory not found")
    site = Site(id=body.id or str(uuid4()), **body.model_dump(exclude={"id"}))
    db.add(site); db.add(AuditLog(id=str(uuid4()), actor=current_actor(request), action="site.created", entity="site", entity_id=site.id, metadata_json={}))
    db.commit(); db.refresh(site); return site


@app.post("/api/v1/sites/{site_id}/floor-plan", status_code=201)
async def upload_floor_plan(site_id: str, request: Request, file: UploadFile = File(...), db: Session = Depends(get_db)):
    site = db.get(Site, site_id)
    if not site: raise HTTPException(404, "Site not found")
    media_types = {"image/jpeg":"jpg", "image/png":"png", "image/webp":"webp"}
    if file.content_type not in media_types: raise HTTPException(415, "Floor plan must be JPEG, PNG, or WebP")
    payload = await file.read(25 * 1024 * 1024 + 1)
    if len(payload) > 25 * 1024 * 1024: raise HTTPException(413, "Floor plan exceeds 25 MB")
    key = f"sites/{site_id}/floor-plans/{uuid4()}.{media_types[file.content_type]}"
    client = boto3.client("s3", endpoint_url=settings.s3_endpoint, aws_access_key_id=settings.s3_access_key, aws_secret_access_key=settings.s3_secret_key)
    try: client.put_object(Bucket=settings.s3_bucket, Key=key, Body=BytesIO(payload), ContentType=file.content_type)
    except Exception as exc: raise HTTPException(503, "Floor plan storage is unavailable") from exc
    site.floor_plan_ref = key
    db.add(AuditLog(id=str(uuid4()), actor=current_actor(request), action="site.floor_plan_uploaded", entity="site", entity_id=site.id, metadata_json={"object_key": key}))
    db.commit()
    return {"site_id": site.id, "floor_plan_ref": key}


@app.get("/api/v1/sites/{site_id}/floor-plan/url")
def floor_plan_url(site_id: str, db: Session = Depends(get_db)):
    site = db.get(Site, site_id)
    if not site: raise HTTPException(404, "Site not found")
    if not site.floor_plan_ref: raise HTTPException(404, "No floor plan is configured")
    client = boto3.client("s3", endpoint_url=settings.s3_public_endpoint, aws_access_key_id=settings.s3_access_key, aws_secret_access_key=settings.s3_secret_key)
    try: url = client.generate_presigned_url("get_object", Params={"Bucket": settings.s3_bucket, "Key": site.floor_plan_ref}, ExpiresIn=300)
    except Exception as exc: raise HTTPException(503, "Floor plan storage is unavailable") from exc
    return {"url": url, "expires_in": 300}


@app.get("/api/v1/cameras", response_model=list[CameraRead])
def list_cameras(site_id: str | None = None, db: Session = Depends(get_db)):
    q = db.query(Camera)
    if site_id: q = q.filter(Camera.site_id == site_id)
    return q.order_by(Camera.name).all()


@app.post("/api/v1/cameras", response_model=CameraRead, status_code=201)
def add_camera(body: CameraCreate, request: Request, db: Session = Depends(get_db)):
    camera = Camera(id=body.id or str(uuid4()), **body.model_dump(exclude={"id"}))
    if not db.get(Site, camera.site_id): raise HTTPException(404, "Site not found")
    db.add(camera); db.add(AuditLog(id=str(uuid4()), actor=current_actor(request), action="camera.created", entity="camera", entity_id=camera.id, metadata_json={}))
    db.commit(); db.refresh(camera); publish("cameras", {"camera_id": camera.id, "status": camera.status})
    return camera


@app.get("/api/v1/cameras/{camera_id}", response_model=CameraRead)
def camera_detail(camera_id: str, db: Session = Depends(get_db)):
    return ensure_camera(db, camera_id)


@app.get("/api/v1/cameras/{camera_id}/calibration")
def camera_calibration(camera_id: str, db: Session = Depends(get_db)):
    camera = ensure_camera(db, camera_id)
    return {"camera_id": camera.id, "calibrated": bool(camera.calibration), "calibration": camera.calibration}


@app.put("/api/v1/cameras/{camera_id}/calibration")
def set_camera_calibration(camera_id: str, body: CameraCalibration, request: Request, db: Session = Depends(get_db)):
    camera = ensure_camera(db, camera_id)
    if camera.resolution:
        match = re.fullmatch(r"\s*(\d+)\s*[xX]\s*(\d+)\s*", camera.resolution)
        if match and (int(match.group(1)), int(match.group(2))) != (body.reference_width, body.reference_height):
            raise HTTPException(422, "Calibration reference size must match the camera's configured resolution")
    camera.calibration = body.model_dump()
    db.add(AuditLog(id=str(uuid4()), actor=current_actor(request), action="camera.calibration_updated", entity="camera", entity_id=camera.id, metadata_json={"reference_width": body.reference_width, "reference_height": body.reference_height, "units": body.units, "ground_reference": body.ground_reference}))
    db.commit()
    return {"camera_id": camera.id, "calibrated": True, "calibration": camera.calibration}


@app.post("/api/v1/cameras/{camera_id}/health")
def ingest_camera_health(camera_id: str, body: CameraHealthIn, db: Session = Depends(get_db)):
    camera = ensure_camera(db, camera_id)
    health = body.health_state
    camera.status = "online" if health == "online" else "degraded" if health == "degraded" else health
    record = CameraHealth(id=str(uuid4()), camera_id=camera.id, timestamp=body.timestamp, fps=body.fps, latency_ms=body.latency_ms, dropped_frames=body.dropped_frames, health_state=health)
    db.add(record)
    unhealthy = {"offline", "frozen", "stale", "black", "blurred"}
    if health in unhealthy:
        bucket = int(body.timestamp.timestamp()) // 600
        create_event(db, {"idempotency_key": f"camera-health:{camera.id}:{health}:{bucket}", "event_type": "camera_health_condition", "timestamp": body.timestamp, "severity": "high" if health in {"offline", "frozen", "black"} else "medium", "confidence": 1.0, "source": "camera-health-v1", "camera_id": camera.id, "object_ids": [], "zone_id": None, "reason_codes": ["camera_" + health], "evidence_ref": None, "payload": {"fps": body.fps, "latency_ms": body.latency_ms, "dropped_frames": body.dropped_frames}})
        db.commit()
    else:
        db.commit()
    publish("cameras", {"camera_id": camera.id, "status": camera.status, "health_state": health})
    return {"camera_id": camera.id, "camera_status": camera.status, "health_state": health, "timestamp": body.timestamp}


@app.get("/api/v1/cameras/{camera_id}/health")
def camera_health_history(camera_id: str, limit: int = Query(100, ge=1, le=500), db: Session = Depends(get_db)):
    ensure_camera(db, camera_id)
    return db.query(CameraHealth).filter_by(camera_id=camera_id).order_by(CameraHealth.timestamp.desc()).limit(limit).all()


@app.patch("/api/v1/cameras/{camera_id}", response_model=CameraRead)
def update_camera(camera_id: str, body: CameraUpdate, request: Request, db: Session = Depends(get_db)):
    camera = ensure_camera(db, camera_id)
    changes = body.model_dump(exclude_unset=True)
    for key, value in changes.items(): setattr(camera, key, value)
    db.add(AuditLog(id=str(uuid4()), actor=current_actor(request), action="camera.updated", entity="camera", entity_id=camera.id, metadata_json={"fields": list(changes)}))
    db.commit(); db.refresh(camera); return camera


@app.delete("/api/v1/cameras/{camera_id}")
def retire_camera(camera_id: str, request: Request, db: Session = Depends(get_db)):
    camera = ensure_camera(db, camera_id)
    camera.status = "retired"
    db.add(AuditLog(id=str(uuid4()), actor=current_actor(request), action="camera.retired", entity="camera", entity_id=camera.id, metadata_json={}))
    db.commit()
    return {"id": camera.id, "status": camera.status}


@app.get("/api/v1/cameras/{camera_id}/events")
def camera_events(camera_id: str, limit: int = Query(100, ge=1, le=500), db: Session = Depends(get_db)):
    ensure_camera(db, camera_id)
    return db.query(Event).filter_by(camera_id=camera_id).order_by(Event.timestamp.desc()).limit(limit).all()


@app.get("/api/v1/cameras/{camera_id}/scene")
def camera_scene(camera_id: str, limit: int = Query(100, ge=1, le=500), db: Session = Depends(get_db)):
    ensure_camera(db, camera_id)
    rows = db.query(Detection, TrackedObject).outerjoin(TrackedObject, Detection.object_id == TrackedObject.id).filter(Detection.camera_id == camera_id).order_by(Detection.frame_ts.desc()).limit(limit).all()
    observations = []
    for detection, tracked in rows:
        ppe = db.query(PPEObservation).filter_by(object_id=detection.object_id).order_by(PPEObservation.timestamp.desc()).first() if detection.object_id else None
        observations.append({"id": detection.id, "timestamp": detection.frame_ts, "object_id": detection.object_id, "class": detection.object_class, "confidence": detection.confidence, "bbox": detection.bbox, "first_seen": tracked.first_seen if tracked else None, "last_seen": tracked.last_seen if tracked else None, "ppe": {"helmet": ppe.helmet, "vest": ppe.vest, "gloves": ppe.gloves, "goggles": ppe.goggles, "confidences": ppe.confidences} if ppe else None})
    relations = db.query(SceneRelationship).filter(SceneRelationship.camera_id == camera_id).order_by(SceneRelationship.timestamp.desc()).limit(limit).all()
    return {"camera_id": camera_id, "observations": observations, "relationships": [{"subject_id": r.subject_id, "relation": r.relation, "object_id": r.object_id, "timestamp": r.timestamp, "confidence": r.confidence} for r in relations]}


@app.post("/api/v1/observations", status_code=202)
def ingest_observation(body: AIResult, db: Session = Depends(get_db)):
    camera = ensure_camera(db, body.camera_id)
    machine_baseline = None
    derived_anomaly_score = body.anomaly_score
    if body.machine_id and body.machine_state:
        history = db.query(MachineState).filter(MachineState.machine_id == body.machine_id, MachineState.timestamp < body.timestamp).order_by(MachineState.timestamp.desc()).limit(200).all()
        if len(history) >= 20:
            state_count = sum(1 for item in history if item.state == body.machine_state)
            ratio = state_count / len(history)
            rarity = 1.0 - ratio
            derived_anomaly_score = max(derived_anomaly_score or 0.0, rarity)
            machine_baseline = {"method": "rolling_state_frequency_v1", "sample_count": len(history), "state_count": state_count, "state_frequency": round(ratio, 4), "history_limit": 200}
    zone = db.get(Zone, body.zone_id) if body.zone_id else None
    if body.zone_id and not zone: raise HTTPException(404, "Zone not found")
    if zone and (zone.site_id != camera.site_id or (zone.camera_id and zone.camera_id != camera.id)):
        raise HTTPException(422, "Observation zone must belong to its camera's site")
    existing = db.query(Detection).filter_by(observation_key=body.observation_key).first()
    if not existing:
        track = db.get(TrackedObject, body.object_id) if body.object_id else None
        if body.object_id and not track:
            track = TrackedObject(id=body.object_id, camera_id=camera.id, object_class=body.object_class, first_seen=body.timestamp, last_seen=body.timestamp)
            db.add(track)
        elif track:
            track.last_seen = max(track.last_seen, body.timestamp)
        model_name, separator, model_version = body.model_version.rpartition("-")
        version = model_version if separator else "unspecified"
        model_name = model_name if separator else body.model_version
        model = db.query(ModelVersion).filter_by(model=model_name, version=version).first()
        if not model:
            model = ModelVersion(id=str(uuid4()), model=model_name, version=version, framework="external-provider", config={})
            db.add(model); db.flush()
        detection = Detection(id=str(uuid4()), observation_key=body.observation_key, camera_id=camera.id, object_id=body.object_id, model_version_id=model.id, frame_ts=body.timestamp, object_class=body.object_class, confidence=body.confidence, bbox=list(body.bbox))
        db.add(detection)
        if body.object_id:
            if body.ppe:
                db.add(PPEObservation(id=str(uuid4()), object_id=body.object_id, timestamp=body.timestamp, helmet=body.ppe.get("helmet"), vest=body.ppe.get("vest"), gloves=body.ppe.get("gloves"), goggles=body.ppe.get("goggles"), confidences=body.ppe_confidences))
            center_x, center_y = (body.bbox[0] + body.bbox[2]) / 2, (body.bbox[1] + body.bbox[3]) / 2
            world = body.world_position_m
            if world is None and camera.calibration:
                width = body.image_width or camera.calibration.get("reference_width")
                height = body.image_height or camera.calibration.get("reference_height")
                same_frame = width == camera.calibration.get("reference_width") and height == camera.calibration.get("reference_height")
                in_frame = 0 <= body.bbox[0] < body.bbox[2] <= width and 0 <= body.bbox[1] < body.bbox[3] <= height
                ground = project_to_floor(center_x, body.bbox[3], camera.calibration) if same_frame and in_frame else None
                if ground is not None: world = (ground[0], ground[1], 0.0)
            db.add(TrackPoint(id=str(uuid4()), object_id=body.object_id, timestamp=body.timestamp, x=center_x, y=center_y, depth_m=body.depth_m, speed_mps=body.speed_mps, direction_deg=body.direction_deg, world_x_m=world[0] if world else None, world_y_m=world[1] if world else None, world_z_m=world[2] if world else None))
            if body.posture:
                db.add(BehaviourObservation(id=str(uuid4()), object_id=body.object_id, camera_id=camera.id, timestamp=body.timestamp, behaviour_type=body.posture, confidence=body.confidence, evidence_ref=body.evidence_ref))
        if body.machine_id and body.machine_state:
            db.add(MachineState(id=str(uuid4()), camera_id=camera.id, machine_id=body.machine_id, timestamp=body.timestamp, state=body.machine_state, confidence=body.confidence, anomaly_score=derived_anomaly_score))
        if body.object_id:
            for relation in body.relationships:
                db.add(SceneRelationship(id=str(uuid4()), subject_id=body.object_id, relation=relation.relation, object_id=relation.related_object_id, camera_id=camera.id, timestamp=body.timestamp, confidence=relation.confidence))
        db.commit()
    else:
        db.commit()
    payload = body.model_dump(by_alias=True, mode="json")
    if machine_baseline:
        payload["machine_baseline"] = machine_baseline
        payload["anomaly_score"] = derived_anomaly_score
    payload["center"] = [(body.bbox[0] + body.bbox[2]) / 2, (body.bbox[1] + body.bbox[3]) / 2]
    if zone:
        payload["zone_polygon"] = zone.polygon
        payload["zone_type"] = zone.zone_type
        payload["dwell_threshold_seconds"] = zone.dwell_threshold_seconds
    try:
        Redis.from_url(settings.redis_url, socket_connect_timeout=1).xadd("ai:observations", {"payload": json.dumps(payload)})
    except Exception as exc:
        log.exception("observation persisted but worker enqueue failed")
        raise HTTPException(503, "Observation persisted; AI rules queue is unavailable. Retry with the same observation_key.") from exc
    return {"accepted": True, "duplicate": existing is not None, "queued": True}


@app.get("/api/v1/zones", response_model=list[ZoneRead])
def list_zones(site_id: str | None = None, db: Session = Depends(get_db)):
    q = db.query(Zone)
    if site_id: q = q.filter(Zone.site_id == site_id)
    return q.order_by(Zone.name).all()


@app.post("/api/v1/zones", response_model=ZoneRead, status_code=201)
def add_zone(body: ZoneCreate, request: Request, db: Session = Depends(get_db)):
    site = db.get(Site, body.site_id)
    if not site: raise HTTPException(404, "Site not found")
    if body.camera_id:
        camera = ensure_camera(db, body.camera_id)
        if camera.site_id != body.site_id: raise HTTPException(422, "Zone camera must belong to the selected site")
    zone = Zone(id=body.id or str(uuid4()), **body.model_dump(exclude={"id"}))
    db.add(zone); db.add(AuditLog(id=str(uuid4()), actor=current_actor(request), action="zone.created", entity="zone", entity_id=zone.id, metadata_json={}))
    db.commit(); db.refresh(zone); return zone


@app.patch("/api/v1/zones/{zone_id}", response_model=ZoneRead)
def edit_zone(zone_id: str, body: ZoneUpdate, request: Request, db: Session = Depends(get_db)):
    zone = db.get(Zone, zone_id)
    if not zone: raise HTTPException(404, "Zone not found")
    changes = body.model_dump(exclude_unset=True)
    if changes.get("camera_id"):
        camera = ensure_camera(db, changes["camera_id"])
        if camera.site_id != zone.site_id: raise HTTPException(422, "Zone camera must belong to the selected site")
    for key, value in changes.items(): setattr(zone, key, value)
    db.add(AuditLog(id=str(uuid4()), actor=current_actor(request), action="zone.updated", entity="zone", entity_id=zone.id, metadata_json={"fields": list(changes)}))
    db.commit(); db.refresh(zone); return zone


@app.get("/api/v1/events")
def list_events(limit: int = Query(100, ge=1, le=500), db: Session = Depends(get_db)):
    return db.query(Event).order_by(Event.timestamp.desc()).limit(limit).all()


@app.post("/api/v1/events/ingest", status_code=202)
def ingest_event(body: EventIngest, db: Session = Depends(get_db)):
    event, incident, created = create_event(db, body.model_dump())
    return {"event_id": event.id, "incident_id": incident.id if incident else None, "created": created}


@app.get("/api/v1/incidents", response_model=list[IncidentRead])
def list_incidents(status: str | None = None, limit: int = Query(100, ge=1, le=500), db: Session = Depends(get_db)):
    q = db.query(Incident)
    if status: q = q.filter(Incident.status == status)
    return [incident_read(db, i) for i in q.order_by(Incident.opened_at.desc()).limit(limit).all()]


@app.get("/api/v1/incidents/{incident_id}")
def get_incident(incident_id: str, db: Session = Depends(get_db)):
    incident = db.get(Incident, incident_id)
    if not incident: raise HTTPException(404, "Incident not found")
    event = db.get(Event, incident.event_id)
    evidence = db.query(Evidence).filter_by(incident_id=incident.id).all()
    return {"incident": incident_read(db, incident), "event": event, "evidence": evidence}


@app.post("/api/v1/incidents/{incident_id}/evidence", status_code=201)
async def upload_evidence(incident_id: str, request: Request, file: UploadFile = File(...), db: Session = Depends(get_db)):
    incident = db.get(Incident, incident_id)
    if not incident: raise HTTPException(404, "Incident not found")
    allowed = {"image/jpeg":"jpg", "image/png":"png", "image/webp":"webp", "video/mp4":"mp4", "video/webm":"webm"}
    if file.content_type not in allowed: raise HTTPException(415, "Evidence must be JPEG, PNG, WebP, MP4, or WebM")
    payload = await file.read(50 * 1024 * 1024 + 1)
    if len(payload) > 50 * 1024 * 1024: raise HTTPException(413, "Evidence file exceeds 50 MB")
    if not payload: raise HTTPException(400, "Evidence file is empty")
    evidence_id = str(uuid4())
    key = f"incidents/{incident_id}/{evidence_id}.{allowed[file.content_type]}"
    client = boto3.client("s3", endpoint_url=settings.s3_endpoint, aws_access_key_id=settings.s3_access_key, aws_secret_access_key=settings.s3_secret_key)
    try: client.put_object(Bucket=settings.s3_bucket, Key=key, Body=BytesIO(payload), ContentType=file.content_type)
    except Exception as exc:
        log.exception("evidence storage failed")
        raise HTTPException(503, "Evidence storage is unavailable") from exc
    image = file.content_type.startswith("image/")
    record = Evidence(id=evidence_id, incident_id=incident_id, snapshot=key if image else None, event_clip=key if not image else None, captured_at=datetime.now(timezone.utc))
    db.add(record)
    db.add(AuditLog(id=str(uuid4()), actor=current_actor(request), action="evidence.uploaded", entity="incident", entity_id=incident_id, metadata_json={"evidence_id": evidence_id, "content_type": file.content_type}))
    db.commit()
    return {"id": evidence_id, "incident_id": incident_id, "object_key": key, "content_type": file.content_type, "captured_at": record.captured_at}


@app.get("/api/v1/incidents/{incident_id}/evidence/{evidence_id}/url")
def evidence_download_url(incident_id: str, evidence_id: str, db: Session = Depends(get_db)):
    record = db.query(Evidence).filter_by(id=evidence_id, incident_id=incident_id).first()
    if not record: raise HTTPException(404, "Evidence not found")
    key = record.snapshot or record.event_clip or record.before_clip or record.after_clip
    if not key: raise HTTPException(404, "Evidence object is not attached")
    client = boto3.client("s3", endpoint_url=settings.s3_public_endpoint, aws_access_key_id=settings.s3_access_key, aws_secret_access_key=settings.s3_secret_key)
    try: url = client.generate_presigned_url("get_object", Params={"Bucket": settings.s3_bucket, "Key": key}, ExpiresIn=300)
    except Exception as exc: raise HTTPException(503, "Evidence storage is unavailable") from exc
    return {"url": url, "expires_in": 300}


TRANSITIONS = {"DETECTED": {"ACKNOWLEDGED"}, "OPEN": {"ACKNOWLEDGED"}, "ACKNOWLEDGED": {"INVESTIGATING", "RESOLVED"}, "INVESTIGATING": {"RESOLVED"}, "RESOLVED": set()}


@app.patch("/api/v1/incidents/{incident_id}")
def update_incident(incident_id: str, body: IncidentStatusUpdate, request: Request, db: Session = Depends(get_db)):
    incident = db.get(Incident, incident_id)
    if not incident: raise HTTPException(404, "Incident not found")
    if body.status not in TRANSITIONS.get(incident.status, set()): raise HTTPException(409, f"Invalid transition: {incident.status} → {body.status}")
    previous = incident.status; incident.status = body.status
    if body.assigned_to is not None: incident.assigned_to = body.assigned_to
    if body.status == "RESOLVED": incident.resolved_at = datetime.now(timezone.utc)
    db.add(AuditLog(id=str(uuid4()), actor=current_actor(request), action="incident.status_changed", entity="incident", entity_id=incident.id, metadata_json={"from": previous, "to": body.status}))
    db.commit(); db.refresh(incident)
    publish("incidents", {"incident_id": incident.id, "status": incident.status, "event_id": incident.event_id})
    return incident_read(db, incident)


class GlobalTrackLink(BaseModel):
    global_track_id: str = Field(min_length=1, max_length=100)
    local_track_id: str = Field(min_length=1, max_length=100)
    confidence: float = Field(gt=0, le=1)
    method: str = Field(min_length=1, max_length=80)
    provenance: str = Field(min_length=1, max_length=80)


class InvestigationQuery(BaseModel):
    question: str = Field(min_length=1, max_length=500)
    camera_id: str | None = Field(default=None, max_length=36)


@app.post("/api/v1/global-tracks/link", status_code=201)
def link_global_track(body: GlobalTrackLink, request: Request, db: Session = Depends(get_db)):
    """Persist an identity link supplied by a configured re-identification provider."""
    local = db.get(TrackedObject, body.local_track_id)
    if not local:
        raise HTTPException(404, "Camera-local track not found")
    camera = ensure_camera(db, local.camera_id)
    global_track = db.get(GlobalTrack, body.global_track_id)
    if global_track and global_track.site_id != camera.site_id:
        raise HTTPException(422, "Global track belongs to a different site")
    if not global_track:
        global_track = GlobalTrack(id=body.global_track_id, site_id=camera.site_id, object_class=local.object_class, last_seen=local.last_seen)
        db.add(global_track)
    membership = db.query(GlobalTrackMembership).filter_by(local_track_id=local.id).first()
    if membership and membership.global_track_id != global_track.id:
        raise HTTPException(409, "Camera-local track is already linked to another global identity")
    if not membership:
        membership = GlobalTrackMembership(id=str(uuid4()), global_track_id=global_track.id, local_track_id=local.id, camera_id=camera.id, matched_at=local.last_seen, confidence=body.confidence, method=body.method, provenance=body.provenance)
        db.add(membership)
    global_track.last_seen = max(global_track.last_seen, local.last_seen)
    db.add(AuditLog(id=str(uuid4()), actor=current_actor(request), action="global_track.linked", entity="global_track", entity_id=global_track.id, metadata_json={"local_track_id": local.id, "camera_id": camera.id, "method": body.method, "confidence": body.confidence, "provenance": body.provenance}))
    db.commit()
    return {"global_track_id": global_track.id, "local_track_id": local.id, "camera_id": camera.id, "confidence": body.confidence, "method": body.method, "provenance": body.provenance}


@app.get("/api/v1/scene-graph")
def scene_graph(site_id: str | None = None, camera_id: str | None = None, since_minutes: int = Query(60, ge=1, le=10080), radius_m: float | None = Query(None, gt=0, le=10000), center_x_m: float | None = None, center_y_m: float | None = None, limit: int = Query(500, ge=1, le=2000), db: Session = Depends(get_db)):
    """Filtered graph built only from persisted observations and provider links."""
    if radius_m is not None and (center_x_m is None or center_y_m is None): raise HTTPException(422, "center_x_m and center_y_m are required when radius_m is set")
    cutoff = datetime.now(timezone.utc) - __import__("datetime").timedelta(minutes=since_minutes)
    q = db.query(TrackPoint, TrackedObject, Camera).join(TrackedObject, TrackPoint.object_id == TrackedObject.id).join(Camera, TrackedObject.camera_id == Camera.id).filter(TrackPoint.timestamp >= cutoff)
    if site_id: q = q.filter(Camera.site_id == site_id)
    if camera_id: q = q.filter(Camera.id == camera_id)
    rows = q.order_by(TrackPoint.timestamp.desc()).limit(limit).all()
    nodes, edges, seen = [], [], set()
    for point, track, camera in rows:
        if radius_m is not None and center_x_m is not None and center_y_m is not None:
            if point.world_x_m is None or point.world_y_m is None: continue
            if (point.world_x_m-center_x_m)**2 + (point.world_y_m-center_y_m)**2 > radius_m**2: continue
        if track.id not in seen:
            seen.add(track.id)
            nodes.append({"id": track.id, "kind": "local_track", "camera_id": camera.id, "site_id": camera.site_id, "class": track.object_class, "timestamp": point.timestamp, "image_position": {"x": point.x, "y": point.y}, "world_position_m": {"x": point.world_x_m, "y": point.world_y_m, "z": point.world_z_m} if point.world_x_m is not None else None, "depth_m": point.depth_m, "speed_mps": point.speed_mps})
            membership = db.query(GlobalTrackMembership).filter_by(local_track_id=track.id).first()
            if membership:
                global_key = "global:" + membership.global_track_id
                if global_key not in seen:
                    global_track = db.get(GlobalTrack, membership.global_track_id)
                    if global_track:
                        nodes.append({"id": global_key, "kind": "global_identity_hypothesis", "site_id": global_track.site_id, "class": global_track.object_class, "timestamp": global_track.last_seen})
                        seen.add(global_key)
                edges.append({"source": "global:" + membership.global_track_id, "target": track.id, "relation": "provider_identity_match", "confidence": membership.confidence, "method": membership.method, "provenance": membership.provenance, "timestamp": membership.matched_at})
            for relation in db.query(SceneRelationship).filter(SceneRelationship.subject_id == track.id, SceneRelationship.camera_id == camera.id, SceneRelationship.timestamp >= cutoff).order_by(SceneRelationship.timestamp.desc()).limit(8).all():
                edges.append({"source": relation.subject_id, "target": relation.object_id, "relation": relation.relation, "confidence": relation.confidence, "timestamp": relation.timestamp})
    return {"nodes": nodes, "edges": edges, "count": len(nodes), "scope": {"site_id": site_id, "camera_id": camera_id, "since_minutes": since_minutes, "radius_m": radius_m, "spatial_filter_applied": radius_m is not None and center_x_m is not None and center_y_m is not None}}


@app.get("/api/v1/tracks/{object_id}/trajectory")
def track_trajectory(object_id: str, since_minutes: int = Query(240, ge=1, le=10080), limit: int = Query(1000, ge=1, le=5000), db: Session = Depends(get_db)):
    track = db.get(TrackedObject, object_id)
    if not track: raise HTTPException(404, "Track not found")
    cutoff = datetime.now(timezone.utc) - __import__("datetime").timedelta(minutes=since_minutes)
    points = db.query(TrackPoint).filter(TrackPoint.object_id == object_id, TrackPoint.timestamp >= cutoff).order_by(TrackPoint.timestamp.asc()).limit(limit).all()
    behaviours = db.query(BehaviourObservation).filter(BehaviourObservation.object_id == object_id, BehaviourObservation.timestamp >= cutoff).order_by(BehaviourObservation.timestamp.asc()).limit(limit).all()
    ppe = db.query(PPEObservation).filter(PPEObservation.object_id == object_id, PPEObservation.timestamp >= cutoff).order_by(PPEObservation.timestamp.asc()).limit(limit).all()
    return {"object_id": track.id, "camera_id": track.camera_id, "coordinate_frame": "camera_image_pixels; world coordinates are null unless provider calibration supplies them", "points": [{"timestamp": p.timestamp, "x": p.x, "y": p.y, "depth_m": p.depth_m, "world_position_m": {"x":p.world_x_m,"y":p.world_y_m,"z":p.world_z_m} if p.world_x_m is not None else None, "speed_mps": p.speed_mps, "direction_deg": p.direction_deg} for p in points], "behaviour": [{"timestamp": b.timestamp, "type": b.behaviour_type, "confidence": b.confidence, "evidence_ref": b.evidence_ref} for b in behaviours], "ppe": [{"timestamp": p.timestamp, "helmet": p.helmet, "vest": p.vest, "gloves": p.gloves, "goggles": p.goggles, "confidences": p.confidences} for p in ppe]}


@app.get("/api/v1/incidents/{incident_id}/activity")
def incident_activity(incident_id: str, db: Session = Depends(get_db)):
    incident = db.get(Incident, incident_id)
    if not incident: raise HTTPException(404, "Incident not found")
    rows = db.query(AuditLog).filter(AuditLog.entity == "incident", AuditLog.entity_id == incident_id).order_by(AuditLog.timestamp.asc()).all()
    return [{"id": r.id, "actor": r.actor, "action": r.action, "timestamp": r.timestamp, "details": r.metadata_json} for r in rows]


@app.get("/api/v1/analytics/perception")
def perception_performance(db: Session = Depends(get_db)):
    rows = db.query(ModelVersion, func.count(Detection.id)).outerjoin(Detection, Detection.model_version_id == ModelVersion.id).group_by(ModelVersion.id).order_by(ModelVersion.model, ModelVersion.version).all()
    latest_health = []
    for camera in db.query(Camera).order_by(Camera.name).all():
        latest = db.query(CameraHealth).filter_by(camera_id=camera.id).order_by(CameraHealth.timestamp.desc()).first()
        if latest:
            latest_health.append({"camera_id": camera.id, "camera_name": camera.name, "status": latest.health_state, "fps": latest.fps, "latency_ms": latest.latency_ms, "dropped_frames": latest.dropped_frames, "observed_at": latest.timestamp})
    return {"models": [{"model": m.model, "version": m.version, "framework": m.framework, "detections_persisted": count, "deployed_at": m.deployed_at} for m, count in rows], "camera_health_latest": latest_health, "source": "persisted_model_versions_detections_camera_health"}


@app.get("/api/v1/machines/{machine_id}/baseline")
def machine_baseline(machine_id: str, window: int = Query(200, ge=20, le=2000), db: Session = Depends(get_db)):
    history = db.query(MachineState).filter_by(machine_id=machine_id).order_by(MachineState.timestamp.desc()).limit(window).all()
    counts = {}
    for sample in history: counts[sample.state] = counts.get(sample.state, 0) + 1
    total = len(history)
    return {"machine_id": machine_id, "method": "rolling_state_frequency_v1", "available": total >= 20, "sample_count": total, "window_limit": window, "state_counts": counts, "state_frequency": {state: round(count / total, 4) for state, count in counts.items()} if total else {}, "latest_observed_anomaly_score": history[0].anomaly_score if history else None, "minimum_samples": 20, "note": "Frequency baseline is explainable state history; it is not a physical sensor model."}


@app.get("/api/v1/analytics/overview")
def analytics_overview(days: int = Query(7, ge=1, le=90), db: Session = Depends(get_db)):
    since = datetime.now(timezone.utc) - __import__("datetime").timedelta(days=days)
    events = db.query(Event).filter(Event.timestamp >= since).order_by(Event.timestamp.asc()).all()
    day_buckets = {}
    by_type, by_severity, by_camera, by_zone, reason_codes = {}, {}, {}, {}, {}
    for event in events:
        day = event.timestamp.date().isoformat()
        day_buckets[day] = day_buckets.get(day, 0) + 1
        for bucket, key in ((by_type, event.event_type), (by_severity, event.severity), (by_camera, event.camera_id)):
            bucket[key] = bucket.get(key, 0) + 1
        if event.zone_id: by_zone[event.zone_id] = by_zone.get(event.zone_id, 0) + 1
        for code in event.reason_codes or []: reason_codes[code] = reason_codes.get(code, 0) + 1
    return {"window_days": days, "generated_at": datetime.now(timezone.utc), "event_count": len(events), "events_by_day": [{"date": k, "count": day_buckets.get(k, 0)} for k in sorted(day_buckets)], "by_type": by_type, "by_severity": by_severity, "by_camera": by_camera, "by_zone": by_zone, "reason_codes": reason_codes, "source": "persisted_events"}


@app.get("/api/v1/analytics/heatmap")
def analytics_heatmap(camera_id: str, bins: int = Query(12, ge=4, le=40), since_minutes: int = Query(1440, ge=1, le=10080), db: Session = Depends(get_db)):
    camera = ensure_camera(db, camera_id)
    match = re.fullmatch(r"\s*(\d+)\s*[xX]\s*(\d+)\s*", camera.resolution or "")
    if not match: return {"camera_id": camera_id, "available": False, "reason": "Camera resolution is not configured; pixel coordinates cannot be normalized safely.", "bins": []}
    width, height = map(int, match.groups())
    since = datetime.now(timezone.utc) - __import__("datetime").timedelta(minutes=since_minutes)
    points = db.query(TrackPoint).join(TrackedObject, TrackPoint.object_id == TrackedObject.id).filter(TrackedObject.camera_id == camera_id, TrackPoint.timestamp >= since).all()
    counts = {}
    for p in points:
        x, y = p.x / width, p.y / height
        if not (0 <= x <= 1 and 0 <= y <= 1): continue
        bx, by = min(bins-1, int(x*bins)), min(bins-1, int(y*bins))
        counts[(bx,by)] = counts.get((bx,by), 0) + 1
    return {"camera_id": camera_id, "available": True, "coordinate_frame": "normalized_camera_image", "window_minutes": since_minutes, "observation_count": sum(counts.values()), "bins": [{"x": x, "y": y, "count": n} for (x,y), n in sorted(counts.items())]}


@app.post("/api/v1/investigations/query")
def investigation_query(body: InvestigationQuery, db: Session = Depends(get_db)):
    question = body.question.strip()
    text_q = question.lower()
    q = db.query(Event).join(Incident, Incident.event_id == Event.id).order_by(Event.timestamp.desc())
    if body.camera_id: q = q.filter(Event.camera_id == body.camera_id)
    if "critical" in text_q: q = q.filter(Event.severity == "critical")
    elif "high" in text_q: q = q.filter(Event.severity == "high")
    elif "medium" in text_q: q = q.filter(Event.severity == "medium")
    if "today" in text_q: q = q.filter(func.date(Event.timestamp) == datetime.now(timezone.utc).date())
    if "ppe" in text_q or "helmet" in text_q or "vest" in text_q: q = q.filter(Event.event_type.in_(["missing_helmet", "missing_vest"]))
    rows = q.limit(50).all()
    return {"question": question, "answer": f"Found {len(rows)} persisted event(s) matching the supported filters. Results below are drawn from the incident event store; no ungrounded inference was generated.", "grounding": {"source": "events", "filters": {"today": "today" in text_q, "severity": "critical" if "critical" in text_q else "high" if "high" in text_q else "medium" if "medium" in text_q else None, "category": "ppe" if any(w in text_q for w in ("ppe","helmet","vest")) else None, "camera_id": body.camera_id}, "limit": 50}, "events": [{"id": e.id, "incident_id": db.query(Incident.id).filter_by(event_id=e.id).scalar(), "timestamp":e.timestamp,"camera_id":e.camera_id,"event_type":e.event_type,"severity":e.severity,"reason_codes":e.reason_codes} for e in rows]}


@app.get("/api/v1/incidents/{incident_id}/report")
def incident_report(incident_id: str, db: Session = Depends(get_db)):
    incident = db.get(Incident, incident_id)
    if not incident: raise HTTPException(404, "Incident not found")
    event = db.get(Event, incident.event_id)
    audit = db.query(AuditLog).filter(AuditLog.entity == "incident", AuditLog.entity_id == incident.id).order_by(AuditLog.timestamp.asc()).all()
    esc = lambda value: __import__("html").escape(str(value))
    codes = "".join(f"<li>{esc(c)}</li>" for c in event.reason_codes or [])
    history = "".join(f"<li><time>{esc(a.timestamp.isoformat())}</time> — {esc(a.action)} — {esc(a.metadata_json)}</li>" for a in audit)
    html = f"<!doctype html><html><head><meta charset='utf-8'><title>IntelliWatch Incident {esc(incident.id)}</title><style>body{{font:16px system-ui;max-width:850px;margin:48px auto;color:#20221e}}h1{{font-size:30px}}.brand{{color:#73805e;letter-spacing:.15em}}section{{border-top:1px solid #bbb;padding:20px 0}}small{{color:#667}}@media print{{button{{display:none}}}}</style></head><body><div class='brand'>INTELLIWATCH · INCIDENT REPORT</div><h1>{esc(event.event_type.replace('_',' ').title())}</h1><p>{esc(event.severity.upper())} · {esc(event.timestamp.isoformat())}</p><section><h2>Incident record</h2><p>Status: {esc(incident.status)} · Camera: {esc(event.camera_id)} · Confidence: {event.confidence:.3f}</p><p>Assigned to: {esc(incident.assigned_to or 'Unassigned')}</p></section><section><h2>Evidence rationale</h2><ul>{codes}</ul><p>Evidence reference: {esc(event.evidence_ref or 'No evidence object attached')}</p></section><section><h2>Lifecycle audit</h2><ul>{history or '<li>No lifecycle changes recorded.</li>'}</ul></section><button onclick='window.print()'>Print / Save PDF</button><small>Generated from IntelliWatch persisted incident and audit records.</small></body></html>"
    from fastapi.responses import HTMLResponse
    return HTMLResponse(html, headers={"Content-Disposition": f'attachment; filename="intelliwatch-incident-{incident.id}.html"'})


@app.get("/api/v1/incidents/{incident_id}/summary")
def incident_summary(incident_id: str, db: Session = Depends(get_db)):
    incident = db.get(Incident, incident_id)
    if not incident: raise HTTPException(404, "Incident not found")
    event = db.get(Event, incident.event_id)
    lifecycle = db.query(AuditLog).filter(AuditLog.entity == "incident", AuditLog.entity_id == incident_id).order_by(AuditLog.timestamp.asc()).all()
    detection_rows = db.query(Detection).filter(Detection.camera_id == event.camera_id, Detection.object_id.in_(event.object_ids or []), Detection.frame_ts <= event.timestamp).order_by(Detection.frame_ts.desc()).limit(20).all() if event.object_ids else []
    latest_machine = None
    machine_id = (event.payload or {}).get("machine_id")
    if machine_id:
        state = db.query(MachineState).filter_by(machine_id=machine_id, camera_id=event.camera_id).filter(MachineState.timestamp <= event.timestamp).order_by(MachineState.timestamp.desc()).first()
        if state: latest_machine = {"machine_id": state.machine_id, "state": state.state, "anomaly_score": state.anomaly_score, "timestamp": state.timestamp}
    return {"incident_id": incident.id, "summary": f"{event.severity.title()} severity {event.event_type.replace('_', ' ')} recorded at {event.timestamp.isoformat()} by camera {event.camera_id}.", "lifecycle": {"status": incident.status, "opened_at": incident.opened_at, "resolved_at": incident.resolved_at, "assigned_to": incident.assigned_to, "audit": [{"actor": a.actor, "action": a.action, "timestamp": a.timestamp, "details": a.metadata_json} for a in lifecycle]}, "assessment": {"confidence": event.confidence, "reason_codes": event.reason_codes, "source": event.source, "correlation_count": (event.payload or {}).get("correlation_count", 1), "evidence_ref": event.evidence_ref}, "supporting_observations": [{"detection_id": d.id, "object_id": d.object_id, "timestamp": d.frame_ts, "class": d.object_class, "confidence": d.confidence, "model_version_id": d.model_version_id} for d in detection_rows], "machine_state": latest_machine, "generated_from": "persisted event, incident, audit, perception, and machine-state records"}


@app.get("/api/v1/analytics/incidents")
def incident_analytics(db: Session = Depends(get_db)):
    rows = db.query(Event.severity, func.count(Event.id)).join(Incident, Incident.event_id == Event.id).group_by(Event.severity).all()
    return {"by_severity": {severity: count for severity, count in rows}, "total": sum(count for _, count in rows)}


@app.get("/api/v1/analytics/cameras")
def camera_analytics(db: Session = Depends(get_db)):
    return {"total": db.query(func.count(Camera.id)).scalar() or 0, "by_status": dict(db.query(Camera.status, func.count(Camera.id)).group_by(Camera.status).all())}


@app.get("/api/v1/analytics/ppe")
def ppe_analytics(site_id: str | None = None, days: int = Query(7, ge=1, le=90), db: Session = Depends(get_db)):
    since = datetime.now(timezone.utc) - __import__("datetime").timedelta(days=days)
    q = db.query(PPEObservation).join(TrackedObject, PPEObservation.object_id == TrackedObject.id).join(Camera, TrackedObject.camera_id == Camera.id).filter(PPEObservation.timestamp >= since)
    if site_id: q = q.filter(Camera.site_id == site_id)
    records = q.all()
    result = {}
    for item in ("helmet", "vest", "gloves", "goggles"):
        values = [getattr(row, item) for row in records if getattr(row, item) is not None]
        result[item] = {"present": sum(value is True for value in values), "missing": sum(value is False for value in values), "known_observations": len(values), "compliance_rate": round(sum(value is True for value in values) / len(values), 4) if values else None}
    return {"available": bool(records), "window_days": days, "observation_records": len(records), "by_item": result, "source": "persisted_ppe_observations", "reason": None if records else "No PPE observations have been ingested in this time window."}


@app.websocket("/ws/{channel}")
async def live_channel(websocket: WebSocket, channel: str):
    if channel not in {"events", "incidents", "cameras"}:
        await websocket.close(code=1008); return
    token = websocket.query_params.get("token", "")
    try: decode_token(token)
    except HTTPException:
        await websocket.close(code=1008); return
    await websocket.accept()
    client = Redis.from_url(settings.redis_url, decode_responses=True)
    pubsub = client.pubsub(); pubsub.subscribe(channel)
    try:
        await websocket.send_json({"type": "connected", "channel": channel})
        while True:
            message = await asyncio.to_thread(pubsub.get_message, True, 1.0)
            if message and message["type"] == "message": await websocket.send_text(message["data"])
            await asyncio.sleep(0.05)
    except WebSocketDisconnect:
        pass
    finally:
        pubsub.close(); client.close()


@app.websocket("/ws/scene/{camera_id}")
async def scene_channel(websocket: WebSocket, camera_id: str):
    try: decode_token(websocket.query_params.get("token", ""))
    except HTTPException:
        await websocket.close(code=1008); return
    await websocket.accept()
    await websocket.send_json({"type": "scene_channel_ready", "camera_id": camera_id, "message": "Waiting for AI worker observations"})
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect: pass
