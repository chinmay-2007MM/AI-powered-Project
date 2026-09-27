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
from .models import Camera, Zone, Event, Incident, Evidence, AuditLog, Factory, Site, User, TrackedObject, Detection, ModelVersion, PPEObservation, TrackPoint, BehaviourObservation, MachineState, CameraHealth
from .schemas import CameraCreate, CameraRead, CameraUpdate, CameraHealthIn, ZoneCreate, ZoneRead, ZoneUpdate, EventIngest, IncidentStatusUpdate, IncidentRead, FactoryCreate, SiteCreate, SiteRead, AIResult
from .services import create_event, ensure_camera, publish
from .auth import decode_token, make_token, verify_password

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
app.add_middleware(CORSMiddleware, allow_origins=[v.strip() for v in settings.cors_origins.split(",")], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


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
        ai_path = path in {"/api/v1/events/ingest", "/api/v1/observations"} or bool(re.fullmatch(r"/api/v1/cameras/[^/]+/health", path))
        allowed = role == "ai_service" if ai_path else role in {"operator", "admin"}
        if not allowed:
            from starlette.responses import JSONResponse
            return JSONResponse({"detail": "Insufficient role"}, status_code=403)
    request.state.actor = claims.get("sub", "unknown")
    return await call_next(request)


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
    checks = {"database": "ok", "redis": "unknown", "object_storage": "unknown"}
    try: db.execute(text("SELECT 1"))
    except Exception as exc: checks["database"] = "unavailable"; log.warning("database health failure: %s", exc)
    try: Redis.from_url(settings.redis_url, socket_connect_timeout=1).ping(); checks["redis"] = "ok"
    except Exception: checks["redis"] = "unavailable"
    try:
        boto3.client("s3", endpoint_url=settings.s3_endpoint, aws_access_key_id=settings.s3_access_key, aws_secret_access_key=settings.s3_secret_key).head_bucket(Bucket=settings.s3_bucket)
        checks["object_storage"] = "ok"
    except Exception: checks["object_storage"] = "unavailable"
    return {"status": "ok" if checks["database"] == "ok" else "degraded", "timestamp": datetime.now(timezone.utc), "components": checks}


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
    return {"camera_id": camera_id, "observations": observations}


@app.post("/api/v1/observations", status_code=202)
def ingest_observation(body: AIResult, db: Session = Depends(get_db)):
    camera = ensure_camera(db, body.camera_id)
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
            db.add(TrackPoint(id=str(uuid4()), object_id=body.object_id, timestamp=body.timestamp, x=center_x, y=center_y, depth_m=body.depth_m, speed_mps=body.speed_mps, direction_deg=body.direction_deg))
            if body.posture:
                db.add(BehaviourObservation(id=str(uuid4()), object_id=body.object_id, camera_id=camera.id, timestamp=body.timestamp, behaviour_type=body.posture, confidence=body.confidence, evidence_ref=body.evidence_ref))
        if body.machine_id and body.machine_state:
            db.add(MachineState(id=str(uuid4()), camera_id=camera.id, machine_id=body.machine_id, timestamp=body.timestamp, state=body.machine_state, confidence=body.confidence, anomaly_score=body.anomaly_score))
        db.commit()
    else:
        db.commit()
    payload = body.model_dump(by_alias=True, mode="json")
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


@app.get("/api/v1/analytics/incidents")
def incident_analytics(db: Session = Depends(get_db)):
    rows = db.query(Event.severity, func.count(Event.id)).join(Incident, Incident.event_id == Event.id).group_by(Event.severity).all()
    return {"by_severity": {severity: count for severity, count in rows}, "total": sum(count for _, count in rows)}


@app.get("/api/v1/analytics/cameras")
def camera_analytics(db: Session = Depends(get_db)):
    return {"total": db.query(func.count(Camera.id)).scalar() or 0, "by_status": dict(db.query(Camera.status, func.count(Camera.id)).group_by(Camera.status).all())}


@app.get("/api/v1/analytics/ppe")
def ppe_analytics():
    return {"available": False, "reason": "No PPE observations have been ingested yet."}


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
